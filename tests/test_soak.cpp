// Soak / leak harness: drive real workloads in repeated batches and assert resources don't grow
// unboundedly. Two independent signals:
//   * fd count (via /proc/self/fd) must be EXACT batch-to-batch — a per-iteration fd leak is fatal.
//   * RSS (via /proc/self/statm) must be FLAT at steady state — measured as a second-derivative: after a
//     warmup batch fills the allocator/frame pools, a further batch must not grow RSS. A leak keeps
//     climbing linearly; allocator warmup does not. This catches "grows during the run, freed at exit"
//     bugs that LSan (reachable-at-exit only) cannot see; run under ASan/LSan for the complementary check.
//
// KIOTO_SOAK_ITERATIONS overrides the batch size for a real long-running soak (default is CI-friendly).
// Linux-only (the measurements read /proc); on other platforms this TU compiles to zero tests.
#include <kioto/base/config.h>

#if KIOTO_OS_LINUX

#include <unistd.h>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string_view>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
#include "io_contexts.h"

using namespace std::chrono_literals;
using namespace std::string_view_literals;

namespace {
    KIOTO_ALWAYS_INLINE auto dispatch_result(std::error_code ec, std::size_t n) noexcept {
        kioto::async_result<kioto::execution::set_value_t(std::size_t), kioto::execution::set_error_t(std::error_code)> r;
        if (ec) { if (ec == std::errc::operation_canceled) r.set_stopped(); else r.set_error(ec); }
        else r.set_value(n);
        return r;
    }
    inline const auto as_throwing = kioto::execution::let_value(dispatch_result);

    [[nodiscard]] auto rss_kib() -> long {
        std::ifstream f("/proc/self/statm");
        long total = 0, resident = 0;
        f >> total >> resident;
        return resident * (::sysconf(_SC_PAGESIZE) / 1024);
    }
    [[nodiscard]] auto open_fd_count() -> long {
        long n = 0;
        for (const auto& e : std::filesystem::directory_iterator("/proc/self/fd")) { static_cast<void>(e); ++n; }
        return n;   // includes the transient dir fd, but identically across calls -> cancels out
    }
    [[nodiscard]] auto soak_iterations() -> int {
        if (const char* env = std::getenv("KIOTO_SOAK_ITERATIONS")) return std::atoi(env);
        return 1500;
    }

    // Sequential connection churn on one context: connect + accept + roundtrip + close, `count` times.
    // At most one live connection at a time, so a healthy run has a bounded, flat resource footprint.
    template<typename Ctx>
    void churn_connections(Ctx& ctx, kioto::tcp::acceptor<typename Ctx::scheduler>& acc,
                           kioto::endpoint ep, int count) {
        using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
        using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
        kioto::async_scope scope;
        scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, typename Ctx::scheduler sched,
                                               kioto::endpoint peer, int count) -> typename Ctx::template task<> {
            for (int i = 0; i < count; ++i) {
                socket_t client{sched};
                co_await client.async_connect(peer);          // kernel handshake completes independently
                auto server = co_await acc.async_accept();
                co_await (kioto::async_write(client, kioto::as_bytes("ping"sv)) | as_throwing);
                char buf[8];
                co_await server.async_read_some(kioto::as_writable_bytes(buf));
                // client + server destroyed here -> both fds closed, handles torn down
            }
        }(acc, ctx.get_scheduler(), ep, count));
        ctx.run();
        kioto::this_thread::sync_wait(scope.join());
    }

    template<typename Ctx>
    void churn_timers(Ctx& ctx, int count) {
        kioto::async_scope scope;
        scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, int count)
                                               -> typename Ctx::template task<> {
            for (int i = 0; i < count; ++i) co_await sched.schedule_after(1us);   // add + fire + remove from the heap
        }(ctx.get_scheduler(), count));
        ctx.run();
        kioto::this_thread::sync_wait(scope.join());
    }
}

TEST_CASE_TEMPLATE("connection churn does not leak fds or memory", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    const int n = soak_iterations();

    churn_connections(ctx, acceptor, ep, n / 4);       // warmup: reach steady state
    const long fd0 = open_fd_count();
    churn_connections(ctx, acceptor, ep, n);
    const long rss1 = rss_kib();
    const long fd1 = open_fd_count();
    churn_connections(ctx, acceptor, ep, n);
    const long rss2 = rss_kib();
    const long fd2 = open_fd_count();

    INFO("fds: ", fd0, " -> ", fd1, " -> ", fd2, " | rss(KiB): ", rss1, " -> ", rss2);
    CHECK(fd1 == fd0);                 // no fd leak across the first measured batch
    CHECK(fd2 == fd0);                 // ...nor the second
    CHECK(rss2 - rss1 < 1024);         // steady-state RSS growth < 1 MiB (a real leak keeps climbing)
}

TEST_CASE_TEMPLATE("timer churn does not leak", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    Ctx ctx;
    const int n = soak_iterations();

    churn_timers(ctx, n / 4);
    const long fd0 = open_fd_count();
    churn_timers(ctx, n);
    const long rss1 = rss_kib();
    churn_timers(ctx, n);
    const long rss2 = rss_kib();
    const long fd2 = open_fd_count();

    INFO("rss(KiB): ", rss1, " -> ", rss2);
    CHECK(fd2 == fd0);                 // timers churn no fds
    CHECK(rss2 - rss1 < 1024);         // timer-heap add/remove leaves no residue
}

#endif // KIOTO_OS_LINUX
