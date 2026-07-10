// Per-OPERATION cancellation (test_runtime_cancel covers runtime-level stop). Three properties:
//   1. cancelling a pending accept completes it stopped and leaves the acceptor healthy;
//   2. cancelling a mid-flight read leaves the SOCKET reusable;
//   3. cancelling in-flight reads cross-thread during a live reactor is race-free (the epoll deregister
//      hazard TSan caught) — run under -fsanitize=thread in CI to turn a latent race into a failure.
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/runtime/runtime.h>
#include <kioto/io/io.h>
#include <kioto/io/pipe.h>
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

    // when_any(op, timer->stopped): the timer trips first while the peer stays idle, cancelling `op`.
    template<typename Sndr, typename Sched>
    auto with_timeout(Sndr sndr, Sched sched, std::chrono::milliseconds ms) {
        return kioto::when_any(
            std::move(sndr),
            sched.schedule_after(ms) | kioto::let_value([]() noexcept { return kioto::just_stopped(); }));
    }

    auto wait_until = [](auto pred) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (not pred()) {
            REQUIRE(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(1ms);
        }
    };
}

TEST_CASE_TEMPLATE("a pending accept is cancelled to stopped and the acceptor stays usable", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    bool accept_stopped = false;
    bool reaccept_ok = false;
    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, typename Ctx::scheduler sched,
                                           bool& stopped, bool& ok) -> typename Ctx::template task<> {
        // no client yet -> the 30ms timeout cancels this accept
        auto first = co_await kioto::execution::stopped_as_optional(
            with_timeout(acc.async_accept(), sched, 30ms));
        stopped = not first.has_value();
        // the acceptor must still work: a later client connects and this accept completes
        auto sock = co_await acc.async_accept();
        ok = sock.is_open();
    }(acceptor, ctx.get_scheduler(), accept_stopped, reaccept_ok));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        co_await sched.schedule_after(60ms);   // connect only after the first accept was cancelled
        socket_t client{sched};
        co_await client.async_connect(peer);
        co_await sched.schedule_after(20ms);   // stay alive so the accept side completes
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(accept_stopped);
    CHECK(reaccept_ok);
}

TEST_CASE_TEMPLATE("a cancelled read leaves the socket reusable for a subsequent read", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    bool read_stopped = false;
    std::size_t reread = 0;
    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, typename Ctx::scheduler sched,
                                           bool& stopped, std::size_t& n2) -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        char buf[16];
        // peer stays idle -> this read is cancelled by the timeout
        auto first = co_await kioto::execution::stopped_as_optional(
            with_timeout(sock.async_read_some(kioto::as_writable_bytes(buf)), sched, 30ms));
        stopped = not first.has_value();
        // reuse the SAME socket: the peer now sends and this read must succeed
        n2 = co_await sock.async_read_some(kioto::as_writable_bytes(buf));
    }(acceptor, ctx.get_scheduler(), read_stopped, reread));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t client{sched};
        co_await client.async_connect(peer);
        co_await sched.schedule_after(60ms);   // idle past the cancel window
        co_await (kioto::async_write(client, kioto::as_bytes("hi"sv)) | as_throwing);
        co_await sched.schedule_after(20ms);
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(read_stopped);
    CHECK(reread == 2);
}

namespace {
    // Cross-thread cancel storm: arm K blocking reads on a worker, then request_stop from THIS thread
    // while the reactor is live — K concurrent do_cancel/deregister racing the owner. Repeat many rounds.
    template<typename Ctx>
    void run_cross_thread_cancel_race() {
        using runtime_t = kioto::basic_runtime<Ctx>;
        constexpr int rounds = 300;
        constexpr int k = 8;
        auto rt = kioto_test::make_runtime<Ctx>(2);

        for (int r = 0; r < rounds; ++r) {
            auto src = std::make_shared<kioto::inplace_stop_source>();
            auto remaining = std::make_shared<std::atomic<int>>(k);

            for (int i = 0; i < k; ++i) {
                rt.spawn(kioto::just() | kioto::then([&rt, src, remaining] {
                    auto ws = *runtime_t::current_scheduler();
                    using sched_t = decltype(ws);
                    auto [reader, writer] = kioto::make_pipe(ws);
                    auto pipe = std::make_shared<std::pair<kioto::pipe_reader<sched_t>, kioto::pipe_writer<sched_t>>>(
                        std::move(reader), std::move(writer));
                    rt.spawn_on(ws, [](std::shared_ptr<std::pair<kioto::pipe_reader<sched_t>,
                                                                 kioto::pipe_writer<sched_t>>> p,
                                       std::shared_ptr<kioto::inplace_stop_source> src,
                                       std::shared_ptr<std::atomic<int>> remaining) -> kioto::task<> {
                        char buf[8];
                        // no write -> the read blocks until the cross-thread stop cancels it
                        co_await kioto::execution::stopped_as_optional(
                            kioto::stop_when(p->first.async_read_some(kioto::as_writable_bytes(buf)),
                                             src->get_token()));
                        remaining->fetch_sub(1, std::memory_order_relaxed);
                    }(pipe, src, remaining));
                }));
            }

            std::this_thread::sleep_for(1ms);   // let the reads arm on the worker
            src->request_stop();                 // cross-thread cancel of K in-flight reads
            wait_until([&] { return remaining->load(std::memory_order_relaxed) == 0; });
        }
        CHECK(true);   // reached here without hang / crash / TSan abort
    }
}

#if KIOTO_HAS_IO_URING
TEST_CASE("uring: cross-thread cancel of in-flight reads during a live reactor is race-free") {
    run_cross_thread_cancel_race<kioto::uring_context>();
}
#endif
#if KIOTO_HAS_EPOLL
TEST_CASE("epoll: cross-thread cancel of in-flight reads during a live reactor is race-free") {
    run_cross_thread_cancel_race<kioto::epoll_context>();
}
#endif
#if KIOTO_HAS_IOCP
TEST_CASE("iocp: cross-thread cancel of in-flight reads during a live reactor is race-free") {
    run_cross_thread_cancel_race<kioto::iocp_context>();
}
#endif
