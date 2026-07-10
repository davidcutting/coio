// async_accept_sequence: one high-level "keep accepting" API, lowered to io_uring multishot accept on
// uring and to a re-issued single-shot accept loop on epoll. Runs on every io context, same test code.
#include <atomic>
#include <chrono>
#include <concepts>
#include <system_error>
#include <utility>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    inline constexpr int num_conns = 8;
}

TEST_CASE_TEMPLATE("async_accept_sequence keeps accepting until stopped", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;

    // The point of one backend-neutral API: the sequence sender advertises the SAME completion signatures on
    // every backend — errors as std::error_code (never exception_ptr), whether lowered to io_uring multishot
    // or the epoll coroutine loop.
    namespace ex = kioto::execution;
    using seq_t = decltype(std::declval<acceptor_t&>().async_accept_sequence([](socket_t) noexcept {}));
    static_assert(std::same_as<ex::completion_signatures_of_t<seq_t>,
                  ex::completion_signatures<ex::set_value_t(), ex::set_error_t(std::error_code), ex::set_stopped_t()>>,
                  "accept sequence must complete with error_code on every backend");

    Ctx context;
    auto sched = context.get_scheduler();
    acceptor_t acceptor{sched, kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();

    std::atomic<int> accepted{0};
    kioto::inplace_stop_source stop;
    kioto::async_scope scope;

    // Consumer: the sequence delivers each accepted connection to the sink until we trip the stop.
    scope.spawn_on(sched, [](acceptor_t& acc, std::atomic<int>& count, kioto::inplace_stop_token tok)
                              -> typename Ctx::template task<> {
        co_await kioto::stop_when(
            acc.async_accept_sequence([&count](socket_t sock) noexcept {
                count.fetch_add(1, std::memory_order_relaxed);
                (void)sock;   // drop -> closes the accepted fd; we only count here
            }),
            tok);
    }(acceptor, accepted, stop.get_token()));

    // Controller: open num_conns clients, wait until all are accepted, then stop the sequence.
    scope.spawn_on(sched, [](typename Ctx::scheduler sched, kioto::endpoint ep,
                             std::atomic<int>& count, kioto::inplace_stop_source& stop)
                              -> typename Ctx::template task<> {
        std::vector<socket_t> clients;
        for (int i = 0; i < num_conns; ++i) {
            socket_t c{sched};
            co_await c.async_connect(ep);
            clients.push_back(std::move(c));
        }
        while (count.load(std::memory_order_relaxed) < num_conns) co_await sched.schedule_after(1ms);
        stop.request_stop();
    }(sched, ep, accepted, stop));

    context.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(accepted.load() == num_conns);
}
