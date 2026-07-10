// async_receive_sequence: one high-level "keep receiving datagrams" API, lowered to io_uring multishot
// recv (kernel buf_ring) on uring and to a re-issued single-shot recv loop on epoll. Same test code, both.
#include <atomic>
#include <chrono>
#include <concepts>
#include <span>
#include <system_error>
#include <utility>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/udp.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    inline constexpr int num_dgrams = 100;
}

TEST_CASE_TEMPLATE("async_receive_sequence receives datagrams until stopped", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using udp_socket = kioto::udp::socket<typename Ctx::scheduler>;
    using buffers_t = typename Ctx::scheduler::buffer_pool;

    // One backend-neutral API => one completion-signature set on every backend: errors as std::error_code
    // (never exception_ptr), whether lowered to io_uring multishot recv or the epoll coroutine loop.
    namespace ex = kioto::execution;
    using seq_t = decltype(std::declval<udp_socket&>().async_receive_sequence(
        std::declval<buffers_t&>(), [](std::span<std::byte>) noexcept {}));
    static_assert(std::same_as<ex::completion_signatures_of_t<seq_t>,
                  ex::completion_signatures<ex::set_value_t(), ex::set_error_t(std::error_code), ex::set_stopped_t()>>,
                  "receive sequence must complete with error_code on every backend");

    Ctx context;
    auto sched = context.get_scheduler();

    buffers_t bufs{context, 256, 2048, 0};                 // buf_ring on uring, one reused buffer on epoll
    udp_socket tx{sched, kioto::udp::v4()};
    udp_socket rx{sched, kioto::udp::v4()};
    tx.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    rx.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    tx.connect(rx.local_endpoint());
    rx.connect(tx.local_endpoint());

    std::atomic<int> count{0};
    kioto::inplace_stop_source stop;
    kioto::async_scope scope;

    // Consumer: the sequence delivers each datagram to the sink until we trip the stop.
    scope.spawn_on(sched, [](udp_socket& rx, buffers_t& bufs, std::atomic<int>& count,
                             kioto::inplace_stop_token tok) -> typename Ctx::template task<> {
        co_await kioto::stop_when(
            rx.async_receive_sequence(bufs, [&count](std::span<std::byte> dg) noexcept {
                count.fetch_add(1, std::memory_order_relaxed);
                (void)dg;
            }),
            tok);
    }(rx, bufs, count, stop.get_token()));

    // Controller: send num_dgrams, wait until all are received, then stop the sequence.
    scope.spawn_on(sched, [](typename Ctx::scheduler sched, udp_socket& tx, std::atomic<int>& count,
                             kioto::inplace_stop_source& stop) -> typename Ctx::template task<> {
        std::byte payload[64]{};
        for (int i = 0; i < num_dgrams; ++i) co_await tx.async_send(kioto::as_bytes(payload));
        while (count.load(std::memory_order_relaxed) < num_dgrams) co_await sched.schedule_after(1ms);
        stop.request_stop();
    }(sched, tx, count, stop));

    context.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(count.load() == num_dgrams);
}
