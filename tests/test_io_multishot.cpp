#include <atomic>
#include <chrono>
#include <cstddef>
#include <span>
#include <doctest/doctest.h>
#include <kioto/base/config.h>
// uring-only feature: on platforms without io_uring this TU compiles to a single skip marker.
#if KIOTO_HAS_IO_URING
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/udp.h>
#include <kioto/io/driver/uring_context.h>

using namespace std::chrono_literals;

namespace {
    using io_context = kioto::uring_context;
    using udp_socket = kioto::udp::socket<io_context::scheduler>;

    // Consume the multishot recv until `tok` is tripped, counting each delivered datagram. Completes
    // stopped when cancelled (unwinds the coroutine — fine on a scope), so no nested join is needed.
    auto consumer(udp_socket& rx, kioto::buffer_ring& bufs,
                  std::atomic<int>& count, kioto::inplace_stop_token tok) -> io_context::task<> {
        co_await kioto::stop_when(
            rx.async_receive_sequence(bufs, [&](std::span<std::byte> dg) noexcept {   // multishot lowering on uring
                count.fetch_add(1, std::memory_order_relaxed);
                (void)dg;
            }),
            tok);
    }

    // Blast n datagrams, let them drain, then trip the stop so the consumer completes.
    auto controller(io_context::scheduler sched, udp_socket& tx, int n,
                    kioto::inplace_stop_source& stop) -> io_context::task<> {
        std::byte payload[64]{};
        for (int i = 0; i < n; ++i) co_await tx.async_send(kioto::as_bytes(payload));
        co_await sched.schedule_after(50ms);
        stop.request_stop();
        co_return;
    }
}

TEST_CASE("uring multishot receive delivers every datagram") {
    io_context ctx;
    auto sched = ctx.get_scheduler();

    kioto::buffer_ring bufs{ctx, 256, 2048, 0};
    udp_socket tx{sched, kioto::udp::v4()};
    udp_socket rx{sched, kioto::udp::v4()};
    tx.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    rx.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    rx.set_option(udp_socket::receive_buffer_size{1 << 22});
    tx.connect(rx.local_endpoint());
    rx.connect(tx.local_endpoint());

    std::atomic<int> count{0};
    kioto::inplace_stop_source stop;
    constexpr int n = 2000;

    kioto::async_scope scope;
    scope.spawn_on(sched, consumer(rx, bufs, count, stop.get_token()));
    scope.spawn_on(sched, controller(sched, tx, n, stop));
    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(count.load() == n);
}

#else
TEST_CASE("uring multishot receive (skipped: no io_uring on this platform)") { CHECK(true); }
#endif
