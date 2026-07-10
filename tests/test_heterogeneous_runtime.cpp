// A heterogeneous runtime: io_uring cores + epoll cores in one process, capability-routed.
#include <atomic>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/runtime/init.h>

// This case is specifically io_uring + epoll cores in one process — skip where either is unavailable
// (Windows/IOCP). The heterogeneous machinery is otherwise backend-agnostic.
#if KIOTO_HAS_IO_URING and KIOTO_HAS_EPOLL
#include <kioto/io/driver/uring_context.h>
#include <kioto/io/driver/epoll_context.h>

namespace {
    using uring_ex = kioto::executor<kioto::uring_driver>;
    using epoll_ex = kioto::executor<kioto::epoll_driver>;
}

// Compile-time routing table is correct before we run anything.
static_assert(kioto::detail::eligible_pools<kioto::capability::io, uring_ex, epoll_ex>().second == 2,
              "both uring and epoll provide io");
static_assert(kioto::detail::eligible_pools<kioto::capability::timer, uring_ex, epoll_ex>().second == 2,
              "both provide timer");
static_assert(kioto::detail::eligible_pools<kioto::capability::io, uring_ex, epoll_ex>().first[0] == 0,
              "first eligible io pool is the uring pool");

TEST_CASE("heterogeneous runtime: 2 uring + 2 epoll, capability-routed spawn, clean teardown") {
    std::atomic<int> ok{0};
    {
        auto rt = kioto::make_runtime(
            kioto::pool(2, kioto::driver_init<kioto::uring_driver>(1024u)),
            kioto::pool(2, kioto::driver_init<kioto::epoll_driver>())
        );
        CHECK(rt.size() == 4);

        // capability::io -> first eligible pool (the uring pool)
        for (int i = 0; i < 8; ++i)
            rt.spawn_on<kioto::capability::io>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));

        // explicit escape hatch -> exercise the epoll pool actually running work
        for (int i = 0; i < 4; ++i)
            rt.spawn_on_pool<1>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));

        kioto::this_thread::sync_wait(rt.join());
    } // rt destroyed -> stop() joins all 4 threads
    CHECK(ok.load() == 12);
}

#else
TEST_CASE("heterogeneous runtime (skipped: needs io_uring + epoll)") { CHECK(true); }
#endif
