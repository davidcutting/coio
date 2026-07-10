// A heterogeneous runtime: io_uring cores + epoll cores in one process, capability-routed.
#include <atomic>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/init.h>

// This case is specifically io_uring + epoll cores in one process — skip where either is unavailable
// (Windows/IOCP). The heterogeneous machinery is otherwise backend-agnostic.
#if COIO_HAS_IO_URING and COIO_HAS_EPOLL
#include <coio/asyncio/uring_context.h>
#include <coio/asyncio/epoll_context.h>

namespace {
    using uring_ex = coio::executor<coio::uring_driver>;
    using epoll_ex = coio::executor<coio::epoll_driver>;
}

// Compile-time routing table is correct before we run anything.
static_assert(coio::detail::eligible_pools<coio::capability::io, uring_ex, epoll_ex>().second == 2,
              "both uring and epoll provide io");
static_assert(coio::detail::eligible_pools<coio::capability::timer, uring_ex, epoll_ex>().second == 2,
              "both provide timer");
static_assert(coio::detail::eligible_pools<coio::capability::io, uring_ex, epoll_ex>().first[0] == 0,
              "first eligible io pool is the uring pool");

TEST_CASE("heterogeneous runtime: 2 uring + 2 epoll, capability-routed spawn, clean teardown") {
    std::atomic<int> ok{0};
    {
        auto rt = coio::make_runtime(
            coio::pool(2, coio::driver_init<coio::uring_driver>(1024u)),
            coio::pool(2, coio::driver_init<coio::epoll_driver>())
        );
        CHECK(rt.size() == 4);

        // capability::io -> first eligible pool (the uring pool)
        for (int i = 0; i < 8; ++i)
            rt.spawn_on<coio::capability::io>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));

        // explicit escape hatch -> exercise the epoll pool actually running work
        for (int i = 0; i < 4; ++i)
            rt.spawn_on_pool<1>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));

        coio::this_thread::sync_wait(rt.join());
    } // rt destroyed -> stop() joins all 4 threads
    CHECK(ok.load() == 12);
}

#else
TEST_CASE("heterogeneous runtime (skipped: needs io_uring + epoll)") { CHECK(true); }
#endif
