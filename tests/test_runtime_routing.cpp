// Stage 4: load-aware capability routing — spawn_on<Cap> spreads across ALL eligible pools (p2c),
// not just the first.
#include <atomic>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/init.h>

// Uses io_uring pools explicitly (and includes its header, which #errors without liburing) — skip where
// io_uring is unavailable. The p2c routing itself is backend-agnostic (see test_heterogeneous_runtime).
#if COIO_HAS_IO_URING
#include <coio/asyncio/uring_context.h>

TEST_CASE("spawn_on<Cap> distributes across multiple eligible pools (power-of-two-choices)") {
    std::atomic<int> ok{0};
    std::size_t to0 = 0, to1 = 0;
    {
        // two pools, BOTH provide io -> spawn_on<io> must not dump everything on pool 0
        auto rt = coio::make_runtime(
            coio::pool(2, coio::driver_init<coio::uring_driver>(256u)),
            coio::pool(2, coio::driver_init<coio::uring_driver>(256u))
        );
        constexpr int n = 60;
        for (int i = 0; i < n; ++i)
            rt.spawn_on<coio::capability::io>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        to0 = rt.spawns_routed_to<0>();
        to1 = rt.spawns_routed_to<1>();
        coio::this_thread::sync_wait(rt.join());
        CHECK(ok.load() == n);
        CHECK(to0 + to1 == n);         // every spawn was routed to some eligible pool
        CHECK(to0 > 0);                // ...and BOTH pools received work (not just the first)
        CHECK(to1 > 0);
    }
}

TEST_CASE("spawn_on<Cap> with a single eligible pool still routes there (fast path)") {
    std::atomic<int> ok{0};
    {
        // one io pool + one timer-only pool: io is served ONLY by pool 0
        auto rt = coio::make_runtime(
            coio::pool(2, coio::driver_init<coio::uring_driver>(256u)),
            coio::pool(1, coio::driver_init<coio::timer_driver>())
        );
        for (int i = 0; i < 8; ++i)
            rt.spawn_on<coio::capability::io>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        CHECK(rt.spawns_routed_to<0>() == 8);   // all io -> the only io-capable pool
        CHECK(rt.spawns_routed_to<1>() == 0);
        coio::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 8);
}

#else
TEST_CASE("runtime routing (skipped: build has no io_uring)") { CHECK(true); }
#endif
