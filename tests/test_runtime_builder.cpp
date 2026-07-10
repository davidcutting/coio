// The fluent runtime builder: coio::runtime::builder().pool(n).driver<D>(args)...build()
#include <atomic>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/drivers.h>

// Hardcodes the Linux driver set (uring/epoll); skip on a build without io_uring (Windows/IOCP, epoll-only).
#if COIO_HAS_IO_URING

TEST_CASE("runtime::builder fluent builder: 2 uring + 2 epoll, routed spawn, clean teardown") {
    std::atomic<int> ok{0};
    {
        auto rt = coio::runtime::builder()
            .pool(2).driver<coio::uring_driver>(1024u)
            .pool(2).driver<coio::epoll_driver>()
            .build();

        CHECK(rt.size() == 4);

        for (int i = 0; i < 8; ++i)
            rt.spawn_on<coio::capability::io>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        for (int i = 0; i < 4; ++i)
            rt.spawn_on_pool<1>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));

        coio::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 12);
}

TEST_CASE("runtime::builder: single uring pool, exact-worker-count and capability routing") {
    {
        auto rt = coio::runtime::builder()
            .pool(3).driver<coio::uring_driver>(512u)
            .build();
        CHECK(rt.size() == 3);
        std::atomic<int> ran{0};
        rt.spawn_on<coio::capability::timer>(coio::just() | coio::then([&] { ran.fetch_add(1); }));
        coio::this_thread::sync_wait(rt.join());
        CHECK(ran.load() == 1);
    }
}

#else
TEST_CASE("runtime builder (skipped: build has no io_uring)") { CHECK(true); }
#endif
