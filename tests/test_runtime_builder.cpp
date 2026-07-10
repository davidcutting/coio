// The fluent runtime builder: kioto::runtime::builder().pool(n).driver<D>(args)...build()
#include <atomic>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/driver/drivers.h>

// Hardcodes the Linux driver set (uring/epoll); skip on a build without io_uring (Windows/IOCP, epoll-only).
#if KIOTO_HAS_IO_URING

TEST_CASE("runtime::builder fluent builder: 2 uring + 2 epoll, routed spawn, clean teardown") {
    std::atomic<int> ok{0};
    {
        auto rt = kioto::runtime::builder()
            .pool(2).driver<kioto::uring_driver>(1024u)
            .pool(2).driver<kioto::epoll_driver>()
            .build();

        CHECK(rt.size() == 4);

        for (int i = 0; i < 8; ++i)
            rt.spawn_on<kioto::capability::io>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        for (int i = 0; i < 4; ++i)
            rt.spawn_on_pool<1>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));

        kioto::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 12);
}

TEST_CASE("runtime::builder: single uring pool, exact-worker-count and capability routing") {
    {
        auto rt = kioto::runtime::builder()
            .pool(3).driver<kioto::uring_driver>(512u)
            .build();
        CHECK(rt.size() == 3);
        std::atomic<int> ran{0};
        rt.spawn_on<kioto::capability::timer>(kioto::just() | kioto::then([&] { ran.fetch_add(1); }));
        kioto::this_thread::sync_wait(rt.join());
        CHECK(ran.load() == 1);
    }
}

#else
TEST_CASE("runtime builder (skipped: build has no io_uring)") { CHECK(true); }
#endif
