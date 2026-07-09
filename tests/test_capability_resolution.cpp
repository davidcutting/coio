// The builder's .capability<>() : declare intent, the fastest available driver is auto-selected.
#include <atomic>
#include <concepts>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/drivers.h>

// Fast-path-first resolution is a compile-time fact of the build's driver registry.
static_assert(std::same_as<coio::detail::resolve_t<coio::default_drivers, coio::capability::io>, coio::uring_driver>,
              "networking resolves to io_uring (fast path) on this build");
static_assert(std::same_as<coio::detail::resolve_t<coio::default_drivers, coio::capability::timer>, coio::uring_driver>,
              "timer resolves to the first provider (io_uring) too");
static_assert(std::same_as<
                  coio::detail::resolve_t<coio::default_drivers, coio::capability::io, coio::capability::timer>,
                  coio::uring_driver>,
              "a driver covering BOTH requested capabilities is chosen");
static_assert(std::same_as<coio::detail::resolve_t<coio::default_drivers, coio::capability::file>, coio::uring_driver>,
              "regular files resolve to io_uring (the only file-capable backend on this build)");
// epoll does NOT provide file; an epoll-only registry can't serve files (would be a resolve static_assert).
static_assert(coio::detail::resolve_index<coio::type_list<coio::epoll_driver, coio::timer_driver>, coio::capability::file>()
                  == coio::type_list<coio::epoll_driver, coio::timer_driver>::npos,
              "no epoll/timer driver provides capability::file");

TEST_CASE("builder .capability<>(): auto-selects the driver and runs") {
    std::atomic<int> ok{0};
    {
        auto rt = coio::runtime::builder()
            .pool(2).capability<coio::capability::io>()   // -> 2x executor<uring_driver> (fast path, default ring size)
            .build();
        CHECK(rt.size() == 2);
        for (int i = 0; i < 8; ++i)
            rt.spawn_on<coio::capability::io>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        coio::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 8);
}

TEST_CASE("builder .capability<file>(): resolves to io_uring and routes file work there") {
    std::atomic<int> ok{0};
    {
        auto rt = coio::runtime::builder()
            .pool(2).capability<coio::capability::io, coio::capability::file>()  // networking + files -> io_uring
            .build();
        CHECK(rt.size() == 2);
        for (int i = 0; i < 4; ++i)
            rt.spawn_on<coio::capability::file>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        coio::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 4);
}

TEST_CASE("builder: capability and explicit driver pools coexist in one runtime") {
    std::atomic<int> ok{0};
    {
        auto rt = coio::runtime::builder()
            .pool(2).capability<coio::capability::io>()   // resolved -> uring
            .pool(2).driver<coio::epoll_driver>()         // pinned -> epoll
            .build();
        CHECK(rt.size() == 4);
        for (int i = 0; i < 6; ++i)
            rt.spawn_on<coio::capability::io>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        for (int i = 0; i < 6; ++i)
            rt.spawn_on_pool<1>(coio::just() | coio::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        coio::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 12);
}
