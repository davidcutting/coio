// The builder's .capability<>() : declare intent, the fastest available driver is auto-selected.
#include <atomic>
#include <concepts>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/driver/drivers.h>

// This suite asserts the LINUX driver set (io_uring leads, epoll fallback). On a build without io_uring
// (Windows/IOCP, or an epoll-only Linux build) it compiles to a skip marker — the capability machinery
// itself is backend-agnostic and exercised via the other runtime tests.
#if KIOTO_HAS_IO_URING

// Fast-path-first resolution is a compile-time fact of the build's driver registry.
static_assert(std::same_as<kioto::detail::resolve_t<kioto::default_drivers, kioto::capability::io>, kioto::uring_driver>,
              "networking resolves to io_uring (fast path) on this build");
static_assert(std::same_as<kioto::detail::resolve_t<kioto::default_drivers, kioto::capability::timer>, kioto::uring_driver>,
              "timer resolves to the first provider (io_uring) too");
static_assert(std::same_as<
                  kioto::detail::resolve_t<kioto::default_drivers, kioto::capability::io, kioto::capability::timer>,
                  kioto::uring_driver>,
              "a driver covering BOTH requested capabilities is chosen");
static_assert(std::same_as<kioto::detail::resolve_t<kioto::default_drivers, kioto::capability::file>, kioto::uring_driver>,
              "regular files resolve to io_uring (the only file-capable backend on this build)");
// cpu is a ROLE tag claimed only by the lightweight worker driver, so .capability<cpu>() picks it —
// NOT io_uring, even though io_uring is first in the fast-path registry and can also run CPU work.
static_assert(std::same_as<kioto::detail::resolve_t<kioto::default_drivers, kioto::capability::cpu>, kioto::timer_driver>,
              "cpu resolves to the lightweight worker driver, not the io_uring reactor");
// epoll does NOT provide file; an epoll-only registry can't serve files (would be a resolve static_assert).
static_assert(kioto::detail::resolve_index<kioto::type_list<kioto::epoll_driver, kioto::timer_driver>, kioto::capability::file>()
                  == kioto::type_list<kioto::epoll_driver, kioto::timer_driver>::npos,
              "no epoll/timer driver provides capability::file");

TEST_CASE("builder .capability<>(): auto-selects the driver and runs") {
    std::atomic<int> ok{0};
    {
        auto rt = kioto::runtime::builder()
            .pool(2).capability<kioto::capability::io>()   // -> 2x executor<uring_driver> (fast path, default ring size)
            .build();
        CHECK(rt.size() == 2);
        for (int i = 0; i < 8; ++i)
            rt.spawn_on<kioto::capability::io>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        kioto::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 8);
}

TEST_CASE("builder .capability<file>(): resolves to io_uring and routes file work there") {
    std::atomic<int> ok{0};
    {
        auto rt = kioto::runtime::builder()
            .pool(2).capability<kioto::capability::io, kioto::capability::file>()  // networking + files -> io_uring
            .build();
        CHECK(rt.size() == 2);
        for (int i = 0; i < 4; ++i)
            rt.spawn_on<kioto::capability::file>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        kioto::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 4);
}

TEST_CASE("builder .capability<cpu>(): a lightweight worker (thread) pool") {
    std::atomic<int> ok{0};
    {
        auto rt = kioto::runtime::builder()
            .pool(4).capability<kioto::capability::cpu>()   // -> 4x executor<timer_driver>, a thread pool
            .build();
        CHECK(rt.size() == 4);
        for (int i = 0; i < 16; ++i)
            rt.spawn_on<kioto::capability::cpu>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        kioto::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 16);
}

TEST_CASE("builder: capability and explicit driver pools coexist in one runtime") {
    std::atomic<int> ok{0};
    {
        auto rt = kioto::runtime::builder()
            .pool(2).capability<kioto::capability::io>()   // resolved -> uring
            .pool(2).driver<kioto::epoll_driver>()         // pinned -> epoll
            .build();
        CHECK(rt.size() == 4);
        for (int i = 0; i < 6; ++i)
            rt.spawn_on<kioto::capability::io>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        for (int i = 0; i < 6; ++i)
            rt.spawn_on_pool<1>(kioto::just() | kioto::then([&] { ok.fetch_add(1, std::memory_order_relaxed); }));
        kioto::this_thread::sync_wait(rt.join());
    }
    CHECK(ok.load() == 12);
}

#else
TEST_CASE("capability resolution (skipped: build has no io_uring)") { CHECK(true); }
#endif
