#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/runtime.h>
#include "io_contexts.h"

namespace {
    template<typename Runtime>
    auto spawn_counting(Runtime& rt, std::atomic<int>& done, int n) -> void {
        for (int i = 0; i < n; ++i) {
            rt.spawn(coio::just() | coio::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
        }
    }
}

TEST_CASE("balanced tier runs every spawned task and join() waits for quiescence") {
    constexpr int n = 500;

    auto run = [&](auto rt) {
        std::atomic<int> done{0};
        spawn_counting(rt, done, n);
        coio::this_thread::sync_wait(rt.join());
        CHECK_EQ(done.load(), n);
    };

#if COIO_HAS_IO_URING
    SUBCASE("uring") { run(coio::uring_runtime{3}); }
#endif
#if COIO_HAS_EPOLL
    SUBCASE("epoll") { run(coio_test::make_runtime<coio::epoll_context>(3)); }
#endif
#if COIO_HAS_IOCP
    SUBCASE("iocp") { run(coio_test::make_runtime<coio::iocp_context>(3)); }
#endif
}

TEST_CASE("balanced tier survives many cross-thread producers (no lost wakeups)") {
    constexpr int producers = 4;
    constexpr int per_producer = 500;
    constexpr int total = producers * per_producer;

    auto stress = [](auto rt) {
        std::atomic<int> done{0};
        std::vector<std::jthread> threads;
        for (int p = 0; p < producers; ++p) {
            threads.emplace_back([&rt, &done] {
                for (int i = 0; i < per_producer; ++i) {
                    rt.spawn(coio::just() | coio::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
                }
            });
        }
        for (auto& t : threads) t.join();
        coio::this_thread::sync_wait(rt.join());
        return done.load();
    };

#if COIO_HAS_IO_URING
    SUBCASE("uring") { CHECK_EQ(stress(coio::uring_runtime{3}), total); }
#endif
#if COIO_HAS_EPOLL
    SUBCASE("epoll") { CHECK_EQ(stress(coio_test::make_runtime<coio::epoll_context>(3)), total); }
#endif
#if COIO_HAS_IOCP
    SUBCASE("iocp") { CHECK_EQ(stress(coio_test::make_runtime<coio::iocp_context>(3)), total); }
#endif
}

TEST_CASE("current_scheduler() resolves to the local worker inside a balanced task") {
    constexpr int n = 100;
    coio_test::default_runtime rt{2};
    std::atomic<int> done{0};
    std::atomic<int> resolved{0};
    for (int i = 0; i < n; ++i) {
        rt.spawn(coio::just() | coio::then([&done, &resolved] {
            if (coio_test::default_runtime::current_scheduler().has_value()) {
                resolved.fetch_add(1, std::memory_order_relaxed);
            }
            done.fetch_add(1, std::memory_order_relaxed);
        }));
    }
    coio::this_thread::sync_wait(rt.join());
    CHECK_EQ(done.load(), n);
    CHECK_EQ(resolved.load(), n);
}
