#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/runtime.h>
#include <coio/uring_runtime.h>
#include <coio/asyncio/epoll_context.h>

namespace {
    auto make_epoll_runtime(std::size_t n) {
        return coio::basic_runtime<coio::epoll_context>{
            n, [](std::size_t) { return std::make_unique<coio::epoll_context>(); }
        };
    }

    template<typename Runtime>
    auto spawn_counting(Runtime& rt, std::atomic<int>& done, int n) -> void {
        for (int i = 0; i < n; ++i) {
            rt.spawn(coio::just() | coio::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
        }
    }
}

TEST_CASE("balanced tier runs every spawned task and join() waits for quiescence") {
    constexpr int n = 500;

    SUBCASE("uring") {
        coio::uring_runtime rt{3};
        std::atomic<int> done{0};
        spawn_counting(rt, done, n);
        coio::this_thread::sync_wait(rt.join());
        CHECK_EQ(done.load(), n);
    }
    SUBCASE("epoll") {
        auto rt = make_epoll_runtime(3);
        std::atomic<int> done{0};
        spawn_counting(rt, done, n);
        coio::this_thread::sync_wait(rt.join());
        CHECK_EQ(done.load(), n);
    }
}

TEST_CASE("balanced tier survives many cross-thread producers (no lost wakeups)") {
    constexpr int producers = 4;
    constexpr int per_producer = 500;
    constexpr int total = producers * per_producer;

    auto stress = [](auto& rt) {
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

    SUBCASE("uring") {
        coio::uring_runtime rt{3};
        CHECK_EQ(stress(rt), total);
    }
    SUBCASE("epoll") {
        auto rt = make_epoll_runtime(3);
        CHECK_EQ(stress(rt), total);
    }
}

TEST_CASE("current_scheduler() resolves to the local worker inside a balanced task") {
    constexpr int n = 100;
    coio::uring_runtime rt{2};
    std::atomic<int> done{0};
    std::atomic<int> resolved{0};
    for (int i = 0; i < n; ++i) {
        rt.spawn(coio::just() | coio::then([&done, &resolved] {
            if (coio::uring_runtime::current_scheduler().has_value()) {
                resolved.fetch_add(1, std::memory_order_relaxed);
            }
            done.fetch_add(1, std::memory_order_relaxed);
        }));
    }
    coio::this_thread::sync_wait(rt.join());
    CHECK_EQ(done.load(), n);
    CHECK_EQ(resolved.load(), n);
}
