#include <atomic>
#include <optional>
#include <thread>
#include <vector>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/execution_context.h>
#include <coio/utils/async_scope.h>

TEST_CASE("time_loop runs work posted from another thread across several concurrent runners") {
    constexpr int n = 3000;
    constexpr int runners = 4;

    coio::time_loop loop;
    coio::async_scope scope;
    std::atomic<int> done{0};
    std::optional<coio::work_guard<coio::time_loop>> guard{std::in_place, loop};

    std::vector<std::jthread> drivers;
    for (int t = 0; t < runners; ++t) drivers.emplace_back([&loop] { loop.run(); });

    for (int i = 0; i < n; ++i) {
        scope.spawn_on(loop.get_scheduler(),
                       coio::just() | coio::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
    }

    guard.reset();
    coio::this_thread::sync_wait(scope.join());
    loop.request_stop();
    for (auto& d : drivers) d.join();

    CHECK_EQ(done.load(), n);
}

TEST_CASE("time_loop accepts work posted concurrently while runners are already spinning") {
    constexpr int producers = 3;
    constexpr int per_producer = 1000;
    constexpr int total = producers * per_producer;
    constexpr int runners = 3;

    coio::time_loop loop;
    coio::async_scope scope;
    std::atomic<int> done{0};
    std::optional<coio::work_guard<coio::time_loop>> guard{std::in_place, loop};

    std::vector<std::jthread> drivers;
    for (int t = 0; t < runners; ++t) drivers.emplace_back([&loop] { loop.run(); });

    std::vector<std::jthread> threads;
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&loop, &scope, &done] {
            for (int i = 0; i < per_producer; ++i) {
                scope.spawn_on(loop.get_scheduler(),
                               coio::just() | coio::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
            }
        });
    }
    for (auto& t : threads) t.join();

    guard.reset();
    coio::this_thread::sync_wait(scope.join());
    loop.request_stop();
    for (auto& d : drivers) d.join();

    CHECK_EQ(done.load(), total);
}
