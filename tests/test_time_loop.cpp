#include <atomic>
#include <optional>
#include <thread>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/execution_context.h>
#include <kioto/exec/async_scope.h>

TEST_CASE("time_loop runs work posted from other threads (single-owner runner)") {
    constexpr int n = 3000;
    constexpr int runners = 1; // single-owner: one thread drives the loop; work may be posted from any thread

    kioto::time_loop loop;
    kioto::async_scope scope;
    std::atomic<int> done{0};
    std::optional<kioto::work_guard<kioto::time_loop>> guard{std::in_place, loop};

    std::vector<std::jthread> drivers;
    for (int t = 0; t < runners; ++t) drivers.emplace_back([&loop] { loop.run(); });

    for (int i = 0; i < n; ++i) {
        scope.spawn_on(loop.get_scheduler(),
                       kioto::just() | kioto::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
    }

    guard.reset();
    kioto::this_thread::sync_wait(scope.join());
    loop.request_stop();
    for (auto& d : drivers) d.join();

    CHECK_EQ(done.load(), n);
}

TEST_CASE("time_loop accepts work posted concurrently while the runner is already spinning") {
    constexpr int producers = 3;
    constexpr int per_producer = 1000;
    constexpr int total = producers * per_producer;
    constexpr int runners = 1; // single-owner: one runner, many cross-thread producers

    kioto::time_loop loop;
    kioto::async_scope scope;
    std::atomic<int> done{0};
    std::optional<kioto::work_guard<kioto::time_loop>> guard{std::in_place, loop};

    std::vector<std::jthread> drivers;
    for (int t = 0; t < runners; ++t) drivers.emplace_back([&loop] { loop.run(); });

    std::vector<std::jthread> threads;
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&loop, &scope, &done] {
            for (int i = 0; i < per_producer; ++i) {
                scope.spawn_on(loop.get_scheduler(),
                               kioto::just() | kioto::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
            }
        });
    }
    for (auto& t : threads) t.join();

    guard.reset();
    kioto::this_thread::sync_wait(scope.join());
    loop.request_stop();
    for (auto& d : drivers) d.join();

    CHECK_EQ(done.load(), total);
}
