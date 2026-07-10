#include <atomic>
#include <chrono>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    auto sticky_sleep(std::atomic<int>& done) {
        return kioto::just() | kioto::let_value([&done] {
            auto sched = *kioto_test::default_runtime::current_scheduler();
            return sched.schedule_after(20ms)
                 | kioto::then([&done] { done.fetch_add(1, std::memory_order_relaxed); });
        });
    }
}

TEST_CASE("join() to completion runs all work, then destruction is clean") {
    constexpr int n = 200;
    std::atomic<int> done{0};
    {
        kioto_test::default_runtime rt{3};
        for (int i = 0; i < n; ++i) rt.spawn(sticky_sleep(done));
        kioto::this_thread::sync_wait(rt.join());
        CHECK_EQ(done.load(), n);
    }
    CHECK_EQ(done.load(), n);
}

TEST_CASE("destroying a runtime without join() drains gracefully (no abort, no hang)") {
    constexpr int n = 500;
    std::atomic<int> done{0};
    {
        kioto_test::default_runtime rt{3};
        for (int i = 0; i < n; ++i) rt.spawn(sticky_sleep(done));
    }
    CHECK(done.load() <= n);
}

TEST_CASE("a runtime with no work at all destructs cleanly") {
    { kioto_test::default_runtime rt{2}; }
    CHECK(true);
}
