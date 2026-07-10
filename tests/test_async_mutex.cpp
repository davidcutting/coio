#include <thread>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/exec/sync_primitives.h>

namespace {
    class worker {
    public:
        worker() {
            thrd = std::jthread([this] {
                loop.run();
            });
        }

        auto get_scheduler() noexcept {
            return loop.get_scheduler();
        }

    private:
        kioto::time_loop loop;
        std::jthread thrd;
        kioto::work_guard<kioto::time_loop> _{loop};
    };
}

TEST_CASE("async_mutex serializes access between waiters") {
    kioto::async_mutex mutex;
    worker workers[4];
    std::size_t result = 0;
    constexpr std::size_t n = 16;
    kioto::async_scope scope;
    for (std::size_t i = 0; i < n; ++i) {
        auto sched = workers[i % std::ranges::size(workers)].get_scheduler();
        scope.spawn(
            kioto::schedule(sched)
            | kioto::let_value([&]() noexcept { return mutex.lock_guard(); })
            | kioto::then([&result, i](auto) noexcept { result += i; })
        );
    }

    kioto::this_thread::sync_wait(scope.join());

    CHECK_EQ(result, 120); // 0 + 1 + ... + 15 == 120
}
