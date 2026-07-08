#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/runtime.h>
#include <coio/detail/execution.h>
#include <coio/detail/operation_base.h>

namespace {
    // A worker with no reactor: the runtime posts balanced ops onto it via post_node(); it drains them
    // and parks on a condvar otherwise. Satisfies runtime_worker, so basic_runtime can own and drive it.
    struct mock_worker {
        [[nodiscard]] auto get_scheduler() noexcept { return coio::execution::inline_scheduler{}; }
        auto work_started() noexcept -> void { ++work_count_; }
        auto work_finished() noexcept -> void { --work_count_; }

        auto post_node(coio::detail::operation_base& op) noexcept -> void {
            { std::scoped_lock lk{mtx_}; inbox_.push_back(&op); }
            cv_.notify_one();
        }

        auto notify() noexcept -> void {
            { std::scoped_lock lk{mtx_}; notified_ = true; }
            cv_.notify_one();
        }

        auto request_stop() -> void {
            { std::scoped_lock lk{mtx_}; stopped_ = true; }
            cv_.notify_one();
        }

        auto run() -> void {
            for (;;) {
                std::unique_lock lk{mtx_};
                if (inbox_.empty()) {
                    if (stopped_) break;
                    cv_.wait(lk, [&] { return not inbox_.empty() || stopped_; });
                    if (inbox_.empty()) continue;
                }
                auto* op = inbox_.front();
                inbox_.pop_front();
                lk.unlock();
                op->finish();
            }
        }

        std::atomic<int> work_count_{0};
        std::mutex mtx_;
        std::condition_variable cv_;
        std::deque<coio::detail::operation_base*> inbox_;
        bool notified_ = false;
        bool stopped_ = false;
    };

    static_assert(coio::runtime_worker<mock_worker>);
}

TEST_CASE("basic_runtime distributes balanced work across workers' inboxes (mock workers, no backend)") {
    coio::basic_runtime<mock_worker> runtime{
        3, [](std::size_t) { return std::make_unique<mock_worker>(); }
    };

    std::atomic<int> done{0};
    constexpr int task_count = 200;
    for (int i = 0; i < task_count; ++i) {
        runtime.spawn(coio::just() | coio::then([&done] {
            done.fetch_add(1, std::memory_order_relaxed);
        }));
    }
    coio::this_thread::sync_wait(runtime.join());

    CHECK_EQ(done.load(), task_count);
}
