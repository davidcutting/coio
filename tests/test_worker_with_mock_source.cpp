#include <atomic>
#include <cstddef>
#include <vector>
#include <doctest/doctest.h>
#include <coio/asyncio/uring_context.h>
#include <coio/detail/operation_base.h>

namespace {
    struct counting_op : coio::detail::operation_base {
        std::atomic<int>* counter = nullptr;
        auto finish() -> void override { counter->fetch_add(1, std::memory_order_relaxed); }
    };
}

TEST_CASE("a single-issuer worker runs operations posted to its inbox and then drains") {
    coio::uring_context worker;

    std::atomic<int> counter{0};
    constexpr int op_count = 50;
    std::vector<counting_op> pool(op_count);
    for (auto& op : pool) {
        op.counter = &counter;
        worker.post_node(op);
    }

    worker.run();

    CHECK_EQ(counter.load(), op_count);
}
