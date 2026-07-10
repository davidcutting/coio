#include <string_view>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/exec/sync_primitives.h>

TEST_CASE("async_latch waits until the counter reaches zero") {
    using namespace std::string_view_literals;

    kioto::async_latch<> latch{3};
    std::vector<std::string_view> order;
    kioto::async_scope scope;

    scope.spawn(latch.wait() | kioto::then([&]() noexcept {
        order.emplace_back("#2");
    }));

    scope.spawn(latch.arrive_and_wait(2) | kioto::then([&]() noexcept {
        order.emplace_back("#1");
    }));

    CHECK_EQ(latch.count(), 1);
    CHECK_FALSE(latch.try_wait());
    latch.count_down();

    kioto::this_thread::sync_wait(scope.join());

    CHECK(latch.try_wait());
    CHECK_EQ(order, std::vector{"#1"sv, "#2"sv});
}
