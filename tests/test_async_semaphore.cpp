#include <string_view>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/exec/sync_primitives.h>

TEST_CASE("async_semaphore releases permits immediately when available") {
    kioto::async_semaphore<> sema{0};
    CHECK_FALSE(sema.try_acquire());
    sema.release();
    CHECK_EQ(sema.count(), 1);
    CHECK(sema.try_acquire());
    CHECK_EQ(sema.count(), 0);
}

TEST_CASE("async_semaphore resumes waiting tasks after release") {
    using namespace std::string_view_literals;

    kioto::async_semaphore<> sema1{0};
    kioto::async_semaphore<> sema2{0};
    std::vector<std::string_view> order;

    kioto::async_scope scope;

    scope.spawn(sema1.acquire() | kioto::then([&]() noexcept {
        order.emplace_back("#1");
        sema2.release();
    }));

    scope.spawn(sema2.acquire() | kioto::then([&]() noexcept {
        order.emplace_back("#2");
    }));

    order.emplace_back("#0");
    sema1.release();

    kioto::this_thread::sync_wait(scope.join());

    CHECK_EQ(sema1.count(), 0);
    CHECK_EQ(sema2.count(), 0);

    CHECK_EQ(order, std::vector{"#0"sv, "#1"sv, "#2"sv});
}
