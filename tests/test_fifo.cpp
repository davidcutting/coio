#include <string>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/base/fifo.h>

TEST_CASE("fifo preserves order with try operations") {
    kioto::fifo<std::string> queue;

    CHECK(queue.empty());
    CHECK(queue.try_push("one"));
    CHECK(queue.try_push("two"));
    CHECK_EQ(queue.size(), 2);

    auto first = queue.try_pop();
    REQUIRE(first.has_value());
    CHECK_EQ(*first, "one");

    auto second = queue.try_pop();
    REQUIRE(second.has_value());
    CHECK_EQ(*second, "two");

    CHECK_FALSE(queue.try_pop().has_value());
}

TEST_CASE("fifo hands off values between async producers and consumers") {
    kioto::fifo<std::string> queue;
    std::vector<std::string> popped;
    popped.reserve(3);

    kioto::this_thread::sync_wait(kioto::when_all(
        [&]() -> kioto::task<> {
            popped.push_back(co_await queue.async_pop());
            popped.push_back(co_await queue.async_pop());
            popped.push_back(co_await queue.async_pop());
        }(),
        [&]() -> kioto::task<> {
            co_await queue.async_push("one");
            co_await queue.async_push("two");
            co_await queue.async_push("three");
        }()
    ));

    CHECK_EQ(popped, std::vector<std::string>{"one", "two", "three"});
}
