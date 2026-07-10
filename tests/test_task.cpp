#include <stdexcept>
#include <string>
#include <doctest/doctest.h>
#include <kioto/core.h>

namespace {
    auto add_one(int value) -> kioto::task<int> {
        co_return value + 1;
    }

    auto chain_task(int value) -> kioto::task<int> {
        co_return (co_await add_one(value)) * 2;
    }

    auto write_marker(std::string& marker) -> kioto::task<> {
        marker = "task<void>";
        co_return;
    }

    auto fail_task() -> kioto::task<int> {
        throw std::runtime_error{"task failure"};
    }

    auto check_scheduler(auto expected) -> kioto::task<> {
        auto this_scheduler = co_await kioto::execution::read_env(kioto::execution::get_scheduler);
        CHECK_EQ(this_scheduler, expected);
    }
}

TEST_CASE("task returns values and composes with co_await") {
    auto result = kioto::this_thread::sync_wait(chain_task(5));
    REQUIRE(result.has_value());

    auto [value] = result.value();
    CHECK_EQ(value, 12);
}

TEST_CASE("task<void> completes normally") {
    std::string marker;
    kioto::this_thread::sync_wait(write_marker(marker));
    CHECK_EQ(marker, "task<void>");
}

TEST_CASE("task propagates exceptions") {
    CHECK_THROWS_AS(kioto::this_thread::sync_wait(fail_task()), std::runtime_error);
}

TEST_CASE("task scheduler") {
    SUBCASE("run_loop") {
        kioto::execution::run_loop loop;
        auto loop_scheduler = loop.get_scheduler();
        using loop_scheduler_t = decltype(loop_scheduler);
        std::jthread thrd{[&] {
            loop.run();
        }};
        kioto::this_thread::sync_wait(kioto::starts_on(loop_scheduler, check_scheduler(loop_scheduler)));
        kioto::this_thread::sync_wait(kioto::starts_on(loop_scheduler, [&]() -> kioto::task<void, void, loop_scheduler_t> {
            auto this_scheduler = co_await kioto::execution::read_env(kioto::execution::get_scheduler);
            static_assert(std::same_as<loop_scheduler_t, decltype(this_scheduler)>);
            CHECK_EQ(loop_scheduler, this_scheduler);
        }()));
        loop.finish();
    }
    SUBCASE("inline_scheduler") {
        kioto::async_scope scope;
        scope.spawn(check_scheduler(kioto::execution::inline_scheduler{}));
        kioto::this_thread::sync_wait(scope.join());
    }
}
