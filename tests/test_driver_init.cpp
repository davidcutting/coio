// Construct executors via coio::driver_init — single-driver-with-args and multi-driver in-place
// emplacement of non-movable drivers (the foundation of the heterogeneous-runtime builder).
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/init.h>
#include <coio/time_loop.h>
#include <coio/asyncio/uring_context.h>
#include <coio/asyncio/epoll_context.h>
#include <chrono>

using namespace std::chrono_literals;

TEST_CASE("driver_init: single-driver executor with ctor args") {
    coio::uring_context ctx{ coio::driver_init<coio::uring_driver>(4096u) };  // was executor(in_place, 4096)
    auto sched = ctx.get_scheduler();
    bool ran = false;
    coio::async_scope scope;
    scope.spawn_on(sched, [&]() -> coio::uring_context::task<> {
        co_await sched.schedule_after(1ms);
        ran = true;
    }());
    ctx.run();
    coio::this_thread::sync_wait(scope.join());
    CHECK(ran);
}

TEST_CASE("driver_init: multi-driver executor emplaces BOTH non-movable drivers in place") {
    using multi = coio::executor<coio::epoll_driver, coio::timer_driver>;
    multi ctx{ coio::driver_init<coio::epoll_driver>(), coio::driver_init<coio::timer_driver>() };
    auto sched = ctx.get_scheduler();
    bool ran = false;
    coio::async_scope scope;
    scope.spawn_on(sched, [&]() -> multi::task<> {
        co_await sched.schedule_after(1ms);
        ran = true;
    }());
    ctx.run();
    coio::this_thread::sync_wait(scope.join());
    CHECK(ran);
}
