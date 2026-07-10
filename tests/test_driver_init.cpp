// Construct executors via coio::driver_init — single-driver-with-args and multi-driver in-place
// emplacement of non-movable drivers (the foundation of the heterogeneous-runtime builder).
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/init.h>
#include <coio/time_loop.h>
#include <chrono>

using namespace std::chrono_literals;

// Exercises the uring + (epoll+timer) driver sets specifically; skip where io_uring is unavailable.
// driver_init itself (non-movable in-place emplacement) is backend-agnostic.
#if COIO_HAS_IO_URING
#include <coio/asyncio/uring_context.h>
#include <coio/asyncio/epoll_context.h>

TEST_CASE("driver_init: single-driver executor with ctor args") {
    coio::uring_context ctx{ coio::driver_init<coio::uring_driver>(4096u) };  // was executor(in_place, 4096)
    auto sched = ctx.get_scheduler();
    bool ran = false;
    coio::async_scope scope;
    // Pass sched/ran as coroutine PARAMETERS (stored in the frame), not by-reference lambda captures: an
    // immediately-invoked capturing lambda coroutine dangles — its closure is a temporary destroyed at the
    // end of this full-expression, before the coroutine resumes at ctx.run() (stack-use-after-scope).
    scope.spawn_on(sched, [](coio::uring_context::scheduler sched, bool& ran) -> coio::uring_context::task<> {
        co_await sched.schedule_after(1ms);
        ran = true;
    }(sched, ran));
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
    scope.spawn_on(sched, [](multi::scheduler sched, bool& ran) -> multi::task<> {
        co_await sched.schedule_after(1ms);
        ran = true;
    }(sched, ran));
    ctx.run();
    coio::this_thread::sync_wait(scope.join());
    CHECK(ran);
}

#else
TEST_CASE("driver_init (skipped: build has no io_uring)") { CHECK(true); }
#endif
