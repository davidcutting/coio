// Construct executors via kioto::driver_init — single-driver-with-args and multi-driver in-place
// emplacement of non-movable drivers (the foundation of the heterogeneous-runtime builder).
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/runtime/init.h>
#include <kioto/io/time_loop.h>
#include <chrono>

using namespace std::chrono_literals;

// Exercises the uring + (epoll+timer) driver sets specifically; skip where io_uring is unavailable.
// driver_init itself (non-movable in-place emplacement) is backend-agnostic.
#if KIOTO_HAS_IO_URING
#include <kioto/io/driver/uring_context.h>
#include <kioto/io/driver/epoll_context.h>

TEST_CASE("driver_init: single-driver executor with ctor args") {
    kioto::uring_context ctx{ kioto::driver_init<kioto::uring_driver>(4096u) };  // was executor(in_place, 4096)
    auto sched = ctx.get_scheduler();
    bool ran = false;
    kioto::async_scope scope;
    // Pass sched/ran as coroutine PARAMETERS (stored in the frame), not by-reference lambda captures: an
    // immediately-invoked capturing lambda coroutine dangles — its closure is a temporary destroyed at the
    // end of this full-expression, before the coroutine resumes at ctx.run() (stack-use-after-scope).
    scope.spawn_on(sched, [](kioto::uring_context::scheduler sched, bool& ran) -> kioto::uring_context::task<> {
        co_await sched.schedule_after(1ms);
        ran = true;
    }(sched, ran));
    ctx.run();
    kioto::this_thread::sync_wait(scope.join());
    CHECK(ran);
}

TEST_CASE("driver_init: multi-driver executor emplaces BOTH non-movable drivers in place") {
    using multi = kioto::executor<kioto::epoll_driver, kioto::timer_driver>;
    multi ctx{ kioto::driver_init<kioto::epoll_driver>(), kioto::driver_init<kioto::timer_driver>() };
    auto sched = ctx.get_scheduler();
    bool ran = false;
    kioto::async_scope scope;
    scope.spawn_on(sched, [](multi::scheduler sched, bool& ran) -> multi::task<> {
        co_await sched.schedule_after(1ms);
        ran = true;
    }(sched, ran));
    ctx.run();
    kioto::this_thread::sync_wait(scope.join());
    CHECK(ran);
}

#else
TEST_CASE("driver_init (skipped: build has no io_uring)") { CHECK(true); }
#endif
