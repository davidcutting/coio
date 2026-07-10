#include <iostream>
#include <doctest/doctest.h>
#include <kioto/exec/execution.h>
#include <kioto/exec/polymorphic_scheduler.h>

TEST_CASE("scheduler concepts") {
    static_assert(kioto::execution::scheduler<kioto::polymorphic_scheduler>);
    static_assert(kioto::infallible_scheduler<kioto::polymorphic_scheduler, kioto::execution::env<>>);
}

TEST_CASE("polymorphic_scheduler equality") {
    kioto::execution::run_loop loop;
    using run_loop_scheduler = decltype(loop.get_scheduler());
    kioto::polymorphic_scheduler sched1(loop.get_scheduler());
    std::optional<run_loop_scheduler> loop_sched = sched1.target<run_loop_scheduler>();
    REQUIRE(loop_sched.has_value());
    CHECK_EQ(loop_sched.value(), loop.get_scheduler());
    kioto::polymorphic_scheduler sched2(loop.get_scheduler());
    kioto::polymorphic_scheduler sched3 = sched2;
    CHECK_EQ(sched1, loop.get_scheduler());
    CHECK_EQ(sched1, sched2);
    CHECK_EQ(sched1, sched3);
    kioto::polymorphic_scheduler sched4(kioto::execution::inline_scheduler{});
    CHECK_NE(sched1, sched4);
    CHECK_EQ(sched4, kioto::execution::inline_scheduler{});
}

TEST_CASE("polymorphic_scheduler forward-progress-guarantee") {
    kioto::execution::run_loop loop;
    kioto::polymorphic_scheduler sched1(loop.get_scheduler());
    CHECK_EQ(kioto::execution::get_forward_progress_guarantee(sched1), kioto::execution::forward_progress_guarantee::parallel);
    kioto::polymorphic_scheduler sched2(kioto::execution::inline_scheduler{});
    CHECK_EQ(kioto::execution::get_forward_progress_guarantee(sched2), kioto::execution::forward_progress_guarantee::weakly_parallel);
}

TEST_CASE("polymorphic_scheduler schedules inline scheduler") {
    kioto::polymorphic_scheduler sched(kioto::execution::inline_scheduler{});
    kioto::this_thread::sync_wait(kioto::execution::schedule(sched));
}
