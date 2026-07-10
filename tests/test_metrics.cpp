// Executor run-loop metrics: no_metrics is zero-cost; counting_metrics records activity.
#include <atomic>
#include <chrono>
#include <numeric>
#include <type_traits>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/driver/drivers.h>
#include <kioto/runtime/metrics.h>
#include <kioto/runtime/metrics_reporter.h>
#include <kioto/runtime/runtime.h>
#include <kioto/io/time_loop.h>

using namespace std::chrono_literals;

// The default policy must cost nothing — empty type, so [[no_unique_address]] makes it zero-size.
static_assert(std::is_empty_v<kioto::no_metrics>, "no_metrics must be zero-size (zero-cost default)");

namespace {
    using metered = kioto::basic_executor<kioto::counting_metrics, kioto::timer_driver>;

    auto sleeper(metered::scheduler sched) -> metered::task<> {
        co_await sched.schedule_after(1ms);
    }
}

TEST_CASE("counting_metrics records run-loop activity") {
    metered ex;                              // a metered time_loop worker
    auto sched = ex.get_scheduler();
    kioto::async_scope scope;
    for (int i = 0; i < 8; ++i)
        scope.spawn_on(sched, sleeper(sched));   // spawned from THIS (non-owner) thread -> cross-thread submits
    ex.run();                                // drives the sleepers to completion, then exits (work_count == 0)
    kioto::this_thread::sync_wait(scope.join());

    const auto s = ex.metrics().snapshot();
    CHECK(s.turns > 0);
    CHECK(s.ops_run >= 8);                   // at least the 8 sleeper completions
    CHECK(s.parks > 0);                      // parked waiting on the 1ms timers
    CHECK(s.submits_remote >= 8);            // the 8 spawns arrived cross-thread
    CHECK(s.parked_time > std::chrono::nanoseconds{0});  // idle time accrued while parked
}

TEST_CASE("basic_runtime aggregates per-worker metrics") {
    // A metered homogeneous runtime: every worker carries counting_metrics.
    using metered_worker = kioto::basic_executor<kioto::counting_metrics, kioto::timer_driver>;
    kioto::basic_runtime<metered_worker> rt{4};
    {
        kioto::async_scope scope;
        for (int i = 0; i < 32; ++i) {
            auto sched = rt.pick_scheduler();
            scope.spawn_on(sched, sleeper(sched));
        }
        kioto::this_thread::sync_wait(scope.join());
    }

    const auto per_worker = rt.collect_metrics();
    CHECK(per_worker.size() == 4);                       // one snapshot per worker

    const auto total = rt.aggregate_metrics();
    CHECK(total.ops_run >= 32);                          // all 32 sleepers completed somewhere
    // aggregate == field-wise sum of the per-worker snapshots
    const auto summed = std::accumulate(per_worker.begin(), per_worker.end(), kioto::executor_stats{});
    CHECK(summed.ops_run == total.ops_run);
    CHECK(summed.turns == total.turns);
}

TEST_CASE("runtime builder threads a metrics policy through every worker") {
    auto rt = kioto::runtime::builder()
        .metrics<kioto::counting_metrics>()
        .pool(2).capability<kioto::capability::timer>()
        .build();
    CHECK(rt.size() == 2);

    std::atomic<int> ran{0};
    for (int i = 0; i < 16; ++i)
        rt.spawn_on<kioto::capability::timer>(kioto::just() | kioto::then([&] { ran.fetch_add(1); }));
    kioto::this_thread::sync_wait(rt.join());
    CHECK(ran.load() == 16);

    // Every worker was built with counting_metrics -> the runtime can now aggregate.
    const auto per_worker = rt.collect_metrics();
    CHECK(per_worker.size() == 2);
    const auto total = rt.aggregate_metrics();
    CHECK(total.ops_run >= 16);         // the 16 routed ops ran across the two workers
    CHECK(total.turns > 0);
}

TEST_CASE("metrics_reporter diffs snapshots into rates") {
    using metered_worker = kioto::basic_executor<kioto::counting_metrics, kioto::timer_driver>;
    kioto::basic_runtime<metered_worker> rt{2};

    kioto::metrics_reporter reporter{rt};   // baseline snapshot taken here (before any work)
    {
        kioto::async_scope scope;
        for (int i = 0; i < 20; ++i) {
            auto sched = rt.pick_scheduler();
            scope.spawn_on(sched, sleeper(sched));
        }
        kioto::this_thread::sync_wait(scope.join());
    }

    const auto rep = reporter.tick();      // interval = construction -> now, with 20 ops of work in it
    CHECK(rep.interval > std::chrono::nanoseconds{0});
    CHECK(rep.per_worker.size() == 2);
    CHECK(rep.per_worker_rates.size() == 2);
    CHECK(rep.aggregate.ops_run >= 20);            // cumulative
    CHECK(rep.aggregate_rates.ops_per_sec > 0.0);  // rate over the interval
    CHECK(rep.aggregate_rates.utilization >= 0.0);
    CHECK(rep.aggregate_rates.utilization <= 1.0);

    // A second tick immediately after, with no work, shows (near) zero throughput but a valid interval.
    const auto idle = reporter.tick();
    CHECK(idle.interval > std::chrono::nanoseconds{0});
    CHECK(idle.aggregate.ops_run == rep.aggregate.ops_run);  // no new ops -> cumulative unchanged
}

TEST_CASE("metrics_reporter run_every drives a sink off a side thread") {
    using metered_worker = kioto::basic_executor<kioto::counting_metrics, kioto::timer_driver>;
    kioto::basic_runtime<metered_worker> rt{2};

    std::atomic<int> reports{0};
    std::atomic<std::size_t> last_ops{0};
    kioto::metrics_reporter reporter{rt};
    reporter.run_every(5ms, [&](const kioto::runtime_report& r) {
        reports.fetch_add(1, std::memory_order_relaxed);
        last_ops.store(r.aggregate.ops_run, std::memory_order_relaxed);
    });

    {
        kioto::async_scope scope;
        for (int i = 0; i < 20; ++i) {
            auto sched = rt.pick_scheduler();
            scope.spawn_on(sched, sleeper(sched));
        }
        kioto::this_thread::sync_wait(scope.join());
    }
    kioto::this_thread::sleep_for(30ms);   // let a few ticks fire
    reporter.stop();                       // prompt, interruptible

    CHECK(reports.load() >= 1);            // the sink was called
    CHECK(last_ops.load() >= 20);          // and saw the completed work
}

TEST_CASE("latency_histogram percentiles") {
    kioto::latency_histogram h;
    for (int i = 0; i < 100; ++i) h.record(std::chrono::microseconds{1});   // ~1024ns -> bucket 10
    h.record(std::chrono::milliseconds{1});                                  // a single tail sample
    const auto s = h.snapshot();
    CHECK(s.count == 101);
    // 1us is in [1024ns, 2048ns): p50 lower edge is 1024ns; the 1ms outlier is the max, well above p50.
    CHECK(s.p50 >= std::chrono::nanoseconds{512});
    CHECK(s.p50 <= std::chrono::microseconds{2});
    CHECK(s.max >= std::chrono::microseconds{500});
    CHECK(s.p99 <= s.max);
}

TEST_CASE("metrics_reporter probes submit->run scheduling latency") {
    using metered_worker = kioto::basic_executor<kioto::counting_metrics, kioto::timer_driver>;
    kioto::basic_runtime<metered_worker> rt{2};

    kioto::metrics_reporter reporter{rt};
    reporter.probe_scheduling(200us);      // start the probe thread

    // Let probes accumulate against a mostly-idle runtime (near-zero scheduling delay expected).
    kioto::this_thread::sleep_for(60ms);

    const auto rep = reporter.tick();
    CHECK(rep.has_latency);
    CHECK(rep.scheduling_latency.count > 0);              // probes landed and recorded
    CHECK(rep.scheduling_latency.p50 >= std::chrono::nanoseconds{0});
    CHECK(rep.scheduling_latency.p99 <= rep.scheduling_latency.max);
}
