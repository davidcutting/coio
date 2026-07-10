// Probe-degeneration sweep: does active latency probing fail SHARPLY or CONTINUOUSLY as scheduling
// latency rises? Little's Law predicts the achieved sample rate is min(1/I, L/W) for probe interval I,
// max_in_flight L, and per-probe latency W -> flat (interval-limited) below the knee W* = L*I, then a
// continuous 1/W decay (latency-limited) above it. This sweep holds ONE worker at a steady ready-queue
// depth D (self-replenishing tasks) to walk W across the predicted knee and measures samples gathered in
// a fixed window, so we can see the knee and confirm the falloff shape.
//
// Run: bench_probe_sweep  (registered as a single benchmark that runs the sweep once and prints to stderr)
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <benchmark/benchmark.h>
#include <coio/core.h>
#include <coio/asyncio/uring_context.h>
#include <coio/metrics.h>
#include <coio/metrics_reporter.h>
#include <coio/runtime.h>

namespace {
    using metered_uring = coio::basic_executor<coio::counting_metrics, coio::uring_driver>;
    using metered_runtime = coio::basic_runtime<metered_uring>;

    auto us(std::chrono::nanoseconds d) -> double { return std::chrono::duration<double, std::micro>(d).count(); }

    auto make_optional_runtime() -> std::optional<metered_runtime> {
        return std::optional<metered_runtime>{
            std::in_place, std::size_t{1},
            [](std::size_t) { return std::make_unique<metered_uring>(std::in_place, std::size_t{256}); }
        };
    }

    // A self-replenishing task: while running, spawn exactly one replacement on the same worker, then
    // finish. Seeding D of these holds the worker's local ready-queue at ~D (each completion adds one),
    // giving a steady scheduling latency W ~ (D/batch) * turn_cost for a probe landing behind them.
    auto pump(metered_runtime* rt, metered_runtime::worker_scheduler sched, std::atomic<bool>* running)
        -> metered_uring::task<> {
        if (running->load(std::memory_order_relaxed))
            rt->spawn_on(sched, pump(rt, sched, running));
        co_return;
    }

    struct point { long depth; std::uint64_t samples; double p50_us; double p99_us; };

    auto measure(long depth, std::chrono::milliseconds window,
                 std::chrono::microseconds probe_interval, int max_in_flight) -> point {
        auto rt = make_optional_runtime();
        std::atomic<bool> running{true};
        coio::runtime_report rep;
        {
            coio::metrics_reporter reporter{*rt};
            reporter.probe_scheduling(probe_interval, max_in_flight);
            auto sched = rt->pick_scheduler();
            // Bootstrap the D pumps FROM the worker (local spawns) so setup is cheap and the backlog is local.
            rt->spawn_on(sched, coio::just() | coio::let_value([rt = &*rt, sched, running = &running, depth] {
                for (long i = 0; i < depth; ++i) rt->spawn_on(sched, pump(rt, sched, running));
                return coio::just();
            }));
            coio::this_thread::sleep_for(window);   // let it reach steady state + gather probes
            rep = reporter.tick();                   // samples/latency over the window
            running.store(false, std::memory_order_relaxed);
        }   // reporter drained here (backlog now shrinking -> quick)
        coio::this_thread::sync_wait(rt->join());
        const auto& l = rep.scheduling_latency;
        return {depth, l.count, us(l.p50), us(l.p99)};
    }

    void probe_degeneration_sweep(benchmark::State& state) {
        constexpr auto window = std::chrono::milliseconds{300};
        constexpr auto I = std::chrono::microseconds{100};

        for (auto _ : state) {
            // Two max_in_flight settings: the knee W* = L*I should move with L (4 -> 400us, 64 -> 6.4ms),
            // confirming the model PREDICTS where probing degenerates (and giving the lever to push it out).
            for (int L : {4, 64}) {
                const double knee_us = static_cast<double>(L) * static_cast<double>(I.count());
                std::fprintf(stderr,
                    "\nprobe sweep: window %lld ms, interval %lld us, max_in_flight %d  =>  predicted knee W* = %.0f us\n",
                    static_cast<long long>(window.count()), static_cast<long long>(I.count()), L, knee_us);
                std::fprintf(stderr, "  %10s %10s %12s %12s %14s %14s\n",
                             "depth", "samples", "p50(us)", "p99(us)", "rate(1/s)", "model(1/s)");
                for (long depth : {0L, 500L, 2000L, 6000L, 20000L, 60000L, 200000L, 600000L}) {
                    const auto p = measure(depth, window, I, L);
                    const double secs = std::chrono::duration<double>(window).count();
                    const double rate = static_cast<double>(p.samples) / secs;
                    // model rate = min(1/I, L/W) using the measured p50 as W
                    const double w_s = p.p50_us / 1e6;
                    const double model = std::min(1.0 / (static_cast<double>(I.count()) / 1e6),
                                                  w_s > 0 ? static_cast<double>(L) / w_s : 1e18);
                    std::fprintf(stderr, "  %10ld %10llu %12.2f %12.2f %14.0f %14.0f\n",
                                 p.depth, static_cast<unsigned long long>(p.samples), p.p50_us, p.p99_us, rate, model);
                }
            }
        }
    }
    BENCHMARK(probe_degeneration_sweep)->Iterations(1)->UseRealTime();
}
