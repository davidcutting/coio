#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
#include <coio/core.h>                 // just / then / sync_wait / execution::scheduler
#include <coio/metrics.h>
#include <coio/utils/async_scope.h>    // async_scope (probe lifetime)

// Tier-A metrics export: a side-thread (or caller-driven) scraper that turns a metered runtime's monotonic
// per-worker snapshots into RATES by diffing successive samples over the elapsed interval. It sits ENTIRELY
// off the hot path — it only calls the runtime's collect_metrics() (relaxed reads) — and is never a driver
// and never routed. Sink is a user callback, so you can log it, push it to your own metrics library, or
// format it yourself; coio owns no HTTP endpoint / OTel dependency.
//
//     coio::metrics_reporter reporter{rt};                 // baseline snapshot taken here
//     reporter.run_every(1s, [](const coio::runtime_report& r) {
//         std::println("throughput {:.0f} ops/s, utilization {:.0%}",
//                      r.aggregate_rates.ops_per_sec, r.aggregate_rates.utilization);
//     });
//     // ... or drive it yourself from an existing loop: auto r = reporter.tick();
namespace coio {
    // Per-interval derived rates for one executor (or the aggregate), computed by diffing two snapshots.
    struct executor_rates {
        double turns_per_sec;
        double ops_per_sec;
        double parks_per_sec;
        double wakes_per_sec;           // the "wakeup storm" rate
        double submits_local_per_sec;
        double submits_remote_per_sec;
        double utilization;             // 1 - Δparked/Δ(wall·workers), clamped [0,1]
        double fast_path_ratio;         // local / (local + remote) submits over the interval
    };

    // The delta between two snapshots over `dt`, as rates. `workers` scales the utilization denominator so an
    // aggregate over N workers reads 0..1 (pass 1 for a single executor). cur >= prev field-wise (monotonic).
    [[nodiscard]] inline auto rates_between(const executor_stats& prev, const executor_stats& cur,
                                            std::chrono::nanoseconds dt, std::size_t workers = 1) noexcept
        -> executor_rates {
        const double secs = std::chrono::duration<double>(dt).count();
        const auto per = [&](std::size_t d) noexcept { return secs > 0.0 ? static_cast<double>(d) / secs : 0.0; };
        const std::size_t local = cur.submits_local - prev.submits_local;
        const std::size_t remote = cur.submits_remote - prev.submits_remote;
        const std::size_t submits = local + remote;
        const auto parked_delta = (cur.parked_time - prev.parked_time).count();
        const double wall = secs * static_cast<double>(workers == 0 ? 1 : workers);
        const double util = wall > 0.0
            ? std::clamp(1.0 - std::chrono::duration<double>(std::chrono::nanoseconds{parked_delta}).count() / wall, 0.0, 1.0)
            : 0.0;
        return {
            per(cur.turns - prev.turns),
            per(cur.ops_run - prev.ops_run),
            per(cur.parks - prev.parks),
            per(cur.wakes - prev.wakes),
            per(local),
            per(remote),
            util,
            submits > 0 ? static_cast<double>(local) / static_cast<double>(submits) : 0.0,
        };
    }

    // One sample of a runtime: the current cumulative snapshots + the rates over the interval since the
    // previous tick (per worker and aggregated). `interval` is the actual measured wall time between samples.
    struct runtime_report {
        std::chrono::nanoseconds interval;
        std::vector<executor_stats> per_worker;         // current CUMULATIVE counters, one per worker
        std::vector<executor_rates> per_worker_rates;   // rates over `interval`, one per worker
        executor_stats aggregate;                       // cumulative totals across all workers
        executor_rates aggregate_rates;                 // rates over `interval`, aggregated
        bool has_latency = false;                       // true iff probe_scheduling() is running
        latency_stats scheduling_latency{};             // CUMULATIVE submit->run percentiles (probe stream)
    };

    // A runtime this reporter can scrape: anything exposing collect_metrics() (basic_runtime /
    // heterogeneous_runtime built with a snapshotting metrics policy).
    template<typename R>
    concept metrics_source = requires(const R& r) {
        { r.collect_metrics() } -> std::convertible_to<std::vector<executor_stats>>;
    };

    // A runtime a reporter can PROBE for scheduling latency: it must hand back a scheduler to spawn a probe
    // onto (basic_runtime does; heterogeneous_runtime routes by capability instead, so it isn't probeable
    // this way — deferred).
    template<typename R>
    concept probeable_runtime = requires(R& r) {
        { r.get_scheduler() } -> execution::scheduler;
    };

    // Owns a reference to the runtime + the previous sample. NON-movable: run_every() spawns a thread that
    // captures `this`, so the reporter must be pinned (construct it as a named local, like the runtime).
    // Single-consumer: tick() mutates the stored baseline — call it from ONE place (the run_every thread, or
    // your own loop, not both).
    template<metrics_source Runtime>
    class metrics_reporter {
        using clock = std::chrono::steady_clock;

    public:
        // Takes the baseline snapshot now, so the first tick()'s rates cover construction -> first tick.
        explicit metrics_reporter(Runtime& rt)
            : rt_(&rt), prev_(rt.collect_metrics()), prev_time_(clock::now()) {}

        metrics_reporter(const metrics_reporter&) = delete;
        auto operator= (const metrics_reporter&) -> metrics_reporter& = delete;

        // Stops both threads, then drains any probes still in flight. The runtime MUST still be alive here
        // (probes run on its workers) — so declare the reporter AFTER the runtime, and never tear the
        // runtime down while a reporter is probing it.
        ~metrics_reporter() {
            stop();
            coio::this_thread::sync_wait(probe_scope_.join());
        }

        // Sample once: diff the current snapshots against the stored baseline, then adopt them as the new
        // baseline. Cheap (relaxed reads + arithmetic); safe to call from your own timer/health loop.
        [[nodiscard]] auto tick() -> runtime_report {
            const auto now = clock::now();
            auto cur = rt_->collect_metrics();
            const auto dt = now - prev_time_;

            runtime_report rep;
            rep.interval = dt;
            rep.per_worker_rates.reserve(cur.size());
            executor_stats agg_prev{}, agg_cur{};
            for (std::size_t i = 0; i < cur.size(); ++i) {
                const auto prev = i < prev_.size() ? prev_[i] : executor_stats{};
                rep.per_worker_rates.push_back(rates_between(prev, cur[i], dt));
                agg_prev = agg_prev + prev;
                agg_cur = agg_cur + cur[i];
            }
            rep.aggregate = agg_cur;
            rep.aggregate_rates = rates_between(agg_prev, agg_cur, dt, cur.size());
            rep.per_worker = std::move(cur);
            if (probing_.load(std::memory_order_relaxed)) {
                rep.has_latency = true;
                rep.scheduling_latency = latency_.snapshot();
            }

            prev_ = rep.per_worker;
            prev_time_ = now;
            return rep;
        }

        // Start sampling submit->run scheduling latency: a side thread fires a self-timing probe onto the
        // runtime every `interval`, recording (run_time - submit_time) into the histogram. tick()'s report
        // then carries scheduling_latency (cumulative percentiles). At most `max_in_flight` probes are
        // outstanding, so a saturated runtime throttles the probe rate itself (honest backpressure). The
        // probe is a bare schedule+then — it perturbs the runtime only as much as one trivial task per tick.
        auto probe_scheduling(std::chrono::nanoseconds interval = std::chrono::microseconds{500},
                              int max_in_flight = 4) -> void
            requires probeable_runtime<Runtime> {
            probing_.store(true, std::memory_order_relaxed);
            probe_thread_ = std::jthread([this, interval, max_in_flight](std::stop_token st) {
                std::mutex m;
                std::condition_variable_any cv;
                std::unique_lock lk(m);
                while (not st.stop_requested()) {
                    cv.wait_for(lk, st, interval, [&] { return st.stop_requested(); });
                    if (st.stop_requested()) break;
                    if (in_flight_.load(std::memory_order_relaxed) < max_in_flight) fire_probe();
                }
            });
        }

        // Own a thread that calls sink(tick()) every `interval` until stop() (or destruction). The wait is
        // interruptible, so stop() returns promptly. One reporter drives at most one such thread.
        template<typename Sink>
        auto run_every(std::chrono::nanoseconds interval, Sink sink) -> void {
            thread_ = std::jthread([this, interval, sink = std::move(sink)](std::stop_token st) mutable {
                std::mutex m;
                std::condition_variable_any cv;
                std::unique_lock lk(m);
                while (not st.stop_requested()) {
                    cv.wait_for(lk, st, interval, [&] { return st.stop_requested(); });
                    if (st.stop_requested()) break;
                    sink(tick());
                }
            });
        }

        // Stop and join the run_every + probe threads (no-op for any not running). Does NOT drain in-flight
        // probes — the destructor does that (it can sync_wait; stop() may be called from anywhere).
        auto stop() -> void {
            if (thread_.joinable()) { thread_.request_stop(); thread_.join(); }
            if (probe_thread_.joinable()) { probe_thread_.request_stop(); probe_thread_.join(); }
        }

    private:
        // Spawn one self-timing probe: t0 is stamped now (submit), the `then` runs on a worker (submit->run
        // elapsed = scheduling latency). async_scope owns the op's lifetime; `this` stays valid because the
        // destructor joins the scope before any member dies.
        auto fire_probe() -> void {
            const auto t0 = clock::now();
            in_flight_.fetch_add(1, std::memory_order_relaxed);
            probe_scope_.spawn_on(rt_->get_scheduler(), coio::just() | coio::then([this, t0]() noexcept {
                latency_.record(clock::now() - t0);
                in_flight_.fetch_sub(1, std::memory_order_relaxed);
            }));
        }

        Runtime* rt_;
        std::vector<executor_stats> prev_;
        clock::time_point prev_time_;
        std::jthread thread_;

        // Latency probing (only active after probe_scheduling()).
        async_scope probe_scope_;
        latency_histogram latency_;
        std::atomic<bool> probing_{false};
        std::atomic<int> in_flight_{0};
        std::jthread probe_thread_;
    };
}
