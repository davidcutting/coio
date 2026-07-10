// Dogfooding the metrics_reporter against the fan-out benchmarks. Mirrors bench_io's multi_threaded_run
// (external single producer -> N idle workers, the wakeup-storm shape) and multi_threaded_inworker_run
// (N in-worker producers cross-posting, work-begets-work), but on a METERED runtime so we can read what
// the runtime was actually doing during the timed region.
//
// The reporter's baseline is taken right before the timed work and one tick() right after, so the report
// covers the whole run (the "construct -> run -> single tick" idiom). Aggregate rates are emitted as
// Google Benchmark counters; the per-worker breakdown (util spread, wake share) is printed to stderr once.
//
//   Counters:  ops/s   utilization   wake/s   remote_frac   (see below)
#include <cstddef>
#include <cstdio>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <utility>
#include <benchmark/benchmark.h>
#include <coio/core.h>
#include <coio/asyncio/uring_context.h>
#include <coio/metrics.h>
#include <coio/metrics_reporter.h>
#include <coio/runtime.h>

namespace {
    // A uring worker with the counting metrics policy plugged in (vs. uring_context = no_metrics).
    using metered_uring = coio::basic_executor<coio::counting_metrics, coio::uring_driver>;
    using metered_runtime = coio::basic_runtime<metered_uring>;

    auto make_runtime(std::size_t workers, std::size_t entries) {
        return std::optional<metered_runtime>{
            std::in_place, workers,
            [entries](std::size_t) { return std::make_unique<metered_uring>(std::in_place, entries); }
        };
    }

    auto us(std::chrono::nanoseconds d) -> double { return std::chrono::duration<double, std::micro>(d).count(); }

    // Feed the whole-run report into the benchmark's counters + print the breakdown (once per config).
    auto record(benchmark::State& state, const coio::runtime_report& rep, const char* tag, std::size_t workers) -> void {
        const auto& a = rep.aggregate_rates;
        state.counters["ops/s"]      = benchmark::Counter(a.ops_per_sec);
        state.counters["util"]       = benchmark::Counter(a.utilization);
        state.counters["wake/s"]     = benchmark::Counter(a.wakes_per_sec);
        state.counters["remote_frac"]= benchmark::Counter(1.0 - a.fast_path_ratio);
        const auto& l = rep.scheduling_latency;
        if (rep.has_latency) {
            state.counters["lat_p50_us"] = benchmark::Counter(us(l.p50));
            state.counters["lat_p99_us"] = benchmark::Counter(us(l.p99));
        }

        // One detailed dump per (tag, workers) pair — key a set so each config prints once.
        static std::mutex mu;
        static std::set<std::pair<const char*, std::size_t>> seen;
        std::scoped_lock lk(mu);
        if (not seen.emplace(tag, workers).second) return;
        std::fprintf(stderr, "\n[%s] %zu workers, interval %.2f ms\n", tag, rep.per_worker.size(),
                     std::chrono::duration<double, std::milli>(rep.interval).count());
        std::fprintf(stderr, "  throughput %.2fM ops/s  util %.0f%%  wake/s %.0f  remote_frac %.2f\n",
                     a.ops_per_sec / 1e6, a.utilization * 100.0, a.wakes_per_sec, 1.0 - a.fast_path_ratio);
        if (rep.has_latency)
            std::fprintf(stderr, "  sched latency (submit->run, %llu probes): p50 %.2fus  p99 %.2fus  max %.2fus\n",
                         static_cast<unsigned long long>(l.count), us(l.p50), us(l.p99), us(l.max));
    }

    // multi_threaded_run shape: one external producer spawns `tasks` trivial ops, round-robined onto the
    // pool. N workers otherwise idle -> each cross-post may need a wakeup. The wakeup-storm stressor.
    void metered_external_producer(benchmark::State& state) {
        const auto workers = static_cast<std::size_t>(state.range(0));
        constexpr long tasks = 100'000;
        for (auto _ : state) {
            state.PauseTiming();
            auto rt = make_runtime(workers, 256);
            coio::runtime_report rep;
            {
                coio::metrics_reporter reporter{*rt};   // baseline
                reporter.probe_scheduling(std::chrono::microseconds{100});
                state.ResumeTiming();

                for (long i = 0; i < tasks; ++i) rt->spawn(coio::just());
                coio::this_thread::sync_wait(rt->join());

                state.PauseTiming();
                rep = reporter.tick();
            }   // reporter destroyed here: drains probes while rt is still alive
            record(state, rep, "external_producer", workers);
            rt.reset();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * tasks);
    }
    BENCHMARK(metered_external_producer)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();

    // multi_threaded_inworker_run shape: one producer seeded per worker (lands on-worker), each spawns its
    // 1/N share via the round-robin runtime spawn. Work begets work on-worker -> the fast (local) path.
    void metered_inworker_producer(benchmark::State& state) {
        const auto workers = static_cast<std::size_t>(state.range(0));
        constexpr long tasks = 100'000;
        const long per = tasks / static_cast<long>(workers);
        for (auto _ : state) {
            state.PauseTiming();
            auto rt = make_runtime(workers, 256);
            coio::runtime_report rep;
            {
                coio::metrics_reporter reporter{*rt};   // baseline
                reporter.probe_scheduling(std::chrono::microseconds{100});
                state.ResumeTiming();

                for (std::size_t w = 0; w < workers; ++w) {
                    rt->spawn(coio::just() | coio::let_value([rt = &*rt, per] {
                        for (long i = 0; i < per; ++i) rt->spawn(coio::just());
                        return coio::just();
                    }));
                }
                coio::this_thread::sync_wait(rt->join());

                state.PauseTiming();
                rep = reporter.tick();
            }   // reporter destroyed here: drains probes while rt is still alive
            record(state, rep, "inworker_producer", workers);
            rt.reset();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * per * static_cast<long>(workers));
    }
    BENCHMARK(metered_inworker_producer)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();
}
