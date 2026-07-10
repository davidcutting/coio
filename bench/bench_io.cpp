// Google Benchmark microbenchmarks for kioto's reactor, using only the public, pre-existing API so
// they build and run against upstream unchanged. They establish a baseline that later branches
// (single-issuer/thread-per-core runtime, perf tweaks) can be measured against with the SAME harness.
//
//   BM_sticky_read      -- I/O throughput: `concurrency` coroutines on one context each read /dev/zero
//                          in a loop, driven by a single run() thread. With many in flight, one wait
//                          reaps a batch, so the per-op cost is the do_one turn rather than the syscall.
//   BM_multi_owner_spawn -- kioto's multi-owner model: N threads all run() ONE shared context while a
//                          producer spawns trivial tasks; they contend on the context's shared queue.
//                          The natural baseline to compare a thread-per-core runtime against.
//
// Run: bench_io [--benchmark_filter=...]
#include <cstddef>
#include <optional>
#include <vector>
#include <thread>
#include <benchmark/benchmark.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/io/file.h>
#include <kioto/io/driver/uring_context.h>
#include <kioto/runtime/uring_runtime.h>

namespace {
    using io_context = kioto::uring_context;
    using stream_file = kioto::stream_file<io_context::scheduler>;

    auto reader(io_context::scheduler sched, long iters) -> io_context::task<> {
        stream_file file{sched, "/dev/zero", stream_file::read_only};
        char buf[64];
        for (long i = 0; i < iters; ++i) {
            co_await file.async_read_some(kioto::as_writable_bytes(buf));
        }
    }

    void thread_affine_run(benchmark::State& state) {
        const long concurrency = state.range(0);
        constexpr long iters = 20'000; // reads per coroutine per benchmark iteration
        for (auto _ : state) {
            state.PauseTiming();
            std::optional<io_context> ctx{std::in_place};
            kioto::async_scope scope;
            for (long c = 0; c < concurrency; ++c) {
                scope.spawn_on(ctx->get_scheduler(), reader(ctx->get_scheduler(), iters));
            }
            state.ResumeTiming();

            ctx->run(); // timed region: drive every read to completion

            state.PauseTiming();
            kioto::this_thread::sync_wait(scope.join());
            ctx.reset();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * concurrency * iters);
    }
    BENCHMARK(thread_affine_run)->Arg(1)->Arg(8)->Arg(64)->Arg(256)->UseRealTime();

    void multi_threaded_run(benchmark::State& state) {
        const std::size_t workers = static_cast<std::size_t>(state.range(0));
        constexpr long tasks = 200'000;
        constexpr std::size_t entries = 256;
        for (auto _ : state) {
            state.PauseTiming();
            std::optional<kioto::uring_runtime> rt{std::in_place, workers, entries};
            state.ResumeTiming();

            for (long i = 0; i < tasks; ++i) rt->spawn(kioto::just());
            kioto::this_thread::sync_wait(rt->join());

            state.PauseTiming();
            rt.reset();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * tasks);
    }
    BENCHMARK(multi_threaded_run)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();

    // Same total work as multi_threaded_run, but production is DISTRIBUTED: one producer coroutine is
    // seeded per worker (round-robin, so one lands on each), and each spawns its 1/N share via the
    // runtime's round-robin spawn. So there are N parallel in-worker producers cross-posting to the
    // pool, instead of one external thread feeding N idle workers. This is the realistic fan-out shape
    // (work begets work on-worker). If THIS scales where multi_threaded_run cliffs, the cliff is the
    // single-external-producer wakeup storm, not the cross-worker submit path itself.
    void multi_threaded_inworker_run(benchmark::State& state) {
        const std::size_t workers = static_cast<std::size_t>(state.range(0));
        constexpr long tasks = 200'000;
        constexpr std::size_t entries = 256;
        const long per = tasks / static_cast<long>(workers);
        for (auto _ : state) {
            state.PauseTiming();
            std::optional<kioto::uring_runtime> rt{std::in_place, workers, entries};
            state.ResumeTiming();

            for (std::size_t w = 0; w < workers; ++w) {
                rt->spawn(kioto::just() | kioto::let_value([rt = &*rt, per] {
                    for (long i = 0; i < per; ++i) rt->spawn(kioto::just());
                    return kioto::just();
                }));
            }
            kioto::this_thread::sync_wait(rt->join());

            state.PauseTiming();
            rt.reset();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * per * static_cast<long>(workers));
    }
    BENCHMARK(multi_threaded_inworker_run)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();

    // Frame-pool churn: a bounded pipeline of pinned coroutines. Each task does a little work then
    // spawns its replacement ON its worker, keeping ~k in flight, so completed frames recycle through
    // the worker's frame pool instead of being malloc'd fresh. buf[] makes the frame non-trivial.
    auto churn_task(kioto::uring_runtime* rt, io_context::scheduler sched, std::atomic<long>* remaining)
        -> io_context::task<> {
        char buf[256];
        benchmark::DoNotOptimize(buf);
        if (remaining->fetch_sub(1, std::memory_order_relaxed) > 0) {
            rt->spawn_on(sched, churn_task(rt, sched, remaining));
        }
        co_return;
    }

    void coroutine_churn(benchmark::State& state) {
        constexpr long total = 200'000;
        constexpr long depth = 64; // in-flight pipeline depth
        for (auto _ : state) {
            state.PauseTiming();
            std::optional<kioto::uring_runtime> rt{std::in_place, std::size_t{1}, std::size_t{256}};
            std::atomic<long> remaining{total};
            state.ResumeTiming();

            rt->spawn(kioto::just() | kioto::let_value([&] {
                auto sched = *kioto::uring_runtime::current_scheduler();
                for (long i = 0; i < depth; ++i) rt->spawn_on(sched, churn_task(&*rt, sched, &remaining));
                return kioto::just();
            }));
            kioto::this_thread::sync_wait(rt->join());

            state.PauseTiming();
            rt.reset();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * total);
    }
    BENCHMARK(coroutine_churn)->UseRealTime();
}
