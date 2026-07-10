#pragma once
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <kioto/base/config.h>

// A reference run-loop metrics sink for basic_executor<Metrics, Drivers...>. Default is no_metrics
// (execution_context.h) — zero cost/size. Plug counting_metrics in to record activity; a monitor reads
// snapshot().
namespace kioto {
    // A snapshot of one executor's run-loop counters. Rates come from diffing two snapshots over an interval.
    struct executor_stats {
        std::size_t turns;                     // run_once passes
        std::size_t ops_run;                   // ready ops executed
        std::size_t parks;                     // times the worker blocked in poll_wait
        std::size_t wakes;                     // wake syscalls issued to this worker (the "wakeup storm" signal)
        std::size_t submits_local;             // owner fast-path submits (no atomics/syscall)
        std::size_t submits_remote;            // cross-thread posts (inject stack)
        std::chrono::nanoseconds parked_time;  // total time blocked in poll_wait -> 1 - parked/wall = utilization
    };

    // Sum two snapshots field-wise. Aggregating a runtime's per-worker snapshots gives whole-runtime
    // totals (parked_time accumulates as total idle across workers). Used by runtime::aggregate_metrics().
    [[nodiscard]] constexpr auto operator+ (const executor_stats& a, const executor_stats& b) noexcept -> executor_stats {
        return {
            a.turns + b.turns, a.ops_run + b.ops_run, a.parks + b.parks, a.wakes + b.wakes,
            a.submits_local + b.submits_local, a.submits_remote + b.submits_remote,
            a.parked_time + b.parked_time,
        };
    }

    // A metrics policy that can be snapshotted for runtime-level aggregation. counting_metrics qualifies;
    // no_metrics does not (it records nothing) — the runtimes gate collect_metrics/aggregate_metrics on this.
    template<typename M>
    concept snapshotting_metrics = requires(const M& m) {
        { m.snapshot() } -> std::convertible_to<executor_stats>;
    };

    // Percentiles of a latency stream. Values are POWER-OF-TWO bucket lower edges: p99 == 8us means the true
    // p99 is in [8us, 16us) — ~2x resolution, the log-histogram trade for zero-alloc O(1) record.
    struct latency_stats {
        std::uint64_t count;
        std::chrono::nanoseconds p50, p90, p99, max;
    };

    // Fixed alloc-free log2 histogram: bucket i counts samples in [2^i, 2^(i+1)) ns. record() is a single
    // relaxed fetch_add (off the hot path — sampled probes, not every op), safe for concurrent probe
    // completions; snapshot() reads relaxed (torn reads shift a percentile by at most a bucket). CUMULATIVE.
    class latency_histogram {
    public:
        static constexpr int bucket_count = 48;   // up to 2^48 ns ~ 3.2 days; scheduling latency never nears it

        auto record(std::chrono::nanoseconds d) noexcept -> void {
            const auto ns = d.count() <= 0 ? std::uint64_t{0} : static_cast<std::uint64_t>(d.count());
            int b = ns == 0 ? 0 : std::bit_width(ns) - 1;   // floor(log2)
            if (b >= bucket_count) b = bucket_count - 1;
            buckets_[static_cast<std::size_t>(b)].fetch_add(1, std::memory_order_relaxed);
        }

        [[nodiscard]] auto snapshot() const noexcept -> latency_stats {
            std::array<std::uint64_t, bucket_count> b{};
            std::uint64_t total = 0;
            for (int i = 0; i < bucket_count; ++i) { b[static_cast<std::size_t>(i)] = buckets_[static_cast<std::size_t>(i)].load(std::memory_order_relaxed); total += b[static_cast<std::size_t>(i)]; }
            const auto pct = [&](std::uint64_t permille) noexcept -> std::chrono::nanoseconds {
                if (total == 0) return std::chrono::nanoseconds{0};
                const std::uint64_t rank = (total * permille + 999) / 1000;   // ceil, 1-based
                std::uint64_t cum = 0;
                for (int i = 0; i < bucket_count; ++i) {
                    cum += b[static_cast<std::size_t>(i)];
                    if (cum >= rank) return std::chrono::nanoseconds{std::uint64_t{1} << i};
                }
                return std::chrono::nanoseconds{std::uint64_t{1} << (bucket_count - 1)};
            };
            std::chrono::nanoseconds mx{0};
            for (int i = bucket_count - 1; i >= 0; --i) if (b[static_cast<std::size_t>(i)] > 0) { mx = std::chrono::nanoseconds{std::uint64_t{1} << i}; break; }
            return {total, pct(500), pct(900), pct(990), mx};
        }

    private:
        std::array<std::atomic<std::uint64_t>, bucket_count> buckets_{};
    };

    // Owner-thread hooks use single-writer relaxed load+store (a mov, not a locked RMW) -> ~free on the hot
    // path; the cross-thread hooks (woke/submitted_remote) use fetch_add but fire only on slow paths.
    class counting_metrics {
    public:
        // ---- owner-thread hooks (single writer) ----
        KIOTO_ALWAYS_INLINE void turn() noexcept { bump(turns_); }
        KIOTO_ALWAYS_INLINE void ran(std::size_t n) noexcept { add(ops_run_, n); }
        KIOTO_ALWAYS_INLINE void parked() noexcept { bump(parks_); park_start_ = clock::now(); }
        KIOTO_ALWAYS_INLINE void unparked() noexcept {
            const auto d = static_cast<std::int64_t>((clock::now() - park_start_).count());
            parked_ns_.store(parked_ns_.load(rlx) + d, rlx);
        }
        KIOTO_ALWAYS_INLINE void submitted_local() noexcept { bump(submits_local_); }

        // ---- any-thread hooks (multi writer) ----
        KIOTO_ALWAYS_INLINE void woke() noexcept { wakes_.fetch_add(1, rlx); }
        KIOTO_ALWAYS_INLINE void submitted_remote() noexcept { submits_remote_.fetch_add(1, rlx); }

        [[nodiscard]] auto snapshot() const noexcept -> executor_stats {
            return {
                turns_.load(rlx), ops_run_.load(rlx), parks_.load(rlx), wakes_.load(rlx),
                submits_local_.load(rlx), submits_remote_.load(rlx),
                std::chrono::nanoseconds{parked_ns_.load(rlx)},
            };
        }

    private:
        using clock = std::chrono::steady_clock;
        static constexpr auto rlx = std::memory_order_relaxed;
        KIOTO_ALWAYS_INLINE static void bump(std::atomic<std::size_t>& c) noexcept { c.store(c.load(rlx) + 1, rlx); }
        KIOTO_ALWAYS_INLINE static void add(std::atomic<std::size_t>& c, std::size_t n) noexcept { c.store(c.load(rlx) + n, rlx); }

        std::atomic<std::size_t> turns_{0}, ops_run_{0}, parks_{0}, submits_local_{0};  // owner-only
        std::atomic<std::size_t> wakes_{0}, submits_remote_{0};                          // multi-writer
        std::atomic<std::int64_t> parked_ns_{0};                                         // owner-only
        clock::time_point park_start_{};                                                 // owner-only, unshared
    };
}
