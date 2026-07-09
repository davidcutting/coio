// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <coio/execution_context.h>
#include <coio/runtime.h>            // thread_launcher, default_thread_launcher, work_guard
#include <coio/detail/driver.h>      // driver / wait_driver concepts, capability tags
#include <coio/detail/frame_pool.h>
#include <coio/utils/async_scope.h>

// One-stop header for constructing runtimes: the driver-spec primitive, the heterogeneous (multi-pool,
// capability-routed) runtime, and the fluent builder — all the template machinery in one place.
//
//     auto rt = coio::runtime::builder()
//         .pool(4).driver<coio::uring_driver>(4096u)   // 4 io_uring cores
//         .pool(4).driver<coio::epoll_driver>()        // 4 epoll cores
//         .build();
//     rt.spawn_on<coio::capability::io>(work);
//
// Counts and driver args are ordinary runtime values; the executor TYPES, the capability routing table,
// and the builder's validation are all compile-time.
namespace coio {
    namespace detail {
        // A deferred, in-place driver constructor. Drivers own kernel resources and are non-movable, so
        // they must be built directly inside the executor's tuple. driver_spec holds the ctor args and
        // converts to D by returning it as a prvalue (std::make_from_tuple) — guaranteed copy elision
        // emplaces D with NO move, for any driver count. Copyable (args are values) so a worker pool can
        // re-materialize one driver per worker from a single spec. Built via coio::driver_init<D>(args...).
        template<driver D, typename... Args>
        struct driver_spec {
            using driver_type = D;
            std::tuple<Args...> args;
            constexpr operator D() && { return std::make_from_tuple<D>(std::move(args)); }
        };

        template<typename T> inline constexpr bool is_driver_spec = false;
        template<driver D, typename... A> inline constexpr bool is_driver_spec<driver_spec<D, A...>> = true;

        // consteval: the indices of the executor types (in Exs..., i.e. pool order) that provide Cap.
        // Returns {indices, count}. Pure type computation — independent of the runtime worker counts/args,
        // so routing is a compile-time fact of the pool set. static_assert-friendly.
        template<typename Cap, typename... Exs>
        consteval auto eligible_pools() -> std::pair<std::array<std::size_t, sizeof...(Exs)>, std::size_t> {
            std::array<std::size_t, sizeof...(Exs)> idx{};
            std::size_t n = 0, i = 0;
            auto visit = [&](bool provides) { if (provides) idx[n++] = i; ++i; };
            (visit(Exs::template has_capability<Cap>), ...);
            return {idx, n};
        }

        // ---- capability -> driver resolution (for the builder's .capability<Caps...>()) ----

        // Does one Driver provide ALL of Caps? (a driver advertises its set via Driver::capabilities)
        template<typename Driver, typename... Caps>
        inline constexpr bool driver_provides = (Driver::capabilities::template contains<Caps> and ...);

        // consteval: index of the FIRST driver in Registry (a preference-ordered type_list) that provides
        // all Caps, or Registry::npos. Fast-path-first: put the preferred backend earliest in the registry.
        template<typename Registry, typename... Caps>
        consteval auto resolve_index() -> std::size_t {
            std::size_t found = Registry::npos;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                ((found == Registry::npos and driver_provides<typename Registry::template at<I>, Caps...>
                    ? (found = I) : found), ...);
            }(std::make_index_sequence<Registry::size>{});
            return found;
        }

        // The resolved driver type, with a clear diagnostic when nothing on this build provides the caps.
        template<typename Registry, typename... Caps>
        struct resolve {
            static constexpr std::size_t index = resolve_index<Registry, Caps...>();
            static_assert(index != Registry::npos,
                "no driver available on this build provides all the requested capabilities "
                "(e.g. capability::file needs io_uring; an epoll-only build can't serve it)");
            using type = typename Registry::template at<index>;
        };
        template<typename Registry, typename... Caps>
        using resolve_t = typename resolve<Registry, Caps...>::type;

        // A builder with no driver registry: .driver<>() works, .capability<>() is a compile error. The
        // registry (the build's fast-path-first driver set) is supplied by coio::runtime::builder() in
        // <coio/drivers.h>, which names the concrete backends — so it lives THERE, not in this
        // backend-clean header. No customization point, no include-order hazard.
        struct no_registry {};
    }

    // Build a driver in place from its ctor args, for the executor / runtime-pool builders. Args are
    // decayed to values so the spec is copyable and re-materializable per worker. See detail::driver_spec.
    template<detail::driver D, typename... Args>
    [[nodiscard]] constexpr auto driver_init(Args&&... args) -> detail::driver_spec<D, std::decay_t<Args>...> {
        return { std::tuple<std::decay_t<Args>...>(std::forward<Args>(args)...) };
    }

    // A live homogeneous pool of one executor type: N workers + their guards + threads + a round-robin
    // cursor. Internal to the runtime (one per pool_spec).
    template<typename Ex>
    struct executor_pool {
        std::vector<std::unique_ptr<Ex>> workers;
        std::vector<work_guard<Ex>> guards;
        std::vector<std::jthread> threads;
        std::atomic<std::size_t> cursor{0};
    };

    // What pool(count, driver_init...) produces: the DEDUCED executor type + a per-worker recipe (the
    // driver specs, copied to re-materialize one executor per worker). Not the live pool.
    template<typename Ex, typename... Specs>
    struct pool_spec {
        using executor_type = Ex;
        std::size_t count;
        std::tuple<Specs...> specs;
    };

    // Declare a pool of `count` executors, each hosting the drivers you list (via coio::driver_init). The
    // executor type falls out of the drivers; the first must be a wait-driver (checked by executor itself).
    template<typename... Specs>
        requires (sizeof...(Specs) >= 1) and (detail::is_driver_spec<Specs> and ...)
    [[nodiscard]] auto pool(std::size_t count, Specs... specs) {
        using Ex = executor<typename Specs::driver_type...>;
        return pool_spec<Ex, Specs...>{count == 0 ? 1 : count, std::tuple<Specs...>(std::move(specs)...)};
    }

    // A runtime of HETEROGENEOUS executor pools, capability-routed. Where basic_runtime<Worker> holds one
    // homogeneous vector of a single Worker type, this holds a tuple of per-type pools, so a process can
    // run e.g. 4 io_uring cores + 4 epoll cores in one runtime and route work by capability.
    template<typename... PoolSpecs>
    class heterogeneous_runtime {
        static_assert(sizeof...(PoolSpecs) >= 1, "a runtime needs at least one pool");
        using pools_tuple = std::tuple<executor_pool<typename PoolSpecs::executor_type>...>;

    public:
        // default_thread_launcher only for now; a pinning launcher (pin_to_core) needs a non-variadic-clash
        // way to pass it — deferred (a variadic ctor + trailing launcher is ambiguous).
        explicit heterogeneous_runtime(PoolSpecs... specs) {
            auto spec_tuple = std::tuple<PoolSpecs...>(std::move(specs)...);
            std::size_t global = 0; // global worker index across pools, for a future pinning launcher
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (build_pool<I>(std::get<I>(std::move(spec_tuple)), global), ...);
            }(std::index_sequence_for<PoolSpecs...>{});
        }

        heterogeneous_runtime(const heterogeneous_runtime&) = delete;
        auto operator= (const heterogeneous_runtime&) -> heterogeneous_runtime& = delete;

        ~heterogeneous_runtime() {
            // Same discipline as basic_runtime: if join() wasn't driven, cancel + drain the scope WHILE the
            // workers still run (suspended I/O needs them), then stop the threads.
            if (not joined_.load(std::memory_order_acquire)) {
                scope_.close();
                scope_.request_stop();
                this_thread::sync_wait(join());
            }
            stop();
        }

        // Spawn onto an executor that provides Cap. Compile error if none does. When exactly one pool is
        // eligible it routes there directly (constexpr index, no runtime dispatch). When several pools are
        // eligible it load-balances across them by power-of-two-choices on outstanding work — sample two
        // eligible pools, spawn into the less loaded — then round-robin the worker within.
        template<typename Cap, execution::sender Sndr>
        auto spawn_on(Sndr sndr) -> void {
            constexpr auto e = detail::eligible_pools<Cap, typename PoolSpecs::executor_type...>();
            static_assert(e.second > 0, "no executor in this runtime provides that capability");
            if constexpr (e.second == 1) {
                spawn_into<e.first[0]>(std::move(sndr));
            }
            else {
                const auto [a, b] = two_choices(e.first, e.second);
                const std::size_t la = pool_load(a), lb = pool_load(b);
                // strictly-lower load wins; on a tie pick randomly so equally-loaded pools still spread
                // (otherwise trivial work would pile onto whichever index compared first).
                const std::size_t chosen = la < lb ? a : lb < la ? b : ((next_rand() & 1u) ? a : b);
                visit_pool(chosen, [&](auto& p) { spawn_into_pool(p, std::move(sndr)); });
            }
        }

        // Explicit escape hatch: spawn onto a specific pool by index (bypasses capability routing).
        template<std::size_t PoolIndex, execution::sender Sndr>
        auto spawn_on_pool(Sndr sndr) -> void {
            static_assert(PoolIndex < sizeof...(PoolSpecs), "pool index out of range");
            spawn_into<PoolIndex>(std::move(sndr));
        }

        // Test/introspection: how many spawns capability-routing has sent to pool I (its cursor).
        template<std::size_t I>
        [[nodiscard]] auto spawns_routed_to() const noexcept -> std::size_t {
            return std::get<I>(pools_).cursor.load(std::memory_order_relaxed);
        }

        [[nodiscard]] auto join() noexcept {
            return execution::then(scope_.join(), [this]() noexcept {
                joined_.store(true, std::memory_order_release);
            });
        }

        auto stop() -> void {
            if (stopped_.exchange(true)) return;
            std::apply([](auto&... p) { (stop_pool(p), ...); }, pools_);
        }

        [[nodiscard]] auto size() const noexcept -> std::size_t {
            std::size_t total = 0;
            std::apply([&](const auto&... p) { ((total += p.workers.size()), ...); }, pools_);
            return total;
        }

    private:
        template<std::size_t PoolIndex, execution::sender Sndr>
        auto spawn_into(Sndr sndr) -> void {
            spawn_into_pool(std::get<PoolIndex>(pools_), std::move(sndr));
        }

        // Round-robin a worker within one pool and spawn onto its scheduler.
        template<typename Ex, execution::sender Sndr>
        auto spawn_into_pool(executor_pool<Ex>& p, Sndr sndr) -> void {
            const auto w = p.cursor.fetch_add(1, std::memory_order_relaxed) % p.workers.size();
            scope_.spawn_on(p.workers[w]->get_scheduler(), std::move(sndr));
        }

        // Runtime pool index -> the correctly-typed pool: fn is invoked with std::get<idx>(pools_). Exactly
        // one arm of the fold runs (the fold short-circuits on the first match), so fn is called once.
        template<typename F>
        auto visit_pool(std::size_t idx, F&& fn) -> void {
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (void)((I == idx ? (static_cast<void>(fn(std::get<I>(pools_))), true) : false) or ...);
            }(std::index_sequence_for<PoolSpecs...>{});
        }

        // Load of a pool (runtime index) = sum of its workers' outstanding-op counts. Cheap relaxed reads.
        auto pool_load(std::size_t idx) -> std::size_t {
            std::size_t load = 0;
            visit_pool(idx, [&](auto& p) {
                for (auto& w : p.workers) load += w->outstanding();
            });
            return load;
        }

        // Two DISTINCT eligible pool indices (from elig[0..count)) for power-of-two-choices.
        static auto two_choices(const auto& elig, std::size_t count) noexcept -> std::pair<std::size_t, std::size_t> {
            const std::size_t r1 = next_rand() % count;
            std::size_t r2 = next_rand() % (count - 1);
            if (r2 >= r1) ++r2;                       // shift to skip r1 -> distinct
            return {elig[r1], elig[r2]};
        }

        // Per-thread xorshift — good enough for load-balancing sampling, no <random> weight, no shared state.
        static auto next_rand() noexcept -> std::uint32_t {
            static thread_local std::uint32_t s = std::hash<std::thread::id>{}(std::this_thread::get_id()) | 1u;
            s ^= s << 13; s ^= s >> 17; s ^= s << 5;
            return s;
        }

        template<std::size_t I, typename PS>
        auto build_pool(PS spec, std::size_t& global) -> void {
            using Ex = typename PS::executor_type;
            auto& p = std::get<I>(pools_);
            p.workers.reserve(spec.count);
            for (std::size_t w = 0; w < spec.count; ++w) p.workers.push_back(build_worker<Ex>(spec.specs));
            for (auto& ex : p.workers) p.guards.emplace_back(*ex);
            for (std::size_t w = 0; w < spec.count; ++w) {
                p.threads.push_back(default_thread_launcher{}(global++, [ex = p.workers[w].get()] { run_worker(*ex); }));
            }
        }

        // One fresh copy of the driver specs per worker -> each executor emplaces its own drivers.
        template<typename Ex, typename SpecTuple>
        static auto build_worker(const SpecTuple& specs) -> std::unique_ptr<Ex> {
            return std::apply([](auto... s) { return std::make_unique<Ex>(std::move(s)...); }, specs);
        }

        template<typename Ex>
        static auto run_worker(Ex& worker) -> void {
            detail::frame_pool pool;
            detail::tl_frame_pool = &pool;
            worker.run();
            detail::tl_frame_pool = nullptr;
        }

        template<typename Ex>
        static auto stop_pool(executor_pool<Ex>& p) -> void {
            for (auto& w : p.workers) w->request_stop();
            p.guards.clear();
            for (auto& t : p.threads) if (t.joinable()) t.join();
        }

        pools_tuple pools_;
        async_scope scope_;
        std::atomic<bool> stopped_{false};
        std::atomic<bool> joined_{false};
    };

    // Build a heterogeneous runtime from a set of pool(...) declarations. The runtime is non-movable
    // (owns scope + threads); returned by guaranteed copy elision into `auto rt = make_runtime(...)`.
    template<typename... PoolSpecs>
    [[nodiscard]] auto make_runtime(PoolSpecs... specs) -> heterogeneous_runtime<PoolSpecs...> {
        return heterogeneous_runtime<PoolSpecs...>{std::move(specs)...};
    }

    namespace detail {
        // Turn an in-progress pool (count + its accumulated driver specs) into a finished pool_spec whose
        // executor type is deduced from the drivers.
        template<typename... Specs>
        [[nodiscard]] auto make_pool_spec(std::size_t count, std::tuple<Specs...> drivers) {
            static_assert(sizeof...(Specs) >= 1, "each pool needs at least one .driver<D>()");
            using Ex = executor<typename Specs::driver_type...>;
            return pool_spec<Ex, Specs...>{count == 0 ? 1 : count, std::move(drivers)};
        }
    }

    // Fluent builder for a heterogeneous runtime (entry point: coio::runtime::builder()):
    //     auto rt = coio::runtime::builder()
    //         .pool(2).driver<uring_driver>(1024u)     // a pool of 2 executor<uring_driver>
    //         .pool(2).driver<epoll_driver>()          // a pool of 2 executor<epoll_driver>
    //         .build();
    // Each .pool()/.driver<D>() returns a NEW builder type carrying the accumulated structure; .build()
    // materialises the runtime. Chained drivers on one pool make a multi-driver executor:
    //     .pool(2).driver<uring_driver>(1024u).driver<gpu_driver>(dev)  // executor<uring_driver, gpu_driver>
    // HasCurrent tracks (at the type level) whether a pool is open, so .driver<> before .pool() and an
    // empty .build() are compile errors. CurrentDrivers is the open pool's driver_spec tuple; DonePools is
    // the finished pool_spec tuple.
    template<bool HasCurrent, typename CurrentDrivers, typename DonePools, typename Registry = detail::no_registry>
    class runtime_builder {
        template<bool, typename, typename, typename> friend class runtime_builder;

        std::size_t current_count_ = 0;
        CurrentDrivers current_drivers_{};
        DonePools done_{};

        runtime_builder(std::size_t cc, CurrentDrivers cd, DonePools dp)
            : current_count_(cc), current_drivers_(std::move(cd)), done_(std::move(dp)) {}

    public:
        runtime_builder() requires (not HasCurrent) = default;

        // Open a new pool of `count` executors, finalising any pool already open.
        [[nodiscard]] auto pool(std::size_t count) && {
            if constexpr (HasCurrent) {
                auto done = std::tuple_cat(std::move(done_),
                    std::make_tuple(detail::make_pool_spec(current_count_, std::move(current_drivers_))));
                return runtime_builder<true, std::tuple<>, decltype(done), Registry>{count, {}, std::move(done)};
            }
            else {
                return runtime_builder<true, std::tuple<>, DonePools, Registry>{count, {}, std::move(done_)};
            }
        }

        // Add a driver EXPLICITLY (built in place from its ctor args) to the open pool. Use this to pin a
        // specific backend / pass ctor args / build a bespoke topology.
        template<detail::driver D, typename... Args>
        [[nodiscard]] auto driver(Args&&... args) && {
            static_assert(HasCurrent, "call .pool(count) before .driver<D>()");
            auto drivers = std::tuple_cat(std::move(current_drivers_),
                std::make_tuple(driver_init<D>(std::forward<Args>(args)...)));
            return runtime_builder<true, decltype(drivers), DonePools, Registry>{
                current_count_, std::move(drivers), std::move(done_) };
        }

        // Add a driver by CAPABILITY instead of naming it: the fastest available driver on this build that
        // provides all of Caps is selected and default-constructed. "Just give me networking and files."
        // Coexists with .driver<>() (which pins a specific backend). Needs a driver registry — use
        // coio::runtime::builder() from <coio/drivers.h> (or a registry-parameterised builder).
        template<typename... Caps>
        [[nodiscard]] auto capability() && {
            static_assert(HasCurrent, "call .pool(count) before .capability<...>()");
            static_assert(not std::same_as<Registry, detail::no_registry>,
                "capability resolution needs a driver registry — use coio::runtime::builder() from "
                "<coio/drivers.h> (or a registry-parameterised builder), or name the driver with .driver<D>()");
            using D = detail::resolve_t<Registry, Caps...>;
            auto drivers = std::tuple_cat(std::move(current_drivers_), std::make_tuple(driver_init<D>()));
            return runtime_builder<true, decltype(drivers), DonePools, Registry>{
                current_count_, std::move(drivers), std::move(done_) };
        }

        // Materialise the runtime (launches all threads).
        [[nodiscard]] auto build() && {
            static_assert(HasCurrent, "add at least one pool: .pool(count).driver<D>(...) or .capability<...>()");
            auto all = std::tuple_cat(std::move(done_),
                std::make_tuple(detail::make_pool_spec(current_count_, std::move(current_drivers_))));
            return std::apply([](auto... ps) { return make_runtime(std::move(ps)...); }, std::move(all));
        }
    };

    // The builder ENTRY POINT (coio::runtime::builder()) lives in <coio/drivers.h>, which names the concrete
    // backends and so can bake in the build's fast-path-first registry for .capability<>(). This header
    // provides everything else (the fluent runtime_builder above works with any registry passed to it).
}
