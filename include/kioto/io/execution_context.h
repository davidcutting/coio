// ReSharper disable CppPolymorphicClassWithNonVirtualPublicDestructor
// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <atomic>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory_resource>
#include <optional>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <kioto/exec/execution.h>
#include <kioto/base/atomic_intrusive_stack.h>
#include <kioto/io/driver/driver.h>
#include <kioto/exec/stop_token.h>
#include <kioto/base/utility.h>
#include <kioto/base/suppress_push.h> // IWYU pragma: keep

namespace kioto {
    template<typename T, typename Alloc, typename Sched>
    class task;

    namespace detail {
        // Generic (driver-agnostic) op-state wrapper: work-count tracking + stop-token plumbing around a
        // driver-specific Base supplying context_, rcvr_, do_start(), do_finish(bool), do_cancel().
        template<typename Base>
        class operation_state : public Base {
            using stop_token_t = stop_token_of_t<execution::env_of_t<decltype(std::declval<Base*>()->rcvr_)>>;

            // A Base declaring `static constexpr bool kioto_unstoppable = true` (e.g. datagram send) skips
            // the stop-callback install/teardown even under a stoppable token — the op can't be cancelled.
            static constexpr bool base_unstoppable = [] {
                if constexpr (requires { { Base::kioto_unstoppable } -> std::convertible_to<bool>; })
                    return Base::kioto_unstoppable;
                else return false;
            }();
            static constexpr bool never_stops = unstoppable_token<stop_token_t> or base_unstoppable;

        public:
            using operation_state_concept = execution::operation_state_tag;
            using Base::Base;

            auto start() & noexcept -> void {
                this->context_.work_started();
                if constexpr (not never_stops) {
                    auto stop_token = kioto::get_stop_token(execution::get_env(this->rcvr_));
                    if (stop_token.stop_requested()) {
                        this->context_.work_finished();
                        execution::set_stopped(std::move(this->rcvr_));
                        return;
                    }
                    stop_cb_.emplace(std::move(stop_token), std::bind_front(&operation_state::do_cancel, this));
                }
                // Sync completion (do_start returned false): defer finish by posting ourselves to the queue.
                if (not this->do_start()) this->context_.submit(*this);
            }

            auto finish() -> void override {
                this->context_.work_finished();
                if constexpr (not never_stops) {
                    stop_cb_.reset();
                    this->do_finish(kioto::get_stop_token(execution::get_env(this->rcvr_)).stop_requested());
                }
                else {
                    this->do_finish(false);
                }
            }

        protected:
            using callback_t = decltype(std::bind_front(&operation_state::do_cancel, std::declval<operation_state*>()));
            std::optional<stop_callback_for_t<stop_token_t, callback_t>> stop_cb_;
        };

        // Self-owned, receiver-less run-queue node: runs `fn` on the owner thread then frees itself. The
        // transport for "cleanup MUST run where the resource lives" (e.g. io_uring single-issuer teardown).
        template<typename F>
        struct deferred_action : operation_base {
            deferred_action(std::pmr::polymorphic_allocator<> alloc, F fn) noexcept
                : alloc_(alloc), fn_(std::move(fn)) {}
            auto finish() -> void override {
                fn_();
                auto alloc = alloc_;      // copy: delete_object destroys *this (and alloc_/fn_)
                alloc.delete_object(this);
            }
            std::pmr::polymorphic_allocator<> alloc_;
            F fn_;
        };

        // Fire-and-forget `fn` on `ctx`'s owner thread (off-owner rides the inject stack home). WARNING: if
        // `ctx` has already left run(), the node is never serviced and LEAKS — every caller needs a shutdown
        // backstop (for io teardown, the driver destructor, which reaps whatever's left in the ring/reactor).
        template<typename Ctx, typename F>
        auto defer_to_owner(Ctx& ctx, F fn) -> void {
            std::pmr::polymorphic_allocator<> alloc = ctx.get_allocator();
            auto* node = alloc.new_object<deferred_action<F>>(alloc, std::move(fn));
            ctx.submit(*node);
        }

        template<typename Executor>
        struct exec_env {
            auto query(execution::get_completion_scheduler_t<execution::set_value_t>) const noexcept {
                return ctx_.get_scheduler();
            }
            auto query(get_allocator_t) const noexcept -> std::pmr::polymorphic_allocator<> {
                return ctx_.get_allocator();
            }
            Executor& ctx_; // NOLINT(*-avoid-const-or-ref-data-members)
        };

        // Placement: run this continuation on the executor. The op-state just submits itself onto the run
        // queue; no driver involved.
        template<typename Executor>
        class schedule_sender {
            friend Executor;

            template<typename Rcvr>
            struct state_base : operation_base {
                state_base(Executor& ctx, Rcvr rcvr) noexcept : context_(ctx), rcvr_(std::move(rcvr)) {}

                KIOTO_ALWAYS_INLINE auto do_start() noexcept -> bool { return false; } // placement: post + finish on the loop
                KIOTO_ALWAYS_INLINE auto do_finish(bool) noexcept -> void { execution::set_value(std::move(rcvr_)); }
                KIOTO_ALWAYS_INLINE static auto do_cancel(state_base*) noexcept -> void {}

                Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members)
                Rcvr rcvr_;
            };
            template<typename Rcvr>
            using state = operation_state<state_base<Rcvr>>;

        public:
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<execution::set_value_t()>;

            explicit schedule_sender(Executor& ctx) noexcept : ctx_(&ctx) {}

            KIOTO_ALWAYS_INLINE auto get_env() const noexcept -> exec_env<Executor> { return {*ctx_}; }

            template<similar_to<schedule_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept {
                KIOTO_ASSERT(ctx_ != nullptr);
                return state<Rcvr>{*std::exchange(ctx_, {}), std::move(rcvr)};
            }

        private:
            Executor* ctx_;
        };

        // The scheduler handle. Placement (schedule) is always available; capability senders
        // (schedule_after, io) are added in kioto/scheduler.h, gated on the executor's drivers.
        template<typename Executor>
        class executor_scheduler {
            friend Executor;

        public:
            using scheduler_concept = execution::scheduler_tag;
            using executor_type = Executor;

            explicit executor_scheduler(Executor& ctx) noexcept : ctx_(&ctx) {}

            [[nodiscard]] KIOTO_ALWAYS_INLINE auto schedule() const noexcept -> schedule_sender<Executor> {
                return schedule_sender<Executor>{*ctx_};
            }

            [[nodiscard]] KIOTO_ALWAYS_INLINE static constexpr auto query(execution::get_forward_progress_guarantee_t) noexcept {
                return execution::forward_progress_guarantee::parallel;
            }
            [[nodiscard]] KIOTO_ALWAYS_INLINE auto query(get_allocator_t) const noexcept {
                KIOTO_ASSERT(ctx_ != nullptr);
                return ctx_->get_allocator();
            }
            [[nodiscard]] KIOTO_ALWAYS_INLINE auto context() const noexcept -> Executor& {
                KIOTO_ASSERT(ctx_ != nullptr);
                return *ctx_;
            }

            friend auto operator== (const executor_scheduler&, const executor_scheduler&) noexcept -> bool = default;

        protected:
            Executor* ctx_;
        };

        // Scheduler composition: a driver may contribute `scheduler_mixin<Executor, Base>` deriving from
        // Base to add its capability's senders. The scheduler is the chain of all mixins over
        // executor_scheduler, FIRST driver outermost — so name hiding makes it win ties, matching get_driver.
        template<typename Ex, typename D>
        concept has_scheduler_mixin = requires { typename D::template scheduler_mixin<Ex, executor_scheduler<Ex>>; };

        template<typename Ex, typename... Ds>
        struct compose_scheduler {
            using type = executor_scheduler<Ex>;
        };

        template<typename Ex, typename D, typename... Rest>
        struct compose_scheduler<Ex, D, Rest...> {
            using inner = typename compose_scheduler<Ex, Rest...>::type;
            static auto pick() {
                if constexpr (has_scheduler_mixin<Ex, D>)
                    return std::type_identity<typename D::template scheduler_mixin<Ex, inner>>{};
                else
                    return std::type_identity<inner>{};
            }
            using type = typename decltype(pick())::type;
        };
    }

    // executor<WaitDrv, Rest...> — one thread's run loop + inbox + park state over a driver set. The FIRST
    // driver is the wait-owner (blocks/wakes the thread). Single-owner: exactly one thread calls run();
    // cross-thread work arrives via the lock-free inject stack.
    // The Metrics policy observes the run loop. no_metrics (the default `executor` alias) is empty inline
    // hooks + [[no_unique_address]] -> zero cost/size; a real sink plugs in as basic_executor<M, Drivers...>.
    // Owner-thread hooks are single-threaded; woke()/submitted_remote() may be cross-thread (need atomic RMW).
    template<typename Metrics, detail::wait_driver WaitDrv, detail::driver... Rest>
    class basic_executor {
    public:
        // Composed from the drivers' scheduler_mixins (see compose_scheduler), which keeps driver io in the
        // driver header and this file liburing/epoll-free.
        using scheduler = typename detail::compose_scheduler<basic_executor, WaitDrv, Rest...>::type;
        template<typename T = void, typename Alloc = void>
        using task = kioto::task<T, Alloc, scheduler>;
        using wait_driver_type = WaitDrv;
        using metrics_type = Metrics;

        basic_executor() = default;
        explicit basic_executor(std::pmr::memory_resource& mr) noexcept : allocator_(&mr) {}

        // Construct the sole wait-driver in-place from Args (drivers own kernel resources -> non-movable,
        // built in the tuple). How uring_runtime forwards entries: executor(in_place, entries). Single-driver
        // only; multi-driver with per-driver ctor args would need a richer builder.
        template<typename... Args>
            requires (sizeof...(Rest) == 0) and std::constructible_from<WaitDrv, Args&&...>
        explicit basic_executor(std::in_place_t, Args&&... args)
            : drivers_(std::forward<Args>(args)...) {}

        // Multi-driver path: one driver_init spec per driver (declaration order), emplaced via driver_spec's
        // prvalue conversion (guaranteed elision) so non-movable drivers work. Constrained on tuple-
        // constructibility, not by naming driver_spec, so this header avoids an init.h include cycle.
        template<typename... Specs>
            requires (sizeof...(Specs) == 1 + sizeof...(Rest))
                and std::constructible_from<std::tuple<WaitDrv, Rest...>, Specs...>
        explicit basic_executor(Specs... specs) : drivers_(std::move(specs)...) {}

        basic_executor(const basic_executor&) = delete;
        auto operator= (const basic_executor&) -> basic_executor& = delete;

        template<typename Cap>
        static constexpr bool has_capability =
            WaitDrv::capabilities::template contains<Cap> or (Rest::capabilities::template contains<Cap> or ...);

        template<typename Cap>
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto get_driver() noexcept -> auto& {
            static_assert(has_capability<Cap>, "this executor owns no driver for that capability");
            return driver_for<Cap>(std::make_index_sequence<1 + sizeof...(Rest)>{});
        }

        [[nodiscard]] KIOTO_ALWAYS_INLINE auto get_scheduler() noexcept -> scheduler { return scheduler{*this}; }
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto get_allocator() const noexcept -> std::pmr::polymorphic_allocator<> { return allocator_; }

        // Submit a ready operation. Owner -> straight onto the run queue; any other thread -> lock-free
        // inject stack, waking us iff we made it non-empty (and we are parked).
        KIOTO_ALWAYS_INLINE auto submit(detail::operation_base& op) -> void {
            if (owner_.load(std::memory_order_relaxed) == std::this_thread::get_id()) {
                metrics_.submitted_local();      // owner fast-path (single-writer)
                ready_.push_back(op);
                return;
            }
            metrics_.submitted_remote();         // cross-thread post (any thread)
            if (inject_stack_.push(op) != detail::stack_status::not_empty) wake_up();
        }

        KIOTO_ALWAYS_INLINE auto wake_up() noexcept -> void {
            std::atomic_thread_fence(std::memory_order_seq_cst);
            if (parked_.load(std::memory_order_relaxed)) { wait_driver().wake_up(); metrics_.woke(); }
        }

        // Relaxed RMW: work_count is only a "should I consider exiting" gate, re-validated under the park
        // protocol; it carries no synchronization (that rides submit()+wake_up()'s seq_cst fence).
        // fetch_sub returns the prior value, so ==1 means it just hit 0.
        KIOTO_ALWAYS_INLINE auto work_started() noexcept -> void { work_count_.fetch_add(1, std::memory_order_relaxed); }
        KIOTO_ALWAYS_INLINE auto work_finished() noexcept -> void {
            if (work_count_.fetch_sub(1, std::memory_order_relaxed) == 1) wake_up();
        }

        KIOTO_ALWAYS_INLINE auto request_stop() -> void {
            if (stop_source_.request_stop()) wake_up();
        }

        [[nodiscard]] auto get_stop_token() const noexcept { return stop_source_.get_token(); }

        // A cheap load signal for placement policies (e.g. the runtime's power-of-two-choices): the count
        // of outstanding operations (work_started but not yet work_finished). Read cross-thread relaxed.
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto outstanding() const noexcept -> std::size_t {
            return work_count_.load(std::memory_order_relaxed);
        }

        // Run-loop metrics policy (read via metrics().snapshot() on sinks that provide it). Empty under no_metrics.
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto metrics() noexcept -> Metrics& { return metrics_; }
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto metrics() const noexcept -> const Metrics& { return metrics_; }

        // One non-blocking pass: claim ownership, drain posts, poll every driver, run up to `batch`
        // ready continuations. Returns whether it did any work. Never blocks.
        auto run_once() -> bool {
            owner_.store(std::this_thread::get_id(), std::memory_order_relaxed);
            drain_inbox();
            std::apply([&](auto&... d) { (d.poll(ready_, batch_), ...); }, drivers_);
            std::size_t n = 0;
            for (; n < batch_; ++n) {
                auto* op = ready_.pop_front();
                if (op == nullptr) break;
                op->finish();
            }
            metrics_.ran(n);
            metrics_.turn();
            return n != 0;
        }

        auto run() -> std::size_t {
            owner_.store(std::this_thread::get_id(), std::memory_order_relaxed);
            std::size_t turns = 0;
            for (;;) {
                if (run_once()) { ++turns; continue; }  // made progress (drained inbox / ran ready ops)

                // Idle; exit only if no outstanding work (we drain+run BEFORE this so work-count-less posts
                // still run).
                if (work_count_.load(std::memory_order_relaxed) == 0) break;

                // Park protocol (Dekker): publish parked_, fence, re-check ONLY the inbox, else block. NOT a
                // run_once() here: poll() would reset the wait-driver's wake signal (epoll eventfd / uring
                // msg_ring CQE), so a submit() racing the park could have its wake consumed without draining
                // the inbox -> block forever with the op stranded (the lost-wakeup that hung teardown). The
                // inbox is all a submit() can add after we park; driver completions are caught by poll_wait().
                parked_.store(true, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_seq_cst);
                if (not inject_stack_.empty()) { parked_.store(false, std::memory_order_relaxed); continue; }
                metrics_.parked();
                wait_driver().poll_wait();
                metrics_.unparked();
                parked_.store(false, std::memory_order_relaxed);
            }
            return turns;
        }

        [[nodiscard]] KIOTO_ALWAYS_INLINE auto is_owner() const noexcept -> bool {
            return owner_.load(std::memory_order_relaxed) == std::this_thread::get_id();
        }

    private:
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto wait_driver() noexcept -> WaitDrv& { return std::get<0>(drivers_); }

        template<typename Cap, std::size_t... I>
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto driver_for(std::index_sequence<I...>) noexcept -> auto& {
            // Return the FIRST driver (declaration order) providing Cap. Several may provide it —
            // has_capability only guarantees at least one; earlier drivers win capability disputes.
            return pick<Cap, I...>();
        }
        template<typename Cap, std::size_t I, std::size_t... Rest2>
        [[nodiscard]] KIOTO_ALWAYS_INLINE auto pick() noexcept -> auto& {
            using D = std::tuple_element_t<I, std::tuple<WaitDrv, Rest...>>;
            if constexpr (D::capabilities::template contains<Cap>) return std::get<I>(drivers_);
            else return pick<Cap, Rest2...>();
        }

        KIOTO_ALWAYS_INLINE auto drain_inbox() noexcept -> void {
            if (inject_stack_.empty()) return;
            auto* n = inject_stack_.pop_all();
            detail::operation_base* fifo = nullptr;
            while (n) { auto* next = n->next_; n->next_ = fifo; fifo = n; n = next; }
            while (fifo) { auto* next = fifo->next_; ready_.push_back(*fifo); fifo = next; }
        }

        std::tuple<WaitDrv, Rest...> drivers_;
        std::pmr::polymorphic_allocator<> allocator_;
        inplace_stop_source stop_source_;
        detail::atomic_intrusive_stack<detail::operation_base> inject_stack_{&detail::operation_base::next_};
        detail::ready_queue ready_{&detail::operation_base::next_};
        std::atomic<std::thread::id> owner_{};
        std::atomic<bool> parked_{false};
        std::atomic<std::size_t> work_count_{0};
        std::size_t batch_ = 64;
        [[no_unique_address]] Metrics metrics_{};   // no_metrics -> zero size, empty inline hooks
    };

    // Default metrics policy: every hook a no-op -> zero cost/size. Plug a real sink via
    // basic_executor<M, Drivers...> (e.g. kioto::counting_metrics).
    struct no_metrics {
        KIOTO_ALWAYS_INLINE void turn() noexcept {}
        KIOTO_ALWAYS_INLINE void ran(std::size_t) noexcept {}
        KIOTO_ALWAYS_INLINE void parked() noexcept {}
        KIOTO_ALWAYS_INLINE void unparked() noexcept {}
        KIOTO_ALWAYS_INLINE void woke() noexcept {}
        KIOTO_ALWAYS_INLINE void submitted_local() noexcept {}
        KIOTO_ALWAYS_INLINE void submitted_remote() noexcept {}
    };

    // The common spelling: an executor with metrics off. `basic_executor<M, ...>` opts a sink in.
    template<detail::wait_driver WaitDrv, detail::driver... Rest>
    using executor = basic_executor<no_metrics, WaitDrv, Rest...>;

    template<typename ExecutionContext>
    concept execution_context = requires(ExecutionContext& context) {
        { context.get_scheduler() } -> execution::scheduler;
        context.work_started();
        context.work_finished();
    };

    template<execution_context ExecutionContext>
    class work_guard {
    public:
        work_guard() = default;
        explicit work_guard(ExecutionContext& context) noexcept : context_(&context) { context.work_started(); }
        work_guard(const work_guard& other) noexcept : context_(other.context_) { if (context_) context_->work_started(); }
        work_guard(work_guard&& other) noexcept : context_(std::exchange(other.context_, {})) {}
        ~work_guard() { if (context_) context_->work_finished(); }
        auto operator= (work_guard other) noexcept -> work_guard& { std::swap(context_, other.context_); return *this; }
    private:
        ExecutionContext* context_ = nullptr;
    };
}

#include <kioto/base/suppress_pop.h> // IWYU pragma: keep
