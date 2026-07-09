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
#include <coio/detail/execution.h>
#include <coio/detail/atomic_intrusive_stack.h>
#include <coio/detail/driver.h>
#include <coio/utils/stop_token.h>
#include <coio/utils/utility.h>
#include <coio/detail/suppress_push.h> // IWYU pragma: keep

namespace coio {
    template<typename T, typename Alloc, typename Sched>
    class task;

    namespace detail {
        // A run-queue node that knows its owning executor, so its op-state can submit itself onto the
        // executor's inbox. Generic over the executor type (was loop_base::node, minus the CRTP).
        // The generic operation-state wrapper: work-count tracking + stop-token plumbing around a
        // backend-specific Base (which supplies context_, rcvr_, do_start(), do_finish(bool), do_cancel()).
        // Ported verbatim from loop_base::operation_state — it is entirely backend-agnostic.
        template<typename Base>
        class operation_state : public Base {
            using stop_token_t = stop_token_of_t<execution::env_of_t<decltype(std::declval<Base*>()->rcvr_)>>;

            // A Base may opt out of cancellation entirely by declaring `static constexpr bool
            // coio_unstoppable = true` (e.g. a datagram send, which completes promptly regardless of the
            // peer). Then we skip the stop-callback install/teardown even when the awaiting task carries a
            // stoppable token — the op simply cannot be cancelled, which is correct for such ops. Absent
            // the declaration this defaults false, so every other op keeps today's behavior.
            static constexpr bool base_unstoppable = [] {
                if constexpr (requires { { Base::coio_unstoppable } -> std::convertible_to<bool>; })
                    return Base::coio_unstoppable;
                else return false;
            }();
            static constexpr bool never_stops = unstoppable_token<stop_token_t> or base_unstoppable;

        public:
            using operation_state_concept = execution::operation_state_tag;
            using Base::Base;

            auto start() & noexcept -> void {
                this->context_.work_started();
                if constexpr (not never_stops) {
                    auto stop_token = coio::get_stop_token(execution::get_env(this->rcvr_));
                    if (stop_token.stop_requested()) {
                        this->context_.work_finished();
                        execution::set_stopped(std::move(this->rcvr_));
                        return;
                    }
                    stop_cb_.emplace(std::move(stop_token), std::bind_front(&operation_state::do_cancel, this));
                }
                // Sync completion (do_start returned false, result already set): defer the finish by
                // posting ourselves to the run queue via our own executor — no driver back-channel.
                if (not this->do_start()) this->context_.submit(*this);
            }

            auto finish() -> void override {
                this->context_.work_finished();
                if constexpr (not never_stops) {
                    stop_cb_.reset();
                    this->do_finish(coio::get_stop_token(execution::get_env(this->rcvr_)).stop_requested());
                }
                else {
                    this->do_finish(false);
                }
            }

        protected:
            using callback_t = decltype(std::bind_front(&operation_state::do_cancel, std::declval<operation_state*>()));
            std::optional<stop_callback_for_t<stop_token_t, callback_t>> stop_cb_;
        };

        // A self-owned, receiver-less run-queue node: runs `fn` on the executor's owner thread, then frees
        // itself. It is the transport for "this cleanup MUST run where the resource lives" — e.g. io_uring's
        // single-issuer teardown, where only the owner may touch the ring. Allocated from the executor's
        // memory resource; deletes itself inside finish().
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

        // Fire-and-forget: run `fn` on `ctx`'s owner thread. Off-owner it rides the lock-free inject stack
        // home; on-owner it simply runs on the next turn. WARNING: if `ctx` has already left run(), the node
        // is never serviced and leaks — every caller must have a shutdown backstop (for io teardown that is
        // the driver destructor, which reaps whatever is left in the ring/reactor).
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

        // Placement: "run this continuation on the executor." The op-state just submits itself onto the
        // executor's run queue; no driver is involved. (Was loop_base::schedule_sender.)
        template<typename Executor>
        class schedule_sender {
            friend Executor;

            template<typename Rcvr>
            struct state_base : operation_base {
                state_base(Executor& ctx, Rcvr rcvr) noexcept : context_(ctx), rcvr_(std::move(rcvr)) {}

                COIO_ALWAYS_INLINE auto do_start() noexcept -> bool { return false; } // placement: post + finish on the loop
                COIO_ALWAYS_INLINE auto do_finish(bool) noexcept -> void { execution::set_value(std::move(rcvr_)); }
                COIO_ALWAYS_INLINE static auto do_cancel(state_base*) noexcept -> void {}

                Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members)
                Rcvr rcvr_;
            };
            template<typename Rcvr>
            using state = operation_state<state_base<Rcvr>>;

        public:
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<execution::set_value_t()>;

            explicit schedule_sender(Executor& ctx) noexcept : ctx_(&ctx) {}

            COIO_ALWAYS_INLINE auto get_env() const noexcept -> exec_env<Executor> { return {*ctx_}; }

            template<similar_to<schedule_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }

            template<execution::receiver Rcvr>
            COIO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept {
                COIO_ASSERT(ctx_ != nullptr);
                return state<Rcvr>{*std::exchange(ctx_, {}), std::move(rcvr)};
            }

        private:
            Executor* ctx_;
        };

        // The scheduler handle. Placement (schedule) is always available; capability senders
        // (schedule_after, io) are added in coio/scheduler.h, gated on the executor's drivers.
        template<typename Executor>
        class executor_scheduler {
            friend Executor;

        public:
            using scheduler_concept = execution::scheduler_tag;
            using executor_type = Executor;

            explicit executor_scheduler(Executor& ctx) noexcept : ctx_(&ctx) {}

            [[nodiscard]] COIO_ALWAYS_INLINE auto schedule() const noexcept -> schedule_sender<Executor> {
                return schedule_sender<Executor>{*ctx_};
            }

            [[nodiscard]] COIO_ALWAYS_INLINE static constexpr auto query(execution::get_forward_progress_guarantee_t) noexcept {
                return execution::forward_progress_guarantee::parallel;
            }
            [[nodiscard]] COIO_ALWAYS_INLINE auto query(get_allocator_t) const noexcept {
                COIO_ASSERT(ctx_ != nullptr);
                return ctx_->get_allocator();
            }
            [[nodiscard]] COIO_ALWAYS_INLINE auto context() const noexcept -> Executor& {
                COIO_ASSERT(ctx_ != nullptr);
                return *ctx_;
            }

            friend auto operator== (const executor_scheduler&, const executor_scheduler&) noexcept -> bool = default;

        protected:
            Executor* ctx_;
        };

        // Scheduler composition. A driver may contribute a `scheduler_mixin<Executor, Base>`: a class
        // template deriving from Base that adds its capability's senders (schedule_after, schedule_io, ...)
        // on top of whatever surface Base already has. The executor's scheduler is the chain of all
        // drivers' mixins over executor_scheduler, FIRST driver outermost (most derived) — so when two
        // drivers provide the same capability, ordinary name hiding makes the first one win, matching
        // get_driver's first-match rule.
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

    // ============================================================================================
    // executor<WaitDrv, Rest...> — one thread's run loop + inbox + park state, over a driver set. The
    // FIRST driver is the wait-owner (blocks/wakes the thread). Single-owner: exactly one thread ever
    // calls run()/run_once(); cross-thread work arrives via the lock-free inject stack.
    // ============================================================================================
    template<detail::wait_driver WaitDrv, detail::driver... Rest>
    class executor {
    public:
        // The scheduler is composed from the drivers' scheduler_mixins (see compose_scheduler); this is
        // how backend io stays localized to the backend header while execution_context.h stays
        // liburing/epoll-free.
        using scheduler = typename detail::compose_scheduler<executor, WaitDrv, Rest...>::type;
        template<typename T = void, typename Alloc = void>
        using task = coio::task<T, Alloc, scheduler>;
        using wait_driver_type = WaitDrv;

        executor() = default;
        explicit executor(std::pmr::memory_resource& mr) noexcept : allocator_(&mr) {}

        // Construct the (sole) wait-driver in-place from Args. Drivers own kernel resources and are
        // non-movable, so they're built in the tuple, not passed by value. tuple's element-wise ctor
        // initialises the element in place (no move). This is how uring_runtime forwards `entries`:
        // executor(std::in_place, entries) -> uring_driver{entries}. (Single-driver executors only; a
        // multi-driver executor with per-driver ctor args would need a richer builder.)
        template<typename... Args>
            requires (sizeof...(Rest) == 0) and std::constructible_from<WaitDrv, Args&&...>
        explicit executor(std::in_place_t, Args&&... args)
            : drivers_(std::forward<Args>(args)...) {}

        // Build EACH driver in place from its coio::driver_init spec — the multi-driver construction path.
        // One spec per driver, in declaration order (wait-driver first). Non-movable drivers are emplaced
        // via driver_spec's prvalue conversion (guaranteed elision), so this works for any driver count.
        // Constrained on tuple-constructibility rather than naming driver_spec, so this header stays free
        // of the builder machinery (which lives in init.h and includes THIS header) — no include cycle.
        // Only a driver_spec converts to a non-movable driver, so this accepts exactly the intended args.
        template<typename... Specs>
            requires (sizeof...(Specs) == 1 + sizeof...(Rest))
                and std::constructible_from<std::tuple<WaitDrv, Rest...>, Specs...>
        explicit executor(Specs... specs) : drivers_(std::move(specs)...) {}

        executor(const executor&) = delete;
        auto operator= (const executor&) -> executor& = delete;

        template<typename Cap>
        static constexpr bool has_capability =
            WaitDrv::capabilities::template contains<Cap> or (Rest::capabilities::template contains<Cap> or ...);

        template<typename Cap>
        [[nodiscard]] COIO_ALWAYS_INLINE auto get_driver() noexcept -> auto& {
            static_assert(has_capability<Cap>, "this executor owns no driver for that capability");
            return driver_for<Cap>(std::make_index_sequence<1 + sizeof...(Rest)>{});
        }

        [[nodiscard]] COIO_ALWAYS_INLINE auto get_scheduler() noexcept -> scheduler { return scheduler{*this}; }
        [[nodiscard]] COIO_ALWAYS_INLINE auto get_allocator() const noexcept -> std::pmr::polymorphic_allocator<> { return allocator_; }

        // Submit a ready operation. Owner -> straight onto the run queue; any other thread -> lock-free
        // inject stack, waking us iff we made it non-empty (and we are parked).
        COIO_ALWAYS_INLINE auto submit(detail::operation_base& op) -> void {
            if (owner_.load(std::memory_order_relaxed) == std::this_thread::get_id()) {
                ready_.push_back(op);
                return;
            }
            if (inject_stack_.push(op) != detail::stack_status::not_empty) wake_up();
        }

        COIO_ALWAYS_INLINE auto wake_up() noexcept -> void {
            std::atomic_thread_fence(std::memory_order_seq_cst);
            if (parked_.load(std::memory_order_relaxed)) wait_driver().wake_up();
        }

        // Relaxed RMW: the counter is only a "should I consider exiting" gate, re-validated under the park
        // protocol. It carries no synchronization itself — the actual work delivery + wakeup rides
        // submit()/inject_stack + wake_up(), which already has a seq_cst fence. fetch_sub returns the prior
        // value, so ==1 means it just hit 0.
        COIO_ALWAYS_INLINE auto work_started() noexcept -> void { work_count_.fetch_add(1, std::memory_order_relaxed); }
        COIO_ALWAYS_INLINE auto work_finished() noexcept -> void {
            if (work_count_.fetch_sub(1, std::memory_order_relaxed) == 1) wake_up();
        }

        COIO_ALWAYS_INLINE auto request_stop() -> void {
            if (stop_source_.request_stop()) wake_up();
        }

        [[nodiscard]] auto get_stop_token() const noexcept { return stop_source_.get_token(); }

        // A cheap load signal for placement policies (e.g. the runtime's power-of-two-choices): the count
        // of outstanding operations (work_started but not yet work_finished). Read cross-thread relaxed.
        [[nodiscard]] COIO_ALWAYS_INLINE auto outstanding() const noexcept -> std::size_t {
            return work_count_.load(std::memory_order_relaxed);
        }

        // One non-blocking pass: claim ownership, drain posts, poll every driver, run up to `batch`
        // ready continuations. Returns whether it did any work. Never blocks.
        auto run_once() -> bool {
            owner_.store(std::this_thread::get_id(), std::memory_order_relaxed);
            drain_inbox();
            std::apply([&](auto&... d) { (d.poll(ready_, batch_), ...); }, drivers_);
            bool did_work = false;
            for (std::size_t i = 0; i < batch_; ++i) {
                auto* op = ready_.pop_front();
                if (op == nullptr) break;
                op->finish();
                did_work = true;
            }
            return did_work;
        }

        auto run() -> std::size_t {
            owner_.store(std::this_thread::get_id(), std::memory_order_relaxed);
            std::size_t turns = 0;
            for (;;) {
                if (run_once()) { ++turns; continue; }  // made progress (drained inbox / ran ready ops)

                // Idle. Exit only when there is also no outstanding async work; otherwise block. (Note we
                // drain+run BEFORE this check, so ops submitted without a work-count — e.g. bare
                // operation_base posts — still run, matching the old loop_base do_one ordering.)
                if (work_count_.load(std::memory_order_relaxed) == 0) break;

                // Arm elision, publish, re-check the inbox (Dekker), else block in the wait-owner. The
                // re-check inspects ONLY the inbox — NOT a full run_once(). A run_once() here would poll the
                // drivers, whose poll() resets the wait-driver's wake signal (epoll's interrupter eventfd /
                // uring's msg_ring CQE). A cross-thread submit() that lands after we set parked_ pushes its
                // op AND fires that wake; if the re-check's poll() consumed the wake without draining the
                // inbox, we'd block in poll_wait() forever with the op stranded (the lost-wakeup that hung
                // off-owner teardown). The inbox is the only thing a submit() can add after we park; driver
                // completions are caught by poll_wait() itself, so an inbox-only load is the correct guard.
                parked_.store(true, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_seq_cst);
                if (not inject_stack_.empty()) { parked_.store(false, std::memory_order_relaxed); continue; }
                wait_driver().poll_wait();
                parked_.store(false, std::memory_order_relaxed);
            }
            return turns;
        }

        [[nodiscard]] COIO_ALWAYS_INLINE auto is_owner() const noexcept -> bool {
            return owner_.load(std::memory_order_relaxed) == std::this_thread::get_id();
        }

    private:
        [[nodiscard]] COIO_ALWAYS_INLINE auto wait_driver() noexcept -> WaitDrv& { return std::get<0>(drivers_); }

        template<typename Cap, std::size_t... I>
        [[nodiscard]] COIO_ALWAYS_INLINE auto driver_for(std::index_sequence<I...>) noexcept -> auto& {
            // Return the FIRST driver (declaration order) providing Cap. Several may provide it —
            // has_capability only guarantees at least one; earlier drivers win capability disputes.
            return pick<Cap, I...>();
        }
        template<typename Cap, std::size_t I, std::size_t... Rest2>
        [[nodiscard]] COIO_ALWAYS_INLINE auto pick() noexcept -> auto& {
            using D = std::tuple_element_t<I, std::tuple<WaitDrv, Rest...>>;
            if constexpr (D::capabilities::template contains<Cap>) return std::get<I>(drivers_);
            else return pick<Cap, Rest2...>();
        }

        COIO_ALWAYS_INLINE auto drain_inbox() noexcept -> void {
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
    };

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

#include <coio/detail/suppress_pop.h> // IWYU pragma: keep
