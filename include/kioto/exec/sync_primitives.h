#pragma once
#include <atomic>
#include <bit>
#include <limits>
#include <mutex>
#include <optional>
#include <utility>
#include <kioto/base/atomic_intrusive_stack.h>
#include <kioto/exec/execution.h>
#include <kioto/base/atomutex.h>
#include <kioto/exec/stop_token.h>

namespace kioto {
    template<typename Mutex>
    concept basic_async_lockable = requires (Mutex&& mtx) {
        { mtx.lock() } -> execution::sender;
        { mtx.unlock() } -> std::same_as<void>;
        requires std::same_as<execution::value_types_of_t<decltype(mtx.lock())>, std::variant<std::tuple<>>>;
    };

    template<typename Mutex>
    concept async_lockable = basic_async_lockable<Mutex> and requires (Mutex&& mtx) {
        { mtx.try_lock() } -> boolean_testable;
    };

    template<typename AsyncMutex>
    class async_unique_lock {
        static_assert(basic_async_lockable<AsyncMutex>, "type `AsyncMutex` shall model `kioto::basic_async_lockable`");
    public:
        using mutex_type = AsyncMutex;

    public:
        async_unique_lock() = default;

        async_unique_lock(mutex_type& mtx, std::adopt_lock_t) noexcept : mtx_(&mtx), owned_(true) {}

        async_unique_lock(mutex_type& mtx, std::defer_lock_t) noexcept : mtx_(&mtx), owned_(false) {}

        async_unique_lock(mutex_type& mtx, std::try_to_lock_t) requires async_lockable<AsyncMutex> : mtx_(&mtx), owned_(mtx.try_lock()) {}

        async_unique_lock(const async_unique_lock&) = delete;

        async_unique_lock(async_unique_lock&& other) noexcept : mtx_(std::exchange(other.mtx_, {})), owned_(std::exchange(other.owned_, {})) {};

        ~async_unique_lock() {
            if (owned_) [[likely]] {
                KIOTO_ASSERT(mtx_ != nullptr);
                mtx_->unlock();
            }
        }

        auto operator= (async_unique_lock other) noexcept -> async_unique_lock& {
            this->swap(other);
            return *this;
        }

        auto swap(async_unique_lock& other) noexcept -> void {
            std::swap(mtx_, other.mtx_);
            std::swap(owned_, other.owned_);
        }

        friend auto swap(async_unique_lock& lhs, async_unique_lock& rhs) noexcept -> void {
            lhs.swap(rhs);
        }

        auto lock() {
            validate_();
            return then(mtx_->lock(), [this]() noexcept {
                owned_ = true;
            });
        }

        [[nodiscard]]
        auto try_lock() -> bool requires async_lockable<AsyncMutex> {
            validate_();
            return owned_ = bool(mtx_->try_lock());
        }

        auto unlock() -> void {
            KIOTO_ASSERT(mtx_ != nullptr);
            KIOTO_ASSERT(owned_);
            mtx_->unlock();
            owned_ = false;
        }

        [[nodiscard]]
        auto mutex() noexcept -> mutex_type* {
            return mtx_;
        }

        [[nodiscard]]
        auto owns_lock() const noexcept -> bool {
            return owned_;
        }

        explicit operator bool() const noexcept {
            return owns_lock();
        }

        [[nodiscard]]
        auto release() noexcept -> mutex_type* {
            owned_ = false;
            return std::exchange(mtx_, nullptr);
        }

    private:
        auto validate_() const noexcept {
            KIOTO_ASSERT(mtx_ != nullptr);
            KIOTO_ASSERT(not owned_);
        }

    private:
        mutex_type* mtx_ = nullptr;
        bool owned_ = false;
    };

    class async_mutex {
    public:
        class lock_sender {
            friend async_mutex;
        private:
            struct state_base {
                using operation_state_concept = execution::operation_state_tag;
                using complete_fn_t = void(*)(state_base*) noexcept;

                state_base(async_mutex& mutex, complete_fn_t complete) noexcept : mtx_(mutex), complete_(complete) {}

                state_base(const state_base&) = delete;

                auto operator= (const state_base&) -> state_base& = delete;

                async_mutex& mtx_;
                const complete_fn_t complete_;
                state_base* next_ = nullptr;
            };

            template<typename Rcvr>
            class state : public state_base {
                friend async_mutex;
            public:
                state(async_mutex& mtx, Rcvr rcvr) noexcept : state_base(mtx, &complete), rcvr_(std::move(rcvr)) {}

                state(const state&) = delete;

                auto operator= (const state&) -> state& = delete;

                KIOTO_ALWAYS_INLINE auto start() & noexcept -> void {
                    while (true) {
                        std::uintptr_t old_state = not_locked;
                        // Try to take an unlocked mutex -> locked_but_no_waiter, resume immediately.
                        if (mtx_.state_.compare_exchange_strong(old_state, locked_but_no_waiter)) {
                            execution::set_value(std::move(rcvr_));
                            return;
                        }
                        // Locked: old_state is the waiter-stack head (a lock_operation* or null). Push self.
                        next_ = std::bit_cast<state_base*>(old_state);
                        if (mtx_.state_.compare_exchange_weak(old_state, std::bit_cast<std::uintptr_t>(this))) {
                            return;
                        }
                    }
                }

            private:
                static auto complete(state_base* self) noexcept -> void {
                    auto this_ = static_cast<state*>(self);
                    execution::set_value(std::move(this_->rcvr_));
                }

            private:
                Rcvr rcvr_;
            };

        public:
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<execution::set_value_t()>;

        private:
            lock_sender(async_mutex& mtx) noexcept : mtx_(&mtx) {}

        public:
            lock_sender(const lock_sender&) = delete;

            lock_sender(lock_sender&& other) noexcept : mtx_(std::exchange(other.mtx_, {})) {}

            auto operator= (const lock_sender&) -> lock_sender& = delete;

            auto operator= (lock_sender&& other) noexcept -> lock_sender& {
                mtx_ = std::exchange(other.mtx_, {});
                return *this;
            }

            template<similar_to<lock_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures {
                return {};
            }

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept -> state<Rcvr> {
                KIOTO_ASSERT(mtx_ != nullptr);
                return {*std::exchange(mtx_, nullptr), std::move(rcvr)};
            }

        private:
            async_mutex* mtx_;
        };

    public:
        async_mutex() = default;

        async_mutex(const async_mutex&) = delete;

        auto operator= (const async_mutex&) -> async_mutex& = delete;

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto lock() noexcept {
            return append_fallback_env(
                execution::affine(lock_sender{*this}),
                execution::prop{execution::get_start_scheduler, execution::inline_scheduler{}}
            );
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto lock_guard() noexcept {
            return execution::then(lock(), [this]() noexcept {
                return async_unique_lock{*this, std::adopt_lock};
            });
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto try_lock() noexcept -> bool {
            auto expected = not_locked;
            return state_.compare_exchange_strong(expected, locked_but_no_waiter);
        }

        auto unlock() -> void {
            lock_sender::state_base* old_head = head_.load();
            if (old_head == nullptr) {
                auto old_state = locked_but_no_waiter;
                if (state_.compare_exchange_strong(old_state, not_locked)) return;

                old_state = state_.exchange(locked_but_no_waiter);
                // Reverse the LIFO waiter stack onto the waiting list so the first waiter resumes first.
                auto current = std::bit_cast<lock_sender::state_base*>(old_state);
                while (current) {
                    old_head = std::exchange(current, std::exchange(current->next_, old_head));
                }
            }
            head_.store(old_head->next_);
            old_head->complete_(old_head);
        }

    private:
        // No state object lives at address 0x01, so 0x01 is a safe sentinel for "not locked".
        static constexpr std::uintptr_t not_locked = 1;
        static constexpr std::uintptr_t locked_but_no_waiter = 0;
        std::atomic<std::uintptr_t> state_ = not_locked; // represent no locked or the top of waiting stack
        std::atomic<lock_sender::state_base*> head_{nullptr}; // waiting list head
    };


    template<
        std::integral CountType = std::atomic_unsigned_lock_free::value_type,
        CountType LeastMaxValue = std::numeric_limits<CountType>::max()
    >
    class async_semaphore {
    public:
        using count_type = CountType;

    private:
        struct state_base {
            using operation_state_concept = execution::operation_state_tag;
            using complete_fn_t = void(*)(state_base*) noexcept;

            state_base(async_semaphore& sema, complete_fn_t complete) noexcept : sema_(sema), complete_(complete) {}

            state_base(const state_base&) = delete;

            auto operator= (const state_base&) -> state_base& = delete;

            async_semaphore& sema_;
            const complete_fn_t complete_;
            state_base* prev_ = nullptr;
            state_base* next_ = nullptr;
        };

        template<typename Rcvr>
        struct state : state_base {
            using stop_token_t = stop_token_of_t<execution::env_of_t<Rcvr>>;

            state(async_semaphore& sema, Rcvr rcvr) noexcept : state_base(sema, &complete), rcvr_(std::move(rcvr)) {}

            KIOTO_ALWAYS_INLINE auto start() & noexcept -> void {
                auto stop_token = kioto::get_stop_token(execution::get_env(rcvr_));
                if constexpr (not unstoppable_token<stop_token_t>) {
                    if (stop_token.stop_requested()) {
                        execution::set_stopped(std::move(rcvr_));
                        return;
                    }

                    stop_cb_.emplace(stop_token, std::bind_front(&state::on_stop_requested, this));
                }

                std::unique_lock guard{this->sema_.mtx_};
                if constexpr (not unstoppable_token<stop_token_t>) {
                    if (stop_token.stop_requested()) {
                        guard.unlock();
                        stop_cb_.reset();
                        execution::set_stopped(std::move(rcvr_));
                        return;
                    }
                }

                if (this->sema_.try_acquire()) {
                    guard.unlock();
                    complete(this);
                    return;
                }

                this->prev_ = std::exchange(this->sema_.waiting_list_tail_, this);
                this->next_ = nullptr;
                if (this->prev_ != nullptr) {
                    this->prev_->next_ = this;
                }
                else {
                    this->sema_.waiting_list_head_ = this;
                }
                guard.unlock();
            }

            static auto complete(state_base* self) noexcept -> void {
                auto this_ = static_cast<state*>(self);
                if constexpr (not unstoppable_token<stop_token_t>) {
                    this_->stop_cb_.reset();
                }
                execution::set_value(std::move(this_->rcvr_));
            }

            auto on_stop_requested() noexcept -> void {
                if constexpr (not unstoppable_token<stop_token_t>) {
                    if (this->sema_.unregister_(this)) {
                        execution::set_stopped(std::move(rcvr_));
                    }
                }
            }

            using stop_cb_t = decltype(std::bind_front(&state::on_stop_requested, std::declval<state*>()));
            Rcvr rcvr_;
            std::optional<stop_callback_for_t<stop_token_t, stop_cb_t>> stop_cb_;
        };

        class acquire_sender {
            friend async_semaphore;
        public:
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                execution::set_value_t(),
                execution::set_stopped_t()
            >;

        public:
            explicit acquire_sender(async_semaphore& sema) noexcept : sema_(&sema) {}

            acquire_sender(const acquire_sender&) = delete;

            acquire_sender(acquire_sender&& other) noexcept : sema_(std::exchange(other.sema_, nullptr)) {}

            auto operator= (acquire_sender other) noexcept -> acquire_sender& {
                std::swap(sema_, other.sema_);
                return *this;
            }

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept -> state<Rcvr> {
                KIOTO_ASSERT(sema_ != nullptr);
                return state<Rcvr>{*std::exchange(sema_, nullptr), std::move(rcvr)};
            }

            template<similar_to<acquire_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures {
                return {};
            }

        private:
            async_semaphore* sema_;
        };

    public:
        explicit async_semaphore(count_type init) noexcept : counter_(init) {
            KIOTO_ASSERT(init >= 0 and init <= max());
        }

        async_semaphore(const async_semaphore&) = delete;

        auto operator= (const async_semaphore&) -> async_semaphore& = delete;

        [[nodiscard]]
        static constexpr auto max() noexcept -> count_type {
            return LeastMaxValue;
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto acquire() noexcept {
            return append_fallback_env(
                execution::affine(acquire_sender{*this}),
                execution::prop{execution::get_start_scheduler, execution::inline_scheduler{}}
            );
        }

        [[nodiscard]]
        auto try_acquire() noexcept -> bool {
            auto current = counter_.load(std::memory_order_acquire);
            do {
                if (current <= 0) return false;
            }
            while (not counter_.compare_exchange_weak(
                current, current - 1,
                std::memory_order_acq_rel, std::memory_order_acquire
            ));
            return true;
        }

        auto release() noexcept -> void {
            state_base* head = nullptr;
            std::unique_lock guard{mtx_};
            if (waiting_list_head_ != nullptr) {
                head = std::exchange(waiting_list_head_, waiting_list_head_->next_);
                if (waiting_list_head_ != nullptr) {
                    waiting_list_head_->prev_ = nullptr;
                }
                else {
                    waiting_list_tail_ = nullptr;
                }
                head->prev_ = nullptr;
                head->next_ = nullptr;
            }
            else {
                auto current = counter_.load(std::memory_order_acquire);
                do {
                    if (current == async_semaphore::max()) std::terminate();
                }
                while (not counter_.compare_exchange_weak(
                    current, current + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire
                ));
            }
            guard.unlock();
            if (head != nullptr) head->complete_(head);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto count() const noexcept -> count_type {
            return counter_.load(std::memory_order_acquire);
        }

    private:
        auto unregister_(state_base* waiter) noexcept -> bool {
            std::scoped_lock _{waiter->sema_.mtx_};
            if (waiter->prev_ != nullptr) {
                waiter->prev_->next_ = waiter->next_;
            }
            else if (waiting_list_head_ == waiter) {
                waiting_list_head_ = waiter->next_;
            }
            else {
                return false;
            }

            if (waiter->next_ != nullptr) {
                waiter->next_->prev_ = waiter->prev_;
            }
            else {
                waiting_list_tail_ = waiter->prev_;
            }

            waiter->prev_ = nullptr;
            waiter->next_ = nullptr;
            return true;
        }

    private:
        std::atomic<count_type> counter_;
        atomutex mtx_;
        state_base* waiting_list_head_ = nullptr;
        state_base* waiting_list_tail_ = nullptr;
    };

    template<typename CountType = std::atomic_unsigned_lock_free::value_type>
    using async_binary_semaphore = async_semaphore<CountType, 1>;


    template<typename CountType = std::atomic_unsigned_lock_free::value_type>
    class async_latch {
    public:
        using count_type = CountType;

    private:
        class wait_sender {
            friend class async_latch<count_type>;
        private:
            struct state_base {
                using operation_state_concept = execution::operation_state_tag;
                using complete_fn_t = void(*)(state_base*) noexcept;

                state_base(async_latch& latch, std::size_t n, complete_fn_t complete) noexcept : latch_(latch), n_(n), complete_(complete) {}

                state_base(const state_base&) = delete;

                auto operator= (const state_base&) -> state_base& = delete;

                async_latch& latch_; // NOLINT(*-avoid-const-or-ref-data-members)
                count_type n_;
                complete_fn_t complete_;
                state_base* next_ = nullptr;
            };

            template<typename Rcvr>
            class state : public state_base {
            public:
                state(async_latch& latch, count_type n, Rcvr rcvr) noexcept :
                    state_base(latch, n, &complete),
                    rcvr_(std::move(rcvr)) {}

                KIOTO_ALWAYS_INLINE auto start() & noexcept -> void {
                    auto& latch = this->latch_;
                    if (latch.count_down(this->n_) == 0) {
                        execution::set_value(std::move(rcvr_));
                        return;
                    }
                    if (latch.waiting_list_.push(*this) == detail::stack_status::empty_but_pushed) {
                        latch.notify_waiters_();
                    }
                }

            private:
                static auto complete(state_base* self) noexcept -> void {
                    auto this_ = static_cast<state*>(self);
                    execution::set_value(std::move(this_->rcvr_));
                }

            private:
                Rcvr rcvr_;
            };

        public:
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<execution::set_value_t()>;

        public:
            wait_sender(async_latch& latch, count_type n) noexcept : latch_(&latch), n_(n) {}

            wait_sender(const wait_sender&) = delete;

            wait_sender(wait_sender&& other) noexcept :
                latch_(std::exchange(other.latch_, {})),
                n_(std::exchange(other.n_, 0)) {}

            auto operator= (const wait_sender&) -> wait_sender& = delete;

            auto operator= (wait_sender&& other) noexcept -> wait_sender& {
                latch_ = std::exchange(other.latch_, {});
                n_ = std::exchange(other.n_, 0);
                return *this;
            }

            template<similar_to<wait_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures {
                return {};
            }

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept -> state<Rcvr> {
                KIOTO_ASSERT(latch_ != nullptr);
                return {*std::exchange(latch_, {}), std::exchange(n_, 0), std::move(rcvr)};
            }

        private:
            async_latch* latch_ = nullptr;
            count_type n_;
        };

    public:
        explicit async_latch(count_type count) noexcept : counter_(count) {}

        async_latch(const async_latch&) = delete;

        auto operator= (const async_latch&) -> async_latch& = delete;

        [[nodiscard]]
        static constexpr auto max() noexcept -> count_type {
            return std::numeric_limits<count_type>::max();
        }

        [[nodiscard]]
        auto count() const noexcept -> count_type {
            return counter_.load(std::memory_order_acquire);
        }

        [[nodiscard]]
        auto try_wait() const noexcept -> bool {
            return count() == 0;
        }

        KIOTO_ALWAYS_INLINE auto count_down(count_type n = 1) noexcept -> count_type {
            auto old = counter_.fetch_sub(n, std::memory_order_acq_rel);
            KIOTO_ASSERT(old >= n);
            if (old == n) notify_waiters_();
            return old - n;
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto wait() noexcept {
            return arrive_and_wait(0);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto arrive_and_wait(count_type n = 1) noexcept {
            return append_fallback_env(
                execution::affine(wait_sender{*this, n}),
                execution::prop{execution::get_start_scheduler, execution::inline_scheduler{}}
            );
        }

    private:
        KIOTO_ALWAYS_INLINE auto notify_waiters_() noexcept -> void {
            auto node = waiting_list_.pop_all();
            while (node) {
                auto next = node->next_;
                node->complete_(node);
                node = next;
            }
        }

    private:
        std::atomic<count_type> counter_;
        detail::atomic_intrusive_stack<typename wait_sender::state_base> waiting_list_{&wait_sender::state_base::next_};
    };
}
