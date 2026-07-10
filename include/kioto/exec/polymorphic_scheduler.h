#pragma once
#include <memory>
#include <optional>
#include <kioto/base/concepts.h>
#include <kioto/exec/execution.h>
#include <kioto/base/new_object.h>
#include <kioto/base/retain_ptr.h>
#include <kioto/base/scope_exit.h>

namespace kioto {
    class polymorphic_scheduler {
    private:
        // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
        struct state_base {
            using operation_state_concept = execution::operation_state_tag;
            using complete_fn_t = void(*)(state_base*) noexcept;

            state_base(complete_fn_t fn) noexcept : complete_(fn) {}

            state_base(const state_base&) = delete;

            auto operator= (const state_base&) -> state_base& = delete;

            const complete_fn_t complete_;
        };

        struct receiver {
            using receiver_concept = execution::receiver_tag;

            auto set_value() && noexcept -> void {
                KIOTO_ASSERT(state_ != nullptr);
                auto state = std::exchange(state_, nullptr);
                state->complete_(state);
            }

            state_base* state_;
        };

        struct state_holder {
            static constexpr std::size_t storage_size = 6 * sizeof(void*);
            static constexpr std::size_t storage_alignment = alignof(void*);

            // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
            struct state_proxy_base {
                state_proxy_base() = default;

                state_proxy_base(const state_proxy_base&) = delete;

                auto operator= (const state_proxy_base&) -> state_proxy_base& = delete;

                virtual auto do_start() noexcept -> void = 0;

                virtual auto delete_self() noexcept -> void = 0;
            };

            // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
            template<typename Sndr, bool>
            struct state_proxy : state_proxy_base {
                using allocator_t = decltype(detail::get_suitable_allocator(std::declval<execution::env_of_t<Sndr>>()));

                state_proxy(Sndr sndr, receiver rcvr) :
                    alloc(detail::get_suitable_allocator(execution::get_env(sndr))),
                    inner(execution::connect(std::move(sndr), std::move(rcvr))) {}

                auto do_start() noexcept -> void override {
                    execution::start(inner);
                }

                auto delete_self() noexcept -> void override {
                    kioto::delete_object(alloc, this);
                }

                allocator_t alloc;
                execution::connect_result_t<Sndr, receiver> inner;
            };

            // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
            template<typename Sndr>
            struct state_proxy<Sndr, true> : state_proxy_base {
                state_proxy(Sndr sndr, receiver rcvr) : inner(execution::connect(std::move(sndr), std::move(rcvr))) {}

                auto do_start() noexcept -> void override {
                    execution::start(inner);
                }

                auto delete_self() noexcept -> void override {
                    std::destroy_at(this);
                }

                execution::connect_result_t<Sndr, receiver> inner;
            };

            template<execution::sender Sndr>
            state_holder(Sndr sndr, receiver rcvr) { // NOLINT(*-pro-type-member-init)
                if constexpr (sizeof(state_proxy<Sndr, true>) <= storage_size and alignof(state_proxy<Sndr, true>) <= storage_alignment) {
                    proxy = ::new(static_cast<void*>(storage)) state_proxy<Sndr, true>(std::move(sndr), std::move(rcvr));
                }
                else {
                    auto alloc = detail::get_suitable_allocator(execution::get_env(sndr));
                    proxy = kioto::new_object<state_proxy<Sndr, false>>(alloc, std::move(sndr), std::move(rcvr));
                }
            }

            state_holder(const state_holder&) = delete;

            ~state_holder() {
                proxy->delete_self();
            }

            auto operator= (const state_holder&) -> state_holder& = delete;

            // ReSharper disable once CppMemberFunctionMayBeConst
            auto do_start() noexcept -> void {
                KIOTO_ASSERT(proxy != nullptr);
                proxy->do_start();
            }

            state_proxy_base* proxy;
            alignas(storage_alignment) std::byte storage[storage_size];
        };

        // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
        struct erased : retain_base<erased> {
            erased() = default;

            erased(const erased&) = delete;

            ~erased() = default;

            auto operator= (const erased&) -> erased& = delete;

            virtual auto do_connect(receiver) -> state_holder = 0;

            virtual auto get_forward_progress_guarantee() const noexcept -> execution::forward_progress_guarantee = 0;

            virtual auto do_lose() noexcept -> void = 0;

            virtual auto equals(const erased*) const noexcept -> bool = 0;

            template<execution::scheduler Sched>
            auto try_get_scheduler() const noexcept -> const Sched* {
                if (auto self = dynamic_cast<const erased_sched<Sched>*>(this)) {
                    return std::addressof(self->sched);
                }
                return nullptr;
            }
        };

        // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
        template<execution::receiver Rcvr>
        struct state : state_base {
            state(Rcvr rcvr, retain_ptr<erased> erased) :
                state_base(&do_complete),
                rcvr_(std::move(rcvr)),
                erased_(std::move(erased)),
                holder_(erased_->do_connect(receiver{this})) {}

            auto start() & noexcept -> void {
                holder_.do_start();
            }

            static auto do_complete(state_base* self) noexcept -> void {
                auto this_ = static_cast<state*>(self);
                execution::set_value(std::move(this_->rcvr_));
            }

            Rcvr rcvr_;
            retain_ptr<erased> erased_;
            state_holder holder_;
        };

        class schedule_sender {
        public:
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<execution::set_value_t()>;

            struct env {
                auto query(execution::get_completion_scheduler_t<execution::set_value_t>) const noexcept -> polymorphic_scheduler {
                    return polymorphic_scheduler(erased_);
                }

                retain_ptr<erased> erased_;
            };

        public:
            explicit schedule_sender(retain_ptr<erased> erased) noexcept : erased_(std::move(erased)) {}

            schedule_sender(const schedule_sender&) = delete;

            schedule_sender(schedule_sender&& other) noexcept = default;

            auto operator= (schedule_sender other) noexcept -> schedule_sender& {
                std::ranges::swap(erased_, other.erased_);
                return *this;
            }

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && {
                KIOTO_ASSERT(erased_ != nullptr);
                return state<Rcvr>{std::move(rcvr), std::move(erased_)};
            }

            template<similar_to<schedule_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept {
                return completion_signatures{};
            }

            KIOTO_ALWAYS_INLINE auto get_env() const noexcept -> env {
                KIOTO_ASSERT(erased_ != nullptr);
                return env{erased_};
            }

        private:
            retain_ptr<erased> erased_;
        };

        // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
        template<typename Sched>
        struct erased_sched : erased {
            explicit erased_sched(Sched sched) noexcept : sched(std::move(sched)) {}

            auto do_connect(receiver rcvr) -> state_holder override {
                return state_holder{execution::schedule(sched), std::move(rcvr)};
            }

            auto get_forward_progress_guarantee() const noexcept -> execution::forward_progress_guarantee override {
                return execution::get_forward_progress_guarantee(sched);
            }

            auto equals(const erased* other_erased) const noexcept -> bool override {
                if (auto other = dynamic_cast<const erased_sched*>(other_erased)) {
                    return this->sched == other->sched;
                }
                return false;
            }

            KIOTO_NO_UNIQUE_ADDRESS Sched sched;
        };

        // ReSharper disable once CppPolymorphicClassWithNonVirtualPublicDestructor
        template<typename Sched, typename Alloc>
        struct erased_for : erased_sched<Sched> {
            using base = erased_sched<Sched>;

            explicit erased_for(Sched sched, Alloc alloc) noexcept : base(std::move(sched)), alloc(std::move(alloc)) {}

            auto do_lose() noexcept -> void override {
                kioto::delete_object(alloc, this);
            }

            KIOTO_NO_UNIQUE_ADDRESS Alloc alloc;
        };

        template<typename Sched, typename Alloc>
        static auto create_erased(Sched sched, Alloc alloc) -> retain_ptr<erased> {
            return retain_ptr<erased>{
                kioto::new_object<erased_for<Sched, Alloc>>(alloc, std::move(sched), std::move(alloc))
            };
        }

    public:
        using scheduler_concept = execution::scheduler_tag;

    private:
        explicit polymorphic_scheduler(retain_ptr<erased> erased) noexcept : erased_(std::move(erased)) {}

    public:
        template<different_from<polymorphic_scheduler> Sched, simple_allocator Alloc = std::allocator<void>>
            requires execution::scheduler<Sched> and infallible_scheduler<Sched, execution::env<>>
        explicit polymorphic_scheduler(Sched sched, Alloc alloc = Alloc()) : erased_(polymorphic_scheduler::create_erased(sched, alloc)) {}

        template<simple_allocator Alloc>
        explicit polymorphic_scheduler(polymorphic_scheduler sched, const Alloc&) noexcept : polymorphic_scheduler(std::move(sched)) {}

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto schedule() const noexcept -> schedule_sender {
            KIOTO_ASSERT(erased_ != nullptr);
            return schedule_sender{erased_};
        }

        KIOTO_ALWAYS_INLINE auto query(execution::get_forward_progress_guarantee_t) const noexcept -> execution::forward_progress_guarantee {
            KIOTO_ASSERT(erased_ != nullptr);
            return erased_->get_forward_progress_guarantee();
        }

        friend auto operator== (const polymorphic_scheduler& lhs, const polymorphic_scheduler& rhs) noexcept -> bool {
            if (lhs.erased_ == nullptr) {
                return rhs.erased_ == nullptr;
            }
            if (rhs.erased_ == nullptr) return false;
            return lhs.erased_->equals(rhs.erased_.get());
        }

        template<different_from<polymorphic_scheduler> Sched> requires execution::scheduler<Sched>
        auto operator== (const Sched& sched) const noexcept -> bool {
            if (erased_ == nullptr) return false;
            auto inner_sched = erased_->try_get_scheduler<Sched>();
            return inner_sched ? *inner_sched == sched : false;
        }

        KIOTO_ALWAYS_INLINE auto swap(polymorphic_scheduler& other) noexcept -> void {
            std::ranges::swap(erased_, other.erased_);
        }

        KIOTO_ALWAYS_INLINE friend auto swap(polymorphic_scheduler& lhs, polymorphic_scheduler& rhs) noexcept -> void {
            lhs.swap(rhs);
        }

        template<different_from<polymorphic_scheduler> Sched> requires execution::scheduler<Sched> and unqualified_object<Sched>
        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto target() const noexcept(std::is_nothrow_copy_constructible_v<Sched>) -> std::optional<Sched> {
            if (erased_ == nullptr) return std::nullopt;
            auto inner_sched = erased_->try_get_scheduler<Sched>();
            return inner_sched ? *inner_sched : std::optional<Sched>{};
        }

    private:
        retain_ptr<erased> erased_;
    };

    static_assert(execution::scheduler<polymorphic_scheduler>);
}

template<typename Alloc>
struct std::uses_allocator<kioto::polymorphic_scheduler, Alloc> : std::true_type {};
