// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>
#include <semaphore>
#include <utility>
#include <kioto/io/execution_context.h>
#include <kioto/base/op_queue.h>
#include <kioto/exec/stop_token.h>
#include <kioto/base/suppress_push.h> // IWYU pragma: keep

namespace kioto {
    template<typename Executor, typename Base = detail::executor_scheduler<Executor>>
    class timer_scheduler;

    // Userspace run-loop worker: no kernel reactor, just a semaphore to park on + a deadline heap. It's a
    // reactor-less CPU worker (capability::cpu) that also serves timers (capability::timer). `time_loop` =
    // executor<timer_driver>; schedule_after() posts a timer here.
    class timer_driver {
    public:
        using capabilities = type_list<capability::cpu, capability::timer>;
        // Scheduler fragment contributed (see detail::compose_scheduler): now/schedule_at/schedule_after
        // layered onto Base's surface.
        template<typename Executor, typename Base>
        using scheduler_mixin = timer_scheduler<Executor, Base>;

        struct operation : detail::operation_base {
            std::chrono::steady_clock::time_point deadline;
            std::size_t heap_index = static_cast<std::size_t>(-1);
        };

        explicit timer_driver(std::pmr::memory_resource& mr = *std::pmr::get_default_resource())
            : heap_(std::pmr::polymorphic_allocator<>{&mr}) {}

        auto poll(detail::ready_queue& ready, std::size_t /*batch*/) -> void {
            std::scoped_lock _{mtx_};
            heap_.take_ready_timers(ready);
        }

        auto poll_wait() -> void {
            std::optional<std::chrono::steady_clock::time_point> earliest;
            { std::scoped_lock _{mtx_}; earliest = heap_.earliest(); }
            if (earliest) static_cast<void>(sema_.try_acquire_until(*earliest));
            else sema_.acquire();
        }

        auto wake_up() noexcept -> void { sema_.release(); }

        auto submit(operation& op) -> void {
            bool became_earliest = false;
            { std::scoped_lock _{mtx_}; became_earliest = heap_.add(op); }
            if (became_earliest) wake_up(); // re-evaluate the deadline in poll_wait
        }

        auto remove(operation& op) -> bool {
            std::scoped_lock _{mtx_};
            return heap_.remove(op);
        }

    private:
        std::mutex mtx_;
        detail::timer_queue<operation, &operation::deadline, &operation::heap_index, std::pmr::polymorphic_allocator<>> heap_;
        std::counting_semaphore<> sema_{0};
    };

    // A scheduler mixin (not standalone): adds the timer senders on top of Base (executor_scheduler or
    // another driver's mixin). The default Base keeps `timer_scheduler<Ex>` == time_loop's scheduler type.
    template<typename Executor, typename Base>
    class timer_scheduler : public Base {
        struct sleep_sender {
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                execution::set_value_t(), execution::set_stopped_t()>;

            template<typename Rcvr>
            struct state_base : timer_driver::operation {
                state_base(Executor& ctx, std::chrono::steady_clock::time_point when, Rcvr rcvr) noexcept
                    : context_(ctx), rcvr_(std::move(rcvr)) { this->deadline = when; }

                KIOTO_ALWAYS_INLINE auto do_start() noexcept -> bool {
                    context_.template get_driver<capability::timer>().submit(*this);
                    return true;
                }
                KIOTO_ALWAYS_INLINE auto do_finish(bool canceled) noexcept -> void {
                    if (canceled) execution::set_stopped(std::move(rcvr_));
                    else execution::set_value(std::move(rcvr_));
                }
                auto do_cancel() -> void {
                    if (context_.template get_driver<capability::timer>().remove(*this)) context_.submit(*this);
                }

                Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members)
                Rcvr rcvr_;
            };
            template<typename Rcvr>
            using state = detail::operation_state<state_base<Rcvr>>;

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept -> state<Rcvr> {
                KIOTO_ASSERT(ctx_ != nullptr);
                return state<Rcvr>{*std::exchange(ctx_, {}), when_, std::move(rcvr)};
            }
            template<similar_to<sleep_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
            KIOTO_ALWAYS_INLINE auto get_env() const noexcept -> detail::exec_env<Executor> { return {*ctx_}; }

            Executor* ctx_;
            std::chrono::steady_clock::time_point when_;
        };

    public:
        using Base::Base;

        [[nodiscard]] static auto now() noexcept -> std::chrono::steady_clock::time_point {
            return std::chrono::steady_clock::now();
        }
        template<typename Rep, typename Period>
        [[nodiscard]] auto schedule_after(std::chrono::duration<Rep, Period> d) const noexcept {
            return schedule_at(now() + d);
        }
        [[nodiscard]] auto schedule_at(std::chrono::steady_clock::time_point when) const noexcept {
            return stop_when(sleep_sender{this->ctx_, when}, this->ctx_->get_stop_token());
        }
    };

    // The public timer/CPU context: a single-owner executor whose one driver is the timer heap.
    using time_loop = executor<timer_driver>;
}

#include <kioto/base/suppress_pop.h> // IWYU pragma: keep
