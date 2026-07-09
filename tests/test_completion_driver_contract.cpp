// A portable stand-in with the exact SHAPE of iocp_driver: a completion-model backend where ops issue
// themselves and the driver only reaps, with no native timer op — so timed scheduling rides the shared
// heap mixin (scheduler_mixin = mock_scheduler over timer_scheduler). Compiling and running this on
// every platform is the contract check for the Windows backend's template plumbing: the mixin chain,
// the dual-capability driver, timer_driver::operation heap reuse, and the generic detail::io_sender
// over a completion-model io_state.
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <semaphore>
#include <system_error>
#include <thread>
#include <utility>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/execution_context.h>
#include <coio/time_loop.h>
#include <coio/utils/async_scope.h>
#include <coio/utils/async_result.h>
#include <coio/detail/io_descriptions.h>
#include <coio/detail/io_sender.h>
#include <coio/detail/op_queue.h>

namespace {
    template<typename Executor, typename Base = coio::timer_scheduler<Executor>>
    class mock_scheduler;

    // Stand-in for iocp_node (OVERLAPPED + run-queue node): the driver reaps it and dispatches complete().
    struct mock_node : coio::detail::operation_base {
        virtual auto complete(int value) noexcept -> void = 0;
        auto request_cancel() noexcept -> void { cancel_requested = true; }
        bool cancel_requested = false;
    };

    template<typename IoOp>
    class mock_state_base;

    class mock_driver {
    public:
        using capabilities = coio::type_list<coio::capability::io, coio::capability::timer>;
        template<typename Executor, typename Base>
        using scheduler_mixin = mock_scheduler<Executor, coio::timer_scheduler<Executor, Base>>;

        struct io_ref {
            int fake_handle = -1;
        };

        using supported_io_ops = coio::type_list<coio::detail::async_read_some_t>;
        template<typename IoOp>
        static constexpr bool supports = supported_io_ops::template contains<IoOp>;
        template<typename IoOp>
        using io_state = mock_state_base<IoOp>;

        // ---- executor-facing contract ----
        auto poll(coio::detail::ready_queue& ready, std::size_t) -> void {
            {
                std::scoped_lock _{mtx_};
                timers_.take_ready_timers(ready);
            }
            while (auto* op = completed_.pop_front()) { // "reap the port"
                static_cast<mock_node*>(op)->complete(42);
                ready.push_back(*op);
            }
        }
        auto poll_wait() -> void {
            std::optional<std::chrono::steady_clock::time_point> earliest;
            {
                std::scoped_lock _{mtx_};
                earliest = timers_.earliest();
            }
            if (earliest) static_cast<void>(sema_.try_acquire_until(*earliest));
            else sema_.acquire();
        }
        auto wake_up() noexcept -> void { sema_.release(); }

        // ---- op-facing: "the overlapped syscall" — the op parks itself; completion arrives via poll ----
        auto issue(mock_node& op) -> void {
            completed_.push_back(op);
            wake_up();
        }

        // ---- timer capability (driven by the shared timer_scheduler mixin, like iocp_driver) ----
        auto submit(coio::timer_driver::operation& op) -> void {
            bool became_earliest = false;
            {
                std::scoped_lock _{mtx_};
                became_earliest = timers_.add(op);
            }
            if (became_earliest) wake_up();
        }
        auto remove(coio::timer_driver::operation& op) -> bool {
            std::scoped_lock _{mtx_};
            return timers_.remove(op);
        }

    private:
        coio::detail::ready_queue completed_{&coio::detail::operation_base::next_};
        std::mutex mtx_;
        coio::detail::timer_queue<
            coio::timer_driver::operation, &coio::timer_driver::operation::deadline,
            &coio::timer_driver::operation::heap_index, std::pmr::polymorphic_allocator<>> timers_{std::pmr::polymorphic_allocator<>{}};
        std::counting_semaphore<> sema_{0};
    };

    template<typename IoOp>
    class mock_state_base {
        static_assert(coio::always_false<IoOp>, "this operation isn't supported");
    };

    template<>
    class mock_state_base<coio::detail::async_read_some_t> : public mock_node {
    public:
        mock_state_base(mock_driver& driver, mock_driver::io_ref ref, coio::detail::async_read_some_t) noexcept
            : driver_(driver), ref_(ref) {}

    protected:
        auto do_start() noexcept -> bool {
            if (ref_.fake_handle < 0) [[unlikely]] {
                // Pre-syscall failure: result set, false -> the op-state posts itself to the run queue.
                result.set_error(std::make_error_code(std::errc::bad_file_descriptor));
                return false;
            }
            driver_.issue(*this);
            return true;
        }

        auto complete(int value) noexcept -> void override { result.set_value(static_cast<std::size_t>(value)); }

        // Generic io_sender hooks, iocp-shaped: cancellation completes through the completion flow.
        auto try_cancel() noexcept -> bool {
            this->request_cancel();
            return false;
        }
        static auto on_finish() noexcept -> void {}

        coio::async_result<coio::execution::set_value_t(std::size_t), coio::execution::set_error_t(std::error_code)> result;

    private:
        mock_driver& driver_;
        mock_driver::io_ref ref_;
    };

    template<typename Executor, typename Base>
    class mock_scheduler : public Base {
    public:
        using scheduler_concept = coio::detail::io_scheduler_tag;
        using Base::Base;

        class io_handle {
            friend mock_scheduler;
        public:
            io_handle(Executor& ctx, int fake_handle) noexcept : ctx_(&ctx), fake_handle_(fake_handle) {}

        private:
            [[nodiscard]] auto ref() const noexcept -> mock_driver::io_ref { return {fake_handle_}; }

            Executor* ctx_;
            int fake_handle_;
        };

        [[nodiscard]] auto make_io_handle(int fake_handle) const -> io_handle {
            return io_handle{*this->ctx_, fake_handle};
        }

        template<typename IoOp>
        [[nodiscard]] auto schedule_io(io_handle& obj, IoOp op) const noexcept {
            return coio::detail::schedule_io(*this->ctx_, obj.ref(), std::move(op));
        }
    };

    using mock_context = coio::executor<mock_driver>;
}

TEST_CASE("a completion-model driver with heap timers composes and completes through the generic io machinery") {
    static_assert(mock_context::has_capability<coio::capability::io>);
    static_assert(mock_context::has_capability<coio::capability::timer>);
    static_assert(coio::timed_scheduler<mock_context::scheduler>);     // schedule_after inherited from the timer mixin
    static_assert(mock_driver::supports<coio::detail::async_read_some_t>);
    static_assert(not mock_driver::supports<coio::detail::async_connect_t>);

    mock_context ctx;
    auto sched = ctx.get_scheduler();
    coio::async_scope scope;
    std::optional<coio::work_guard<mock_context>> guard{std::in_place, ctx};

    std::jthread driver{[&ctx] { ctx.run(); }};

    std::atomic<std::size_t> io_result{0};
    std::atomic<bool> timer_fired{false};
    std::atomic<bool> sync_error{false};

    auto good = sched.make_io_handle(7);
    auto bad = sched.make_io_handle(-1);

    // Async path: do_start parks the op; poll reaps it and complete() delivers 42.
    scope.spawn_on(sched, sched.schedule_io(good, coio::detail::async_read_some_t{})
        | coio::then([&](std::size_t n) { io_result.store(n, std::memory_order_relaxed); }));
    // Sync-failure path: do_start returns false with the error set; operation_state posts the finish.
    scope.spawn_on(sched, sched.schedule_io(bad, coio::detail::async_read_some_t{})
        | coio::then([](std::size_t) noexcept {})
        | coio::upon_error([&](auto&& error) {
              if constexpr (std::same_as<std::decay_t<decltype(error)>, std::error_code>)
                  sync_error.store(true, std::memory_order_relaxed);
          }));
    // Timer path: schedule_after rides the driver's heap and bounds poll_wait.
    scope.spawn_on(sched, sched.schedule_after(std::chrono::milliseconds{1})
        | coio::then([&] { timer_fired.store(true, std::memory_order_relaxed); }));

    guard.reset();
    coio::this_thread::sync_wait(scope.join());
    ctx.request_stop();
    driver.join();

    CHECK_EQ(io_result.load(), 42);
    CHECK(timer_fired.load());
    CHECK(sync_error.load());
}
