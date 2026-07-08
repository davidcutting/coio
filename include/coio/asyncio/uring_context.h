// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <coio/detail/config.h>
#if not COIO_HAS_IO_URING
#error "uh, where is <liburing.h>?"
#endif
#include <liburing.h>
#include <netinet/in.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>
#include <coio/execution_context.h>
#include <coio/utils/async_result.h>
#include <coio/utils/stop_token.h>
#include <coio/detail/io_descriptions.h>

namespace coio {
    // Capability tag: an executor owning a uring_driver can do io + timers.
    struct uring_cap {};

    template<typename Executor>
    class uring_scheduler;

    namespace detail {
        template<typename Sexpr>
        class uring_state_base_for;
    }

    // The io_uring driver: owns the ring, submits SQEs, reaps CQEs, and blocks/wakes the executor. This
    // is the old uring_context reactor MINUS the run loop (which now lives in executor). The single-owner
    // and owner-thread-teardown invariants are unchanged — only cross-thread entry points are wake_up()
    // and request_cancel().
    class uring_driver {
        template<typename Sexpr>
        friend class detail::uring_state_base_for;

    public:
        using capability = uring_cap;
        template<typename Executor>
        using scheduler_of = uring_scheduler<Executor>;

        // = the old uring_node. Cancellation state + hooks the driver dispatches through.
        struct operation : detail::operation_base {
            // The ONLY cross-thread transition is active -> cancel_queued (a foreign thread requesting a
            // stop); the rest are owner-only, so a single CAS from `active` is the whole synchronisation.
            enum cancel_state : std::uint8_t { active, cancel_queued, drained, completed };

            explicit operation(uring_driver& driver) noexcept : driver_(driver) {}

            auto do_cancel() -> void;         // stop-token callback (any thread)
            auto submit_cancel() -> void;     // OWNER thread only (touches the ring)

            virtual auto prepare(::io_uring_sqe* sqe) noexcept -> void = 0;   // Sexpr-specialized (in the .cpp)
            virtual auto complete(int cqe_res) -> void = 0;

            uring_driver& driver_; // NOLINT(*-avoid-const-or-ref-data-members)
            std::atomic<std::uint8_t> cancel_state_{active};
            operation* cancel_link_ = nullptr;   // intrusive link for cancel_stack_
            bool completion_ready_ = false;       // owner-only: CQE arrived while a cancel was queued
        };

        explicit uring_driver(std::size_t entries);
        uring_driver();
        uring_driver(const uring_driver&) = delete;
        ~uring_driver();
        auto operator= (const uring_driver&) -> uring_driver& = delete;

        // ---- executor-facing contract ----
        auto poll(detail::ready_queue& ready, std::size_t batch) -> void;
        auto poll_wait() -> void;
        auto wake_up() noexcept -> void;

        // ---- op-facing ----
        auto submit(operation& op) -> bool;   // grab an SQE, op.prepare(it), set_data; false if exhausted
        auto cancel_fd(int fd) -> void;        // OWNER only: cancel all ops on fd (io_object teardown)

        [[nodiscard]] auto get_uring() noexcept -> ::io_uring* { return &uring_; }
        [[nodiscard]] auto is_owner() const noexcept -> bool {
            return owner_.load(std::memory_order_relaxed) == std::this_thread::get_id();
        }

    private:
        auto allocate_sqe() noexcept -> ::io_uring_sqe*;
        auto submit_sqes() -> void;
        auto post_submit_sqes() -> void;
        auto request_cancel(operation& op) -> void;
        auto drain_cancels(detail::ready_queue& ready) -> void;
        auto classify(detail::ready_queue& ready, void* user_data, int res) -> void;

        std::size_t pending_sqes_ = 0;
        bool enabled_ = false;
        std::atomic<std::thread::id> owner_{};
        ::io_uring uring_{};
        detail::atomic_intrusive_stack<operation> cancel_stack_{&operation::cancel_link_};
    };

    namespace detail {
        template<typename Sexpr>
        struct uring_sexpr_wrapper {
            using type = Sexpr;
        };

        template<>
        struct uring_sexpr_wrapper<async_send_to_t> {
            struct type {
                type(async_send_to_t s) noexcept;
                std::variant<::sockaddr_in, ::sockaddr_in6> peer;
                ::iovec buffer;
                ::msghdr msg;
            };
        };

        template<>
        struct uring_sexpr_wrapper<async_receive_from_t> {
            struct type {
                type(async_receive_from_t s) noexcept;
                ::sockaddr_storage peer;
                ::iovec buffer;
                ::msghdr msg;
            };
        };

        template<>
        struct uring_sexpr_wrapper<async_connect_t> {
            struct type {
                type(async_connect_t s) noexcept;
                std::variant<::sockaddr_in, ::sockaddr_in6> peer;
            };
        };

        template<>
        struct uring_sexpr_wrapper<async_sleep_t> {
            struct type {
                type(async_sleep_t s) noexcept;
                ::__kernel_timespec ts;   // absolute (CLOCK_MONOTONIC) deadline; must outlive the SQE
            };
        };

        // = the old uring_state_base_for: the Sexpr-typed op body. Re-parented onto uring_driver::operation
        // (was uring_context::uring_node). Sexpr-only, so the prepare()/complete() specializations in the
        // .cpp are unchanged. do_start() now hands the op to the driver, which fills the SQE via prepare().
        template<typename Sexpr>
        class uring_state_base_for : private uring_sexpr_wrapper<Sexpr>::type, public uring_driver::operation {
            using base1 = typename uring_sexpr_wrapper<Sexpr>::type;

        public:
            uring_state_base_for(int fd, uring_driver& driver, Sexpr sexpr) noexcept :
                base1(std::move(sexpr)), uring_driver::operation(driver), fd(fd) {}

        protected:
            auto prepare(::io_uring_sqe*) noexcept -> void override {
                static_assert(always_false<Sexpr>, "this operation isn't supported");
            }

            auto do_start() noexcept -> bool {
                if (not driver_.submit(*this)) {
                    result.set_error(std::make_error_code(std::errc::no_buffer_space));
                    return false;
                }
                return true;
            }

            auto complete(int cqe_res) -> void override {
                if (cqe_res < 0) {
                    const std::error_code ec{-cqe_res, std::system_category()};
                    if (ec == std::errc::operation_canceled) result.set_stopped();
                    else result.set_error(ec);
                }
                else {
                    if constexpr (std::same_as<typename Sexpr::value_signature, execution::set_value_t()>) result.set_value();
                    else result.set_value(cqe_res);
                }
            }

        protected:
            int fd;
            async_result<typename Sexpr::value_signature, execution::set_error_t(std::error_code)> result;
        };

        // prepare() specializations (bodies in uring_context.cpp — the sole home of io_uring_prep_*):
        template<> auto uring_state_base_for<async_read_some_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_write_some_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_read_some_at_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_write_some_at_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_receive_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_send_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_receive_from_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_receive_from_t>::complete(int) -> void;
        template<> auto uring_state_base_for<async_send_to_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_accept_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_connect_t>::prepare(::io_uring_sqe*) noexcept -> void;
        // A1 timer: TIMEOUT SQE + -ETIME-as-success completion.
        template<> auto uring_state_base_for<async_sleep_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_sleep_t>::complete(int) -> void;
    }

    // The uring-capable scheduler: the bare placement scheduler + io ops + timers. Lives here (with
    // liburing) so execution_context.h stays backend-clean. executor<uring_driver>::scheduler resolves
    // to this via uring_driver::scheduler_of.
    template<typename Executor>
    class uring_scheduler : public detail::executor_scheduler<Executor> {
        using base = detail::executor_scheduler<Executor>;

    public:
        using scheduler_concept = detail::io_scheduler_tag;
        using base::base;

        class io_object {
            friend uring_scheduler;
        public:
            io_object(Executor& ctx, int fd) noexcept : ctx_(&ctx), fd_(fd) {}
            io_object(const io_object&) = delete;
            io_object(io_object&& other) noexcept : ctx_(other.ctx_), fd_(std::exchange(other.fd_, -1)) {}
            ~io_object() { cancel(); }

            auto operator= (io_object other) noexcept -> io_object& { swap(other); return *this; }
            auto swap(io_object& other) noexcept -> void { std::ranges::swap(ctx_, other.ctx_); std::ranges::swap(fd_, other.fd_); }
            friend auto swap(io_object& a, io_object& b) noexcept -> void { a.swap(b); }

            [[nodiscard]] auto get_io_scheduler() const noexcept -> uring_scheduler {
                COIO_ASSERT(ctx_ != nullptr);
                return uring_scheduler{*ctx_};
            }
            [[nodiscard]] auto native_handle() const noexcept -> int { return fd_; }

            auto release() -> int { cancel(); return std::exchange(fd_, -1); }
            auto cancel() -> void {
                if (fd_ == -1) return;
                // Teardown submits into the ring -> owner only (the single issuer). Pin sticky io to its
                // worker so its destructor runs on-owner.
                ctx_->template get_driver<uring_cap>().cancel_fd(fd_);
            }

        private:
            Executor* ctx_;
            int fd_ = -1;
        };

        template<typename Sexpr>
        struct io_sender {
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                typename Sexpr::value_signature,
                execution::set_error_t(std::error_code),
                execution::set_stopped_t()
            >;

            template<typename Rcvr>
            struct state_base : detail::uring_state_base_for<Sexpr> {
                state_base(Executor& ctx, int fd, Sexpr sexpr, Rcvr rcvr) noexcept
                    : detail::uring_state_base_for<Sexpr>(fd, ctx.template get_driver<uring_cap>(), std::move(sexpr)),
                      context_(ctx), rcvr_(std::move(rcvr)) {}

                COIO_ALWAYS_INLINE auto do_finish(bool) noexcept -> void { this->result.forward_to(std::move(rcvr_)); }

                Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members) — for work_started/finished
                Rcvr rcvr_;
            };
            template<typename Rcvr>
            using state = detail::operation_state<state_base<Rcvr>>;

            template<execution::receiver Rcvr>
            COIO_ALWAYS_INLINE auto connect(Rcvr rcvr) && {
                COIO_ASSERT(context != nullptr);
                return state<Rcvr>{*std::exchange(context, nullptr), fd, std::move(sexpr), std::move(rcvr)};
            }

            template<similar_to<io_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }

            COIO_ALWAYS_INLINE auto get_env() const noexcept -> detail::exec_env<Executor> { return {*context}; }

            int fd;
            Executor* context;
            Sexpr sexpr;
        };

        [[nodiscard]] auto make_io_object(int fd) const -> io_object {
            return io_object{*this->ctx_, fd};
        }

        template<typename Sexpr>
        [[nodiscard]] auto schedule_io(io_object& obj, Sexpr sexpr) const noexcept {
            return stop_when(io_sender<Sexpr>{obj.fd_, this->ctx_, std::move(sexpr)}, this->ctx_->get_stop_token());
        }

        [[nodiscard]] static auto now() noexcept -> std::chrono::steady_clock::time_point {
            return std::chrono::steady_clock::now();
        }
        template<typename Rep, typename Period>
        [[nodiscard]] auto schedule_after(std::chrono::duration<Rep, Period> d) const noexcept {
            return schedule_at(now() + d);
        }
        [[nodiscard]] auto schedule_at(std::chrono::steady_clock::time_point deadline) const noexcept {
            return stop_when(io_sender<detail::async_sleep_t>{-1, this->ctx_, detail::async_sleep_t{deadline}},
                             this->ctx_->get_stop_token());
        }
    };

    // The public io context type: a single-owner executor whose one driver is the ring.
    using uring_context = executor<uring_driver>;
}
