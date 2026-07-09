// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <coio/detail/config.h>
#if not COIO_HAS_EPOLL
#error "uh, where is <sys/epoll.h>?"
#endif
#include <sys/epoll.h>
#include <sys/socket.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <ranges>
#include <utility>
#include <coio/execution_context.h>
#include <coio/utils/async_result.h>
#include <coio/utils/atomutex.h>
#include <coio/utils/stop_token.h>
#include <coio/detail/io_descriptions.h>

namespace coio {
    struct epoll_cap {};

    template<typename Executor>
    class epoll_scheduler;

    namespace detail {
        template<typename Sexpr>
        class epoll_state_base_for;

        class reactor_interrupter {
        public:
            reactor_interrupter();
            reactor_interrupter(const reactor_interrupter&) = delete;
            ~reactor_interrupter();
            auto operator= (const reactor_interrupter&) -> reactor_interrupter& = delete;
            auto interrupt() -> void;
            auto reset() -> bool;
            [[nodiscard]] COIO_ALWAYS_INLINE auto watcher() const noexcept -> int { return reader_; }
        private:
            int reader_;
            int writer_;
        };

        // fstat/fcntl(O_NONBLOCK) validation for an io_object fd (throws). Defined in the .cpp so the
        // syscall detail stays out of the template io_object.
        auto epoll_prepare_fd(int fd) -> void;

        // timerfd-backed timers for epoll's schedule_after — A1 for epoll: a timer is a timerfd readiness
        // op. Defined in the .cpp (timerfd_* syscalls stay out of the template scheduler).
        auto epoll_arm_timerfd(std::chrono::steady_clock::time_point deadline) noexcept -> int; // -> fd or -1
        auto epoll_drain_timerfd(int fd) noexcept -> void;
        auto epoll_close_timer(int epoll_fd, int fd) noexcept -> void; // EPOLL_CTL_DEL + close
    }

    // The epoll driver: a readiness reactor. Unlike uring (completion), an op registers fd interest and
    // performs the syscall when the fd signals. Was epoll_context minus the run loop.
    class epoll_driver {
        template<typename Sexpr>
        friend class detail::epoll_state_base_for;

    public:
        using capability = epoll_cap;
        template<typename Executor>
        using scheduler_of = epoll_scheduler<Executor>;

        struct operation;

        struct per_fd_data {
            atomutex fd_lock;
            std::uint32_t events{};
            operation* in_op{nullptr};
            operation* out_op{nullptr};
        };

        struct operation : detail::operation_base {
            operation(epoll_driver& driver, int fd, per_fd_data* data) noexcept : driver_(driver), fd(fd), data(data) {}

        protected:
            [[nodiscard]] auto register_event(int event_type, std::uint32_t extra_flags) noexcept -> bool;

        private:
            virtual auto perform() noexcept -> bool = 0;

        protected:
            epoll_driver& driver_; // NOLINT(*-avoid-const-or-ref-data-members)
            int fd;
            per_fd_data* data;
            int registered_event_ = 0; // EPOLLIN/EPOLLOUT this op registered, so cancel can deregister it
            friend epoll_driver;
        };

        epoll_driver();
        epoll_driver(const epoll_driver&) = delete;
        ~epoll_driver();
        auto operator= (const epoll_driver&) -> epoll_driver& = delete;

        // executor-facing contract
        auto poll(detail::ready_queue& ready, std::size_t batch) -> void;
        auto poll_wait() -> void;
        auto wake_up() noexcept -> void { interrupter_.interrupt(); }

        // op-facing. Posting the deregistered ops back to the run queue is the CALLER's job (it holds the
        // executor): a cancelling op posts itself via its context_; io_object teardown posts via its ctx_.
        auto deregister(int event, operation* op) -> bool;                // drop op's registration; true if it was registered
        auto cancel_all(per_fd_data* data) -> std::array<operation*, 2>;  // io_object teardown: return the deregistered ops
        auto release_fd(int fd, per_fd_data* data) -> void;               // io_object release: EPOLL_CTL_DEL
        [[nodiscard]] auto epoll_fd() const noexcept -> int { return epoll_fd_; }

    private:
        auto process_ready(detail::ready_queue& ready) -> void;

        int epoll_fd_;
        detail::reactor_interrupter interrupter_;
        ::epoll_event events_[128];
        int event_count_ = 0;
    };

    namespace detail {
        template<typename Sexpr>
        struct epoll_sexpr_wrapper {
            using type = Sexpr;
        };

        template<>
        struct epoll_sexpr_wrapper<async_receive_from_t> {
            struct type {
                type(async_receive_from_t s) : peer{}, buffer(s.buffer) {}
                ::sockaddr_storage peer;
                std::span<std::byte> buffer;
            };
        };

        template<typename Sexpr>
        class epoll_state_base_for : private epoll_sexpr_wrapper<Sexpr>::type, public epoll_driver::operation {
            using base1 = typename epoll_sexpr_wrapper<Sexpr>::type;

        public:
            epoll_state_base_for(int fd, epoll_driver& driver, epoll_driver::per_fd_data* data, Sexpr sexpr) noexcept :
                base1(std::move(sexpr)), epoll_driver::operation(driver, fd, data) {}

        protected:
            auto do_start() noexcept -> bool { static_assert(always_false<Sexpr>, "this operation isn't supported"); unreachable(); }
            auto do_perform() noexcept -> bool { static_assert(always_false<Sexpr>, "this operation isn't supported"); unreachable(); }
            auto perform() noexcept -> bool override { return do_perform(); }

        protected:
            async_result<typename Sexpr::value_signature, execution::set_error_t(std::error_code)> result;
        };

        template<> auto epoll_state_base_for<async_read_some_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_read_some_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_write_some_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_write_some_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_send_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_send_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_from_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_from_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_send_to_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_send_to_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_accept_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_accept_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_connect_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_connect_t>::do_perform() noexcept -> bool;
    }

    template<typename Executor>
    class epoll_scheduler : public detail::executor_scheduler<Executor> {
        using base = detail::executor_scheduler<Executor>;

    public:
        using scheduler_concept = detail::io_scheduler_tag;
        using base::base;

        // Internal, facaded by socket/file (held as their impl_). Precondition: the owning facade — and
        // hence this io_object — outlives every operation issued on it. That is not an extra rule: a
        // schedule_io sender captures &impl_ and the awaiting coroutine holds the facade by reference, so
        // destroying the facade with I/O in flight is already a use-after-free of the facade itself.
        // Teardown cancel() is therefore best-effort for orderly shutdown (stop requested, loop drained,
        // THEN facades destroyed) — not a license to destroy with a foreign request_stop still in flight,
        // which would race the free of data_ against a not-yet-run finish().
        class io_object {
            friend epoll_scheduler;
            io_object(std::nullptr_t, Executor& ctx, int fd)
                : ctx_(&ctx), fd_(fd),
                  data_(ctx.get_allocator().template new_object<epoll_driver::per_fd_data>()) {} // executor's mr
        public:
            io_object(Executor& ctx, int fd) : io_object(nullptr, ctx, fd) { detail::epoll_prepare_fd(fd); }
            io_object(const io_object&) = delete;
            io_object(io_object&& other) noexcept
                : ctx_(other.ctx_), fd_(std::exchange(other.fd_, -1)), data_(std::exchange(other.data_, {})) {}
            ~io_object() { if (data_ != nullptr) reclaim(cancel()); }

            auto operator= (io_object other) noexcept -> io_object& { swap(other); return *this; }
            auto swap(io_object& o) noexcept -> void {
                std::ranges::swap(ctx_, o.ctx_); std::ranges::swap(fd_, o.fd_); std::ranges::swap(data_, o.data_);
            }
            friend auto swap(io_object& a, io_object& b) noexcept -> void { a.swap(b); }

            [[nodiscard]] auto get_io_scheduler() const noexcept -> epoll_scheduler { return epoll_scheduler{*ctx_}; }
            [[nodiscard]] auto native_handle() const noexcept -> int { return fd_; }

            auto release() -> int {
                if (fd_ == -1) return -1;
                reclaim(cancel());
                return std::exchange(fd_, -1);
            }
            // Teardown deregisters any in-flight ops and posts them (as stopped) via our own executor.
            // Returns whether any live op was pulled (its stop callback is still armed until finish() runs).
            auto cancel() -> bool {
                if (fd_ == -1) return false;
                bool had_ops = false;
                for (auto* op : ctx_->template get_driver<epoll_cap>().cancel_all(data_)) {
                    if (op != nullptr) { ctx_->submit(*op); had_ops = true; }
                }
                return had_ops;
            }

        private:
            // Deregister the fd and reclaim per_fd_data. The free MUST land on the OWNER thread unless we are
            // already the owner and cancelled nothing, because two racing readers can still touch data_:
            //   (a) a just-cancelled op is queued with its stop callback still armed and can deref data_ via
            //       do_cancel() until its finish() runs; and
            //   (b) the owner may hold buffered epoll events (e.g. a peer-close EPOLLHUP) whose data.ptr is
            //       this data_, processed on the next poll().
            // Deferring to the owner puts the free AFTER both (FIFO behind the cancel-ops; and after the
            // owner's event drain), so every reader sees a VALID data_ and backs off. release_fd's
            // EPOLL_CTL_DEL first stops any NEW events. On-owner + idle has no such reader -> free inline.
            auto reclaim(bool had_ops) -> void {
                if (fd_ != -1) ctx_->template get_driver<epoll_cap>().release_fd(fd_, data_);
                if (had_ops or not ctx_->is_owner())
                    detail::defer_to_owner(*ctx_, [ctx = ctx_, d = data_] { ctx->get_allocator().delete_object(d); });
                else
                    ctx_->get_allocator().delete_object(data_);
                data_ = nullptr;
            }

            Executor* ctx_;
            int fd_ = -1;
            epoll_driver::per_fd_data* data_ = nullptr;
        };

        template<typename Sexpr>
        struct io_sender {
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                typename Sexpr::value_signature, execution::set_error_t(std::error_code), execution::set_stopped_t()>;

            template<typename Rcvr>
            struct state_base : detail::epoll_state_base_for<Sexpr> {
                state_base(Executor& ctx, int fd, epoll_driver::per_fd_data* data, Sexpr sexpr, Rcvr rcvr) noexcept
                    : detail::epoll_state_base_for<Sexpr>(fd, ctx.template get_driver<epoll_cap>(), data, std::move(sexpr)),
                      context_(ctx), rcvr_(std::move(rcvr)) {}
                COIO_ALWAYS_INLINE auto do_finish(bool) noexcept -> void { this->result.forward_to(std::move(rcvr_)); }
                // Uniform across all io Sexprs: deregister the event we registered, and if we were still
                // registered, post ourselves (as stopped) via our own executor.
                auto do_cancel() -> void {
                    if (this->registered_event_ != 0 and this->driver_.deregister(this->registered_event_, this))
                        context_.submit(*this);
                }
                Executor& context_; // NOLINT
                Rcvr rcvr_;
            };
            template<typename Rcvr>
            using state = detail::operation_state<state_base<Rcvr>>;

            template<execution::receiver Rcvr>
            COIO_ALWAYS_INLINE auto connect(Rcvr rcvr) && {
                COIO_ASSERT(context != nullptr and data != nullptr);
                return state<Rcvr>{*std::exchange(context, nullptr), std::exchange(fd, -1),
                                   std::exchange(data, nullptr), std::move(sexpr), std::move(rcvr)};
            }
            template<similar_to<io_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
            COIO_ALWAYS_INLINE auto get_env() const noexcept -> detail::exec_env<Executor> { return {*context}; }

            int fd;
            Executor* context;
            epoll_driver::per_fd_data* data;
            Sexpr sexpr;
        };

        [[nodiscard]] auto make_io_object(int fd) const -> io_object { return io_object{*this->ctx_, fd}; }

        template<typename Sexpr>
        [[nodiscard]] auto schedule_io(io_object& obj, Sexpr sexpr) const noexcept {
            return stop_when(io_sender<Sexpr>{obj.fd_, this->ctx_, obj.data_, std::move(sexpr)}, this->ctx_->get_stop_token());
        }

        // A timer on epoll is a timerfd readiness op: it owns its timerfd + a private per_fd_data, arms the
        // timerfd to the deadline, waits EPOLLIN, and completes when it fires. Teardown DEL+closes the fd.
        struct sleep_sender {
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                execution::set_value_t(), execution::set_error_t(std::error_code), execution::set_stopped_t()>;

            template<typename Rcvr>
            struct state_base : epoll_driver::operation {
                state_base(Executor& ctx, std::chrono::steady_clock::time_point when, Rcvr rcvr) noexcept
                    : epoll_driver::operation(ctx.template get_driver<epoll_cap>(), -1, &own_data_),
                      context_(ctx), when_(when), rcvr_(std::move(rcvr)) {}
                ~state_base() { if (this->fd != -1) detail::epoll_close_timer(driver_.epoll_fd(), this->fd); }

                COIO_ALWAYS_INLINE auto do_start() noexcept -> bool {
                    const int tfd = detail::epoll_arm_timerfd(when_);
                    if (tfd == -1) { result_.set_error(std::error_code{errno, std::system_category()}); return false; }
                    this->fd = tfd;
                    if (not register_event(EPOLLIN, 0)) { result_.set_error(std::error_code{errno, std::system_category()}); return false; }
                    return true;
                }
                COIO_ALWAYS_INLINE auto do_finish(bool canceled) noexcept -> void {
                    if (canceled) execution::set_stopped(std::move(rcvr_));
                    else result_.forward_to(std::move(rcvr_));
                }
                auto do_cancel() -> void { if (driver_.deregister(EPOLLIN, this)) context_.submit(*this); }
                auto perform() noexcept -> bool override {
                    detail::epoll_drain_timerfd(this->fd);
                    result_.set_value();
                    return true;
                }

                epoll_driver::per_fd_data own_data_;
                Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members)
                std::chrono::steady_clock::time_point when_;
                Rcvr rcvr_;
                async_result<execution::set_value_t(), execution::set_error_t(std::error_code)> result_;
            };
            template<typename Rcvr>
            using state = detail::operation_state<state_base<Rcvr>>;

            template<execution::receiver Rcvr>
            COIO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept -> state<Rcvr> {
                COIO_ASSERT(ctx_ != nullptr);
                return state<Rcvr>{*std::exchange(ctx_, {}), when_, std::move(rcvr)};
            }
            template<similar_to<sleep_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
            COIO_ALWAYS_INLINE auto get_env() const noexcept -> detail::exec_env<Executor> { return {*ctx_}; }

            Executor* ctx_;
            std::chrono::steady_clock::time_point when_;
        };

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

    using epoll_context = executor<epoll_driver>;
}
