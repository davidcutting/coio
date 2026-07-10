// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <kioto/base/config.h>
#if not KIOTO_HAS_EPOLL
#error "uh, where is <sys/epoll.h>?"
#endif
#include <sys/epoll.h>
#include <sys/socket.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <ranges>
#include <utility>
#include <kioto/io/execution_context.h>
#include <kioto/exec/async_result.h>
#include <kioto/base/atomutex.h>
#include <kioto/exec/stop_token.h>
#include <kioto/io/io_descriptions.h>
#include <kioto/io/io_sender.h>

namespace kioto {

    template<typename Executor, typename Base = detail::executor_scheduler<Executor>>
    class epoll_scheduler;

    namespace detail {
        template<typename Sexpr>
        class epoll_state_base_for;

        class driver_interrupter {
        public:
            driver_interrupter();
            driver_interrupter(const driver_interrupter&) = delete;
            ~driver_interrupter();
            auto operator= (const driver_interrupter&) -> driver_interrupter& = delete;
            auto interrupt() -> void;
            auto reset() -> bool;
            [[nodiscard]] KIOTO_ALWAYS_INLINE auto watcher() const noexcept -> int { return reader_; }
        private:
            int reader_;
            int writer_;
        };

        // fstat/fcntl(O_NONBLOCK) validation for an io_handle fd (throws). Defined in the .cpp so the
        // syscall detail stays out of the template io_handle.
        auto epoll_prepare_fd(int fd) -> void;

        // timerfd-backed timers for epoll's schedule_after — A1 for epoll: a timer is a timerfd readiness
        // op. Defined in the .cpp (timerfd_* syscalls stay out of the template scheduler).
        auto epoll_arm_timerfd(std::chrono::steady_clock::time_point deadline) noexcept -> int; // -> fd or -1
        auto epoll_drain_timerfd(int fd) noexcept -> void;
        auto epoll_close_timer(int epoll_fd, int fd) noexcept -> void; // EPOLL_CTL_DEL + close
    }

    // The epoll driver: a readiness reactor. Unlike uring (completion), an op registers fd interest and
    // performs the syscall when the fd signals.
    class epoll_driver {
        template<typename Sexpr>
        friend class detail::epoll_state_base_for;

    public:
        // io via epoll readiness; timer via a per-op timerfd (epoll_scheduler::sleep_sender).
        using capabilities = type_list<capability::io, capability::timer>;
        // Scheduler fragment: io_handle + schedule_io + timerfd-backed timers, on Base.
        template<typename Executor, typename Base>
        using scheduler_mixin = epoll_scheduler<Executor, Base>;

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

        // A non-owning borrow (fd + interest bookkeeping) of the io_handle one in-flight op targets.
        struct io_ref {
            int fd = -1;
            per_fd_data* data = nullptr;
        };

        // Ops this driver implements; schedule_io static_asserts against it. No positioned io (*_at — epoll
        // readiness can't express it) and no async_sleep_t (epoll timers are timerfd sleep_senders).
        using supported_io_ops = type_list<
            detail::async_read_some_t, detail::async_write_some_t,
            detail::async_stream_send_t, detail::async_datagram_send_t,
            detail::async_receive_t, detail::async_stream_receive_t, detail::async_receive_from_t, detail::async_send_to_t,
            detail::async_accept_t, detail::async_connect_t>;
        template<typename IoOp>
        static constexpr bool supports = supported_io_ops::template contains<IoOp>;

        // The per-op state base the generic detail::io_sender derives from.
        template<typename IoOp>
        using io_state = detail::epoll_state_base_for<IoOp>;

        epoll_driver();
        epoll_driver(const epoll_driver&) = delete;
        ~epoll_driver();
        auto operator= (const epoll_driver&) -> epoll_driver& = delete;

        // executor-facing contract
        auto poll(detail::ready_queue& ready, std::size_t batch) -> void;
        auto poll_wait() -> void;
        auto wake_up() noexcept -> void { interrupter_.interrupt(); }

        // op-facing. Posting deregistered ops back to the run queue is the CALLER's job. deregister reads
        // op->registered_event_ UNDER the fd lock — reading it unlocked races register_event (TSan hazard).
        auto deregister(operation* op) -> bool;                           // drop op's registration; true if it was registered
        auto cancel_all(per_fd_data* data) -> std::array<operation*, 2>;  // io_handle teardown: return the deregistered ops
        auto release_fd(int fd, per_fd_data* data) -> void;               // io_handle release: EPOLL_CTL_DEL
        [[nodiscard]] auto epoll_fd() const noexcept -> int { return epoll_fd_; }

    private:
        auto process_ready(detail::ready_queue& ready) -> void;

        int epoll_fd_;
        detail::driver_interrupter interrupter_;
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
            epoll_state_base_for(epoll_driver& driver, epoll_driver::io_ref ref, Sexpr sexpr) noexcept :
                base1(std::move(sexpr)), epoll_driver::operation(driver, ref.fd, ref.data) {}

        protected:
            auto do_start() noexcept -> bool { static_assert(always_false<Sexpr>, "this operation isn't supported"); unreachable(); }
            auto do_perform() noexcept -> bool { static_assert(always_false<Sexpr>, "this operation isn't supported"); unreachable(); }
            auto perform() noexcept -> bool override { return do_perform(); }

            // try_cancel: deregister; true iff still registered -> caller posts us back to finish stopped.
            // Runs on the cancelling thread with no unlocked reads (deregister checks under the fd lock).
            auto try_cancel() -> bool {
                return this->driver_.deregister(this);
            }
            static auto on_finish() noexcept -> void {}

        protected:
            async_result<typename Sexpr::value_signature, execution::set_error_t(std::error_code)> result;
        };

        template<> auto epoll_state_base_for<async_read_some_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_read_some_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_write_some_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_write_some_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_stream_send_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_stream_send_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_datagram_send_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_datagram_send_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_stream_receive_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_stream_receive_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_from_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_receive_from_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_send_to_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_send_to_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_accept_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_accept_t>::do_perform() noexcept -> bool;
        template<> auto epoll_state_base_for<async_connect_t>::do_start() noexcept -> bool;
        template<> auto epoll_state_base_for<async_connect_t>::do_perform() noexcept -> bool;
    }

    // epoll's buffer_pool analog: one reused buffer the loop fallback recvs into (no kernel buf_ring). Ctor
    // matches uring's buffer_ring signature so buffer_pool{ctx, n, size, bgid} constructs uniformly; count &
    // bgid are ignored (single buffer — the loop does one datagram at a time, same synchronous backpressure).
    class epoll_buffer_pool {
    public:
        epoll_buffer_pool(executor<epoll_driver>& /*ctx*/, unsigned /*count*/, unsigned buffer_size, int /*bgid*/)
            : storage_(buffer_size) {}
        [[nodiscard]] auto buffer() noexcept -> std::span<std::byte> { return storage_; }
    private:
        std::vector<std::byte> storage_;
    };

    // Scheduler mixin: adds epoll's io + timer senders on top of Base; default Base keeps
    // `epoll_scheduler<Ex>` == the composed type.
    template<typename Executor, typename Base>
    class epoll_scheduler : public Base {
    public:
        using scheduler_concept = detail::io_scheduler_tag;
        // epoll always holds a real fd, so the handle codec is identity forever.
        using native_handle_type = detail::native_handle;
        using buffer_pool = epoll_buffer_pool;   // loop-fallback buffer (epoll has no multishot)
        using Base::Base;

        // Held by socket/file as their impl_. PRECONDITION: the owning facade outlives every op issued on it
        // (not an extra rule — a suspended op's coroutine already holds the facade by reference, so destroying
        // it with I/O in flight is a use-after-free of the facade). Teardown cancel() is thus best-effort for
        // ORDERLY shutdown (stop, drain, THEN destroy) — not a license to destroy with a foreign request_stop
        // in flight, which would race the free of data_ against a not-yet-run finish().
        class io_handle {
            friend epoll_scheduler;
            io_handle(std::nullptr_t, Executor& ctx, int fd)
                : ctx_(&ctx), fd_(fd),
                  data_(ctx.get_allocator().template new_object<epoll_driver::per_fd_data>()) {} // executor's mr
        public:
            io_handle(Executor& ctx, int fd) : io_handle(nullptr, ctx, fd) { detail::epoll_prepare_fd(fd); }
            io_handle(const io_handle&) = delete;
            io_handle(io_handle&& other) noexcept
                : ctx_(other.ctx_), fd_(std::exchange(other.fd_, -1)), data_(std::exchange(other.data_, {})) {}
            ~io_handle() { if (data_ != nullptr) reclaim(cancel()); }

            auto operator= (io_handle other) noexcept -> io_handle& { swap(other); return *this; }
            auto swap(io_handle& o) noexcept -> void {
                std::ranges::swap(ctx_, o.ctx_); std::ranges::swap(fd_, o.fd_); std::ranges::swap(data_, o.data_);
            }
            friend auto swap(io_handle& a, io_handle& b) noexcept -> void { a.swap(b); }

            [[nodiscard]] auto get_io_scheduler() const noexcept -> epoll_scheduler { return epoll_scheduler{*ctx_}; }
            // Public accessors speak the opaque handle; fd_ stays a driver-internal detail (epoll ops read it).
            [[nodiscard]] auto native_handle() const noexcept -> detail::native_handle { return detail::to_handle(fd_); }

            auto release() -> detail::native_handle {
                if (fd_ == -1) return {};
                reclaim(cancel());
                return detail::to_handle(std::exchange(fd_, -1));
            }
            // Teardown deregisters any in-flight ops and posts them (as stopped) via our own executor.
            // Returns whether any live op was pulled (its stop callback is still armed until finish() runs).
            auto cancel() -> bool {
                if (fd_ == -1) return false;
                bool had_ops = false;
                for (auto* op : ctx_->template get_driver<capability::io>().cancel_all(data_)) {
                    if (op != nullptr) { ctx_->submit(*op); had_ops = true; }
                }
                return had_ops;
            }

        private:
            // The op-facing borrow of this registration (see epoll_driver::io_ref).
            [[nodiscard]] auto ref() const noexcept -> epoll_driver::io_ref { return {fd_, data_}; }

            // Deregister the fd + reclaim per_fd_data. The free MUST defer to the OWNER (unless we're already
            // the owner and cancelled nothing) because two readers can still touch data_: (a) a just-cancelled
            // op whose stop callback is still armed derefs it via do_cancel() until its finish() runs; (b) the
            // owner may hold buffered epoll events (peer-close EPOLLHUP) whose data.ptr is this data_. Owner-
            // deferral orders the free after both (release_fd's EPOLL_CTL_DEL stops NEW events first).
            auto reclaim(bool had_ops) -> void {
                if (fd_ != -1) ctx_->template get_driver<capability::io>().release_fd(fd_, data_);
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

        [[nodiscard]] auto make_io_handle(detail::native_handle handle) const -> io_handle { return io_handle{*this->ctx_, detail::to_native(handle)}; }

        // Every io op goes through the generic detail::io_sender; this driver supplies io_ref + io_state<IoOp>.
        template<typename Sexpr>
        [[nodiscard]] auto schedule_io(io_handle& obj, Sexpr sexpr) const noexcept {
            return detail::schedule_io(*this->ctx_, obj.ref(), std::move(sexpr));
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
                    : epoll_driver::operation(ctx.template get_driver<capability::io>(), -1, &own_data_),
                      context_(ctx), when_(when), rcvr_(std::move(rcvr)) {}
                ~state_base() { if (this->fd != -1) detail::epoll_close_timer(driver_.epoll_fd(), this->fd); }

                KIOTO_ALWAYS_INLINE auto do_start() noexcept -> bool {
                    const int tfd = detail::epoll_arm_timerfd(when_);
                    if (tfd == -1) { result_.set_error(std::error_code{errno, std::system_category()}); return false; }
                    this->fd = tfd;
                    if (not register_event(EPOLLIN, 0)) { result_.set_error(std::error_code{errno, std::system_category()}); return false; }
                    return true;
                }
                KIOTO_ALWAYS_INLINE auto do_finish(bool canceled) noexcept -> void {
                    if (canceled) execution::set_stopped(std::move(rcvr_));
                    else result_.forward_to(std::move(rcvr_));
                }
                auto do_cancel() -> void { if (driver_.deregister(this)) context_.submit(*this); }
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
