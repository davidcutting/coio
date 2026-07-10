// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <kioto/base/config.h>
#if not KIOTO_HAS_IO_URING
#error "uh, where is <liburing.h>?"
#endif
#include <liburing.h>
#include <netinet/in.h>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>
#include <kioto/io/execution_context.h>
#include <kioto/exec/async_result.h>
#include <kioto/exec/stop_token.h>
#include <kioto/io/io_descriptions.h>
#include <kioto/io/io_sender.h>

namespace kioto {
    template<typename Executor, typename Base = detail::executor_scheduler<Executor>>
    class uring_scheduler;

    // Defined below uring_scheduler (its ctor needs the uring_context alias); fwd-declared for use by ref here.
    class buffer_ring;

    namespace detail {
        template<typename Sexpr>
        class uring_state_base_for;
    }

    // The io_uring driver: owns the ring, submits SQEs, reaps CQEs, blocks/wakes the executor. Single-owner
    // + owner-thread-teardown; the only cross-thread entry points are wake_up() and request_cancel().
    class uring_driver {
        template<typename Sexpr>
        friend class detail::uring_state_base_for;

    public:
        // io via ring SQEs; regular files via the same ring (unlike epoll); timer via the native timeout op.
        using capabilities = type_list<capability::io, capability::file, capability::timer>;
        // Scheduler fragment: io_handle + schedule_io + multishot receive + TIMEOUT-backed timers, on Base.
        template<typename Executor, typename Base>
        using scheduler_mixin = uring_scheduler<Executor, Base>;

        // Cancellation state + hooks the driver dispatches through.
        struct operation : detail::operation_base {
            // The ONLY cross-thread transition is active -> cancel_queued (a foreign stop); the rest are
            // owner-only, so a single CAS from `active` is the whole synchronisation.
            enum cancel_state : std::uint8_t { active, cancel_queued, drained, completed };

            explicit operation(uring_driver& driver) noexcept : driver_(driver) {}

            auto do_cancel() -> void;         // stop-token callback (any thread)
            auto submit_cancel() -> void;     // OWNER thread only (touches the ring)

            virtual auto prepare(::io_uring_sqe* sqe) noexcept -> void = 0;   // Sexpr-specialized (in the .cpp)

            // Completion hook, called per CQE from classify(). Returns true when FINISHED (-> ready queue +
            // free), false when still armed for more CQEs (multishot). `cqe_flags` (F_MORE/F_BUFFER + bid)
            // is used only by multishot; single-shot ignores it and always returns true.
            virtual auto on_completion(int cqe_res, unsigned cqe_flags) -> bool = 0;

            uring_driver& driver_; // NOLINT(*-avoid-const-or-ref-data-members)
            std::atomic<std::uint8_t> cancel_state_{active};
            operation* cancel_link_ = nullptr;   // intrusive link for cancel_stack_
            bool completion_ready_ = false;       // owner-only: CQE arrived while a cancel was queued
        };

        // A non-owning borrow of the io_handle one in-flight op targets. Default-constructed = fd-less op
        // (timer): no handle, no inflight tracking.
        struct io_ref {
            int fd = -1;
            std::atomic<int>* inflight = nullptr;   // &io_handle::inflight_, or null for fd-less ops
        };

        // The ops this driver implements (= the uring_state_base_for prepare() specializations below/.cpp),
        // in one visible list; detail::schedule_io static_asserts against it.
        using supported_io_ops = type_list<
            detail::async_read_some_t, detail::async_write_some_t,
            detail::async_read_some_at_t, detail::async_write_some_at_t,
            detail::async_stream_send_t, detail::async_datagram_send_t,
            detail::async_receive_t, detail::async_stream_receive_t, detail::async_receive_from_t, detail::async_send_to_t,
            detail::async_accept_t, detail::async_connect_t, detail::async_sleep_t>;
        template<typename IoOp>
        static constexpr bool supports = supported_io_ops::template contains<IoOp>;

        // The per-op state base the generic detail::io_sender derives from.
        template<typename IoOp>
        using io_state = detail::uring_state_base_for<IoOp>;

        // SQ depth (CQ is 2x); ~105 B pinned (RLIMIT_MEMLOCK) per entry per ring, one ring per worker.
        static constexpr std::size_t default_entries = 1024;

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
        auto cancel_fd(int fd) -> void;        // OWNER only: cancel all ops on fd (io_handle teardown)

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
        auto classify(detail::ready_queue& ready, void* user_data, int res, unsigned flags) -> void;

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

        // The Sexpr-typed op body: do_start() hands the op to the driver, which fills the SQE via the
        // Sexpr-specialized prepare() (bodies in the .cpp).
        template<typename Sexpr>
        class uring_state_base_for : private uring_sexpr_wrapper<Sexpr>::type, public uring_driver::operation {
            using base1 = typename uring_sexpr_wrapper<Sexpr>::type;

        public:
            uring_state_base_for(uring_driver& driver, uring_driver::io_ref ref, Sexpr sexpr) noexcept :
                base1(std::move(sexpr)), uring_driver::operation(driver), fd(ref.fd), inflight_(ref.inflight) {}

        protected:
            auto prepare(::io_uring_sqe*) noexcept -> void override {
                static_assert(always_false<Sexpr>, "this operation isn't supported");
            }

            // inflight_ counts live ops on the fd so teardown can skip the ring when idle. Both mutations
            // are owner-only (single writer; teardown is the only cross-thread reader), so a plain relaxed
            // load+store (a mov, not a locked fetch_add) is correct. Timers are fd-less -> inflight_ null.
            auto do_start() noexcept -> bool {
                if (inflight_ != nullptr)
                    inflight_->store(inflight_->load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
                if (not driver_.submit(*this)) {
                    result.set_error(std::make_error_code(std::errc::no_buffer_space));
                    return false;
                }
                return true;
            }

            // Generic single-shot completion: deliver once, always finished. Specialized in the .cpp for
            // ops with a richer result.
            auto on_completion(int cqe_res, unsigned /*cqe_flags*/) -> bool override {
                if (cqe_res < 0) {
                    const std::error_code ec{-cqe_res, std::system_category()};
                    if (ec == std::errc::operation_canceled) result.set_stopped();
                    else result.set_error(ec);
                }
                else {
                    if constexpr (std::same_as<typename Sexpr::value_signature, execution::set_value_t()>) result.set_value();
                    // Fold EOF-on-zero for stream reads. Guard on the flag: only these ops carry a std::span
                    // `buffer` (send_to's generic path has a ::iovec), so we never touch buffer otherwise.
                    else if constexpr (is_eof_on_zero<Sexpr>) {
                        if (cqe_res == 0 and not this->buffer.empty()) [[unlikely]] result.set_error(kioto::error::eof);
                        else result.set_value(cqe_res);
                    }
                    else result.set_value(cqe_res);
                }
                return true;
            }

            // try_cancel: request an async ring cancel (the CQE flow completes it) -> caller never posts us,
            // always false. on_finish: drop the inflight count before delivery (see do_start).
            auto try_cancel() -> bool {
                uring_driver::operation::do_cancel();
                return false;
            }
            auto on_finish() noexcept -> void {
                if (inflight_ != nullptr)
                    inflight_->store(inflight_->load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
            }

        protected:
            int fd;
            std::atomic<int>* inflight_;
            async_result<typename Sexpr::value_signature, execution::set_error_t(std::error_code)> result;
        };

        // prepare() specializations (bodies in uring_context.cpp — the sole home of io_uring_prep_*):
        template<> auto uring_state_base_for<async_read_some_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_write_some_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_read_some_at_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_write_some_at_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_receive_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_stream_receive_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_stream_send_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_datagram_send_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_receive_from_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_receive_from_t>::on_completion(int, unsigned) -> bool;
        template<> auto uring_state_base_for<async_send_to_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_accept_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_accept_t>::on_completion(int, unsigned) -> bool; // wraps the minted fd -> native_handle
        template<> auto uring_state_base_for<async_connect_t>::prepare(::io_uring_sqe*) noexcept -> void;
        // A1 timer: TIMEOUT SQE + -ETIME-as-success completion.
        template<> auto uring_state_base_for<async_sleep_t>::prepare(::io_uring_sqe*) noexcept -> void;
        template<> auto uring_state_base_for<async_sleep_t>::on_completion(int, unsigned) -> bool;
    }

    // Caller-managed provided-buffer ring for multishot receive (io_uring bufring). The kernel pulls one
    // buffer per datagram; the CQE reports which via its buffer id (bid). A delivered span is valid only
    // until recycle(bid). Owns a kernel registration + storage -> non-movable, owner-thread-only, and must
    // outlive every multishot op naming its bgid. `count` must be a power of two; `buffer_size` is the cap.
    class buffer_ring {
    public:
        buffer_ring(executor<uring_driver>& ctx, unsigned count, unsigned buffer_size, int bgid)
            : ring_(ctx.get_driver<capability::io>().get_uring()),
              storage_(std::make_unique<std::byte[]>(static_cast<std::size_t>(count) * buffer_size)),
              count_(count), buffer_size_(buffer_size),
              mask_(::io_uring_buf_ring_mask(count)), bgid_(bgid) {
            KIOTO_ASSERT(std::has_single_bit(count));
            int err = 0;
            br_ = ::io_uring_setup_buf_ring(ring_, count, bgid, 0, &err);
            if (br_ == nullptr) throw std::system_error{-err, std::system_category(), "io_uring_setup_buf_ring"};
            for (unsigned i = 0; i < count; ++i) {
                ::io_uring_buf_ring_add(br_, addr(i), buffer_size_, static_cast<unsigned short>(i), mask_, static_cast<int>(i));
            }
            ::io_uring_buf_ring_advance(br_, static_cast<int>(count));
        }

        buffer_ring(const buffer_ring&) = delete;
        auto operator= (const buffer_ring&) -> buffer_ring& = delete;
        ~buffer_ring() {
            if (br_ != nullptr) ::io_uring_free_buf_ring(ring_, br_, count_, bgid_);
        }

        [[nodiscard]] auto bgid() const noexcept -> int { return bgid_; }
        [[nodiscard]] auto buffer(unsigned bid) noexcept -> std::span<std::byte> { return {addr(bid), buffer_size_}; }

        // Return a consumed buffer to the kernel for reuse (once its datagram is processed). Owner-thread only.
        auto recycle(unsigned bid) noexcept -> void {
            ::io_uring_buf_ring_add(br_, addr(bid), buffer_size_, static_cast<unsigned short>(bid), mask_, 0);
            ::io_uring_buf_ring_advance(br_, 1);
        }

    private:
        [[nodiscard]] auto addr(unsigned i) noexcept -> std::byte* {
            return storage_.get() + static_cast<std::size_t>(i) * buffer_size_;
        }

        ::io_uring* ring_;
        ::io_uring_buf_ring* br_ = nullptr;
        std::unique_ptr<std::byte[]> storage_;
        unsigned count_;
        unsigned buffer_size_;
        int mask_;
        int bgid_;
    };

    // Scheduler mixin: adds uring's io ops + timers on top of Base. Lives here (with liburing) to keep
    // execution_context.h driver-clean; default Base keeps `uring_scheduler<Ex>` == the composed type.
    template<typename Executor, typename Base>
    class uring_scheduler : public Base {
    public:
        using scheduler_concept = detail::io_scheduler_tag;
        // Opaque handle: identity codec today (bits == fd), a registered fixed-file index later, no facade change.
        using native_handle_type = detail::native_handle;
        // Named here (not as concrete buffer_ring) so the generic facade can take IoScheduler::buffer_pool&
        // without depending on a uring type; drivers without multishot simply don't define it.
        using buffer_pool = buffer_ring;
        using Base::Base;

        // Held by socket/file as their impl_. PRECONDITION: the owning facade (hence this io_handle) outlives
        // and isn't moved while any op issued on it is in flight — else it's a use-after-free of the facade
        // itself. inflight_ counts live ops so teardown distinguishes an idle handle (droppable from any
        // thread) from one with outstanding I/O (whose cancel must reach the single-issuer owner).
        class io_handle {
            friend uring_scheduler;
        public:
            io_handle(Executor& ctx, int fd) noexcept : ctx_(&ctx), fd_(fd) {}
            io_handle(const io_handle&) = delete;
            // move/swap valid only with no ops in flight (precondition), so inflight_ is 0 on both sides.
            io_handle(io_handle&& other) noexcept : ctx_(other.ctx_), fd_(std::exchange(other.fd_, -1)) {}
            ~io_handle() { cancel(); }

            auto operator= (io_handle other) noexcept -> io_handle& { swap(other); return *this; }
            auto swap(io_handle& other) noexcept -> void { std::ranges::swap(ctx_, other.ctx_); std::ranges::swap(fd_, other.fd_); }
            friend auto swap(io_handle& a, io_handle& b) noexcept -> void { a.swap(b); }

            [[nodiscard]] auto get_io_scheduler() const noexcept -> uring_scheduler {
                KIOTO_ASSERT(ctx_ != nullptr);
                return uring_scheduler{*ctx_};
            }
            // Public accessors speak the opaque handle; the raw fd_ stays a driver-internal detail that
            // the io_sender reads directly (it holds `int fd`) and prepare() feeds to the ring.
            [[nodiscard]] auto native_handle() const noexcept -> detail::native_handle { return detail::to_handle(fd_); }

            auto release() -> detail::native_handle { cancel(); return detail::to_handle(std::exchange(fd_, -1)); }
            auto cancel() -> void {
                if (fd_ == -1) return;
                auto& driver = ctx_->template get_driver<capability::io>();
                // Reaping in-flight ops submits a cancel SQE -> owner only (the single issuer).
                if (ctx_->is_owner()) {
                    driver.cancel_fd(fd_);
                }
                else if (inflight_.load(std::memory_order_acquire) != 0) {
                    // Off-owner with ops in flight (discouraged): mail the ring-touching cancel home. Best-
                    // effort; if the owner already stopped, the driver destructor reaps the ring.
                    detail::defer_to_owner(*ctx_, [&driver, fd = fd_] { driver.cancel_fd(fd); });
                }
                // else idle + off-owner: nothing in the ring for this fd -> torn down from any thread.
            }

        private:
            // The op-facing borrow of this registration (see uring_driver::io_ref).
            [[nodiscard]] auto ref() noexcept -> uring_driver::io_ref { return {fd_, &inflight_}; }

            Executor* ctx_;
            int fd_ = -1;
            std::atomic<int> inflight_{0};   // live ops on this fd; owner-mutated, read cross-thread at teardown
        };

        [[nodiscard]] auto make_io_handle(detail::native_handle handle) const -> io_handle {
            return io_handle{*this->ctx_, detail::to_native(handle)}; // decode at the boundary; internals hold the fd
        }

        // Every io op goes through the generic detail::io_sender (unstoppable descriptions skip the per-op
        // stop-hook there; see io_descriptions.h). This driver just supplies io_ref + io_state<IoOp>.
        template<typename Sexpr>
        [[nodiscard]] auto schedule_io(io_handle& obj, Sexpr sexpr) const noexcept {
            return detail::schedule_io(*this->ctx_, obj.ref(), std::move(sexpr));
        }

        // Multishot receive: one armed SQE delivers many datagrams, each into a buffer from `bufs`. `sink`
        // runs per datagram on the owner thread with the bytes (valid only for the call — recycled right
        // after). Completes when the multishot ends (stopped/error/value). Connected-socket only (plain RECV).
        template<typename Sink>
        struct multishot_recv_sender {
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                execution::set_value_t(), execution::set_error_t(std::error_code), execution::set_stopped_t()>;

            template<typename Rcvr>
            struct state_base : uring_driver::operation {
                state_base(Executor& ctx, int fd, std::atomic<int>* inflight, buffer_ring* bufs, Sink sink, Rcvr rcvr) noexcept
                    : uring_driver::operation(ctx.template get_driver<capability::io>()),
                      context_(ctx), fd_(fd), inflight_(inflight), bufs_(bufs), sink_(std::move(sink)),
                      rcvr_(std::move(rcvr)) {}

                auto prepare(::io_uring_sqe* sqe) noexcept -> void override {
                    ::io_uring_prep_recv_multishot(sqe, fd_, nullptr, 0, 0);
                    sqe->buf_group = static_cast<unsigned short>(bufs_->bgid());
                    sqe->flags |= IOSQE_BUFFER_SELECT;
                }

                auto on_completion(int res, unsigned flags) -> bool override {
                    if (res >= 0) {
                        if (flags & IORING_CQE_F_BUFFER) {
                            const unsigned bid = flags >> IORING_CQE_BUFFER_SHIFT;
                            sink_(bufs_->buffer(bid).first(static_cast<std::size_t>(res)));
                            bufs_->recycle(bid);
                        }
                        if (flags & IORING_CQE_F_MORE) return false;   // still armed for more datagrams
                        return not rearm();                            // benign end -> re-arm, else finish (error)
                    }
                    const std::error_code ec{-res, std::system_category()};
                    if (ec == std::errc::operation_canceled) { result_.set_stopped(); return true; }
                    if (ec == std::errc::no_buffer_space and rearm()) return false; // ring drained -> re-arm
                    result_.set_error(ec);
                    return true;
                }

                // Re-arm the multishot. On SQE exhaustion, record the error for the terminal finish.
                auto rearm() noexcept -> bool {
                    if (driver_.submit(*this)) return true;
                    result_.set_error(std::make_error_code(std::errc::no_buffer_space));
                    return false;
                }
                // inflight_ marks this fd as busy for its whole armed lifetime (bumped once on start,
                // dropped once on terminal finish — NOT per re-arm), so io_handle teardown coordinates
                // with it exactly like a single-shot op. Owner-only, single-writer (see io_sender).
                auto do_start() noexcept -> bool {
                    if (inflight_ != nullptr)
                        inflight_->store(inflight_->load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
                    return rearm();
                }
                auto do_finish(bool) noexcept -> void {
                    if (inflight_ != nullptr)
                        inflight_->store(inflight_->load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
                    result_.forward_to(std::move(rcvr_));
                }

                Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members)
                int fd_;
                std::atomic<int>* inflight_;
                buffer_ring* bufs_;
                Sink sink_;
                Rcvr rcvr_;
                async_result<execution::set_value_t(), execution::set_error_t(std::error_code)> result_;
            };
            template<typename Rcvr>
            using state = detail::operation_state<state_base<Rcvr>>;

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && -> state<Rcvr> {
                KIOTO_ASSERT(context != nullptr);
                return state<Rcvr>{*std::exchange(context, nullptr), fd, inflight, bufs, std::move(sink), std::move(rcvr)};
            }
            template<similar_to<multishot_recv_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
            KIOTO_ALWAYS_INLINE auto get_env() const noexcept -> detail::exec_env<Executor> { return {*context}; }

            Executor* context;
            int fd;
            std::atomic<int>* inflight;
            buffer_ring* bufs;
            Sink sink;
        };

        // Arm a multishot receive on `obj`'s fd, buffers from `bufs`. Tied to the io_handle (inflight +
        // cancel_fd teardown) like any op. The multishot lowering behind async_receive_sequence.
        template<typename Sink>
        [[nodiscard]] auto receive_multishot(io_handle& obj, buffer_ring& bufs, Sink sink) const noexcept {
            return multishot_recv_sender<Sink>{this->ctx_, obj.fd_, &obj.inflight_, &bufs, std::move(sink)};
        }

        // Multishot accept: one armed SQE delivers many connections. `sink` runs per connection on the owner
        // thread with the minted fd wrapped as a native_handle (no raw fd escapes). Completes on the multishot
        // end (stopped/error). io_uring fast path behind async_accept_sequence (loop is the fallback).
        template<typename Sink>
        struct multishot_accept_sender {
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                execution::set_value_t(), execution::set_error_t(std::error_code), execution::set_stopped_t()>;

            template<typename Rcvr>
            struct state_base : uring_driver::operation {
                state_base(Executor& ctx, int fd, std::atomic<int>* inflight, Sink sink, Rcvr rcvr) noexcept
                    : uring_driver::operation(ctx.template get_driver<capability::io>()),
                      context_(ctx), fd_(fd), inflight_(inflight), sink_(std::move(sink)), rcvr_(std::move(rcvr)) {}

                auto prepare(::io_uring_sqe* sqe) noexcept -> void override {
                    ::io_uring_prep_multishot_accept(sqe, fd_, nullptr, nullptr, 0);
                }

                auto on_completion(int res, unsigned flags) -> bool override {
                    if (res >= 0) {
                        sink_(detail::to_handle(res));                 // wrap the minted fd at the boundary
                        if (flags & IORING_CQE_F_MORE) return false;   // still armed for more connections
                        return not rearm();                            // armed dropped -> re-arm, else terminal
                    }
                    const std::error_code ec{-res, std::system_category()};
                    if (ec == std::errc::operation_canceled) { result_.set_stopped(); return true; }
                    result_.set_error(ec);
                    return true;
                }

                auto rearm() noexcept -> bool {
                    if (driver_.submit(*this)) return true;
                    result_.set_error(std::make_error_code(std::errc::no_buffer_space));
                    return false;
                }
                auto do_start() noexcept -> bool {
                    if (inflight_ != nullptr)
                        inflight_->store(inflight_->load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
                    return rearm();
                }
                auto do_finish(bool) noexcept -> void {
                    if (inflight_ != nullptr)
                        inflight_->store(inflight_->load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
                    result_.forward_to(std::move(rcvr_));
                }

                Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members)
                int fd_;
                std::atomic<int>* inflight_;
                Sink sink_;
                Rcvr rcvr_;
                async_result<execution::set_value_t(), execution::set_error_t(std::error_code)> result_;
            };
            template<typename Rcvr>
            using state = detail::operation_state<state_base<Rcvr>>;

            template<execution::receiver Rcvr>
            KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && -> state<Rcvr> {
                KIOTO_ASSERT(context != nullptr);
                return state<Rcvr>{*std::exchange(context, nullptr), fd, inflight, std::move(sink), std::move(rcvr)};
            }
            template<similar_to<multishot_accept_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
            KIOTO_ALWAYS_INLINE auto get_env() const noexcept -> detail::exec_env<Executor> { return {*context}; }

            Executor* context;
            int fd;
            std::atomic<int>* inflight;
            Sink sink;
        };

        template<typename Sink>
        [[nodiscard]] auto accept_multishot(io_handle& obj, Sink sink) const noexcept {
            return multishot_accept_sender<Sink>{this->ctx_, obj.fd_, &obj.inflight_, std::move(sink)};
        }

        [[nodiscard]] static auto now() noexcept -> std::chrono::steady_clock::time_point {
            return std::chrono::steady_clock::now();
        }
        template<typename Rep, typename Period>
        [[nodiscard]] auto schedule_after(std::chrono::duration<Rep, Period> d) const noexcept {
            return schedule_at(now() + d);
        }
        [[nodiscard]] auto schedule_at(std::chrono::steady_clock::time_point deadline) const noexcept {
            // A timer is an fd-less io op (default io_ref): a TIMEOUT SQE through the same machinery.
            return detail::schedule_io(*this->ctx_, uring_driver::io_ref{}, detail::async_sleep_t{deadline});
        }
    };

    // The public io context type: a single-owner executor whose one driver is the ring.
    using uring_context = executor<uring_driver>;
}
