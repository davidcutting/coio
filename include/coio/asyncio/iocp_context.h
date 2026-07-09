// ReSharper disable CppPolymorphicClassWithNonVirtualPublicDestructor
// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <coio/detail/config.h>
#if not COIO_HAS_IOCP
#error "IOCP is not available"
#endif

#include <basetsd.h>
#include <WinSock2.h>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <memory_resource>
#include <mutex>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>
#include <coio/execution_context.h>
#include <coio/time_loop.h>
#include <coio/utils/async_result.h>
#include <coio/detail/io_descriptions.h>
#include <coio/detail/io_sender.h>
#include <coio/detail/op_queue.h>
#include <coio/detail/suppress_push.h> // IWYU pragma: keep

namespace coio {
    template<typename Executor, typename Base = timer_scheduler<Executor>>
    class iocp_scheduler;

    namespace detail {
        template<typename Sexpr>
        class iocp_state_base_for;

        enum class seek_whence;

        // The completion-packet node: an OVERLAPPED the kernel writes through + a run-queue node. The
        // driver reaps packets from the port, dispatches complete(), then posts the op to the ready queue.
        // The OVERLAPPED must stay alive until its packet is reaped — guaranteed because the op-state
        // lives until finish(), which runs strictly after the reap.
        struct iocp_node : ::OVERLAPPED, operation_base {
            explicit iocp_node(::HANDLE handle) noexcept : ::OVERLAPPED{}, handle(handle) {}

            virtual auto complete(::DWORD bytes_transferred, ::DWORD error) noexcept -> void = 0;

            // Ask the kernel to abort this op. Any thread — CancelIoEx is documented thread-safe, and a
            // lost race with completion returns ERROR_NOT_FOUND, harmlessly. The aborted packet still
            // arrives through the port (ERROR_OPERATION_ABORTED), so cancellation completes through the
            // normal completion flow — the canceller never posts the op itself.
            auto request_cancel() noexcept -> void;   // cpp: ::CancelIoEx(handle, this)

            ::HANDLE handle;
        };

        // Blocking file helpers for io_handle (cpp: the Win32 syscalls stay out of the header templates).
        // `offset` is the io_handle's tracked stream position (an OVERLAPPED handle has no usable file
        // pointer, so stream_file position lives on the handle and read/write advance it).
        auto iocp_initial_file_offset(::HANDLE handle) noexcept -> std::size_t;
        auto iocp_file_resize(::HANDLE handle, std::size_t new_size, std::size_t restore_offset) -> void;
        auto iocp_file_seek(::HANDLE handle, std::size_t offset, seek_whence whence, std::size_t current_offset) -> std::size_t;
        auto iocp_file_read(::HANDLE handle, std::size_t& offset, std::span<std::byte> buffer) -> std::size_t;
        auto iocp_file_write(::HANDLE handle, std::size_t& offset, std::span<const std::byte> buffer) -> std::size_t;
    }

    // The IOCP driver: a completion port. Unlike epoll (readiness) — and like uring — this is a
    // completion model; but unlike uring there is no submission queue: each op issues its own overlapped
    // syscall in do_start(), and the driver only reaps completion packets. Was iocp_context (loop_base)
    // minus the run loop, which now lives in executor.
    class iocp_driver {
    public:
        // io + regular files via overlapped syscalls + the port; timer via the shared deadline heap
        // (timer_scheduler mixin) — iocp has no native timer op, so the heap's earliest deadline bounds
        // the port wait.
        using capabilities = type_list<capability::io, capability::file, capability::timer>;
        // The scheduler fragment this driver contributes (see detail::compose_scheduler): io_handle +
        // schedule_io, layered onto the shared timer mixin (which resolves get_driver<capability::timer>
        // back to this driver's heap).
        template<typename Executor, typename Base>
        using scheduler_mixin = iocp_scheduler<Executor, timer_scheduler<Executor, Base>>;

        // What one in-flight op needs to target a registration: a non-owning borrow of the scheduler's
        // io_handle, valid only while it lives. Ferried opaquely by the generic detail::io_sender.
        struct io_ref {
            ::HANDLE handle = INVALID_HANDLE_VALUE;
        };

        // The ops this driver implements (= the iocp_state_base_for specializations below/.cpp), in one
        // visible list; detail::schedule_io static_asserts against it. No async_sleep_t — timers ride
        // the heap (see scheduler_mixin).
        using supported_io_ops = type_list<
            detail::async_read_some_t, detail::async_write_some_t,
            detail::async_read_some_at_t, detail::async_write_some_at_t,
            detail::async_stream_send_t, detail::async_datagram_send_t,
            detail::async_receive_t, detail::async_receive_from_t, detail::async_send_to_t,
            detail::async_accept_t, detail::async_connect_t>;
        template<typename IoOp>
        static constexpr bool supports = supported_io_ops::template contains<IoOp>;

        // The per-op state base the generic detail::io_sender derives from.
        template<typename IoOp>
        using io_state = detail::iocp_state_base_for<IoOp>;

        explicit iocp_driver(std::pmr::memory_resource& mr = *std::pmr::get_default_resource());
        iocp_driver(const iocp_driver&) = delete;
        ~iocp_driver();
        auto operator= (const iocp_driver&) -> iocp_driver& = delete;

        // ---- executor-facing contract ----
        auto poll(detail::ready_queue& ready, std::size_t batch) -> void;
        auto poll_wait() -> void;      // blocks on the port, bounded by the earliest timer deadline
        auto wake_up() noexcept -> void;

        // ---- io_handle-facing (cpp: the Win32 syscalls stay out of the header templates) ----
        auto register_handle(::HANDLE handle) -> void;                // associate with the port (throws)
        static auto deregister_handle(::HANDLE handle) -> void;       // detach from the port (throws)
        static auto cancel_handle(::HANDLE handle) noexcept -> void;  // CancelIoEx(handle, nullptr): abort every op on it

        // ---- timer capability (driven by the shared timer_scheduler mixin) ----
        auto submit(timer_driver::operation& op) -> void;   // any thread; wakes the port on a new earliest deadline
        auto remove(timer_driver::operation& op) -> bool;

    private:
        static constexpr ::ULONG_PTR wake_completion_key = 1;

        struct packet {
            ::OVERLAPPED* overlapped = nullptr;   // null = wake packet (nothing to dispatch)
            ::DWORD bytes = 0;
            ::DWORD error = 0;
        };
        // One GetQueuedCompletionStatus. False = nothing dequeued (timeout / empty port); true with a
        // null overlapped = a bare wake packet (its only job was to end a wait).
        auto reap(packet& out, ::DWORD timeout_ms) noexcept -> bool;

        ::HANDLE iocp_;
        packet stash_{};        // owner-only: a real packet poll_wait dequeued; the next poll() consumes
        bool has_stash_ = false; // it first (the wait must not swallow completions — GQCS cannot peek).
        std::mutex timer_mtx_;
        detail::timer_queue<
            timer_driver::operation, &timer_driver::operation::deadline, &timer_driver::operation::heap_index,
            std::pmr::polymorphic_allocator<>> timers_;
    };

    namespace detail {
        template<typename Sexpr>
        struct iocp_sexpr_wrapper {
            using type = Sexpr;
        };

        template<>
        struct iocp_sexpr_wrapper<async_receive_from_t> {
            struct type : async_receive_from_t {
                explicit type(async_receive_from_t s) noexcept : async_receive_from_t(std::move(s)) {}

                ::sockaddr_storage peer_storage{};
                int peer_length = sizeof(::sockaddr_storage);
            };
        };

        template<>
        struct iocp_sexpr_wrapper<async_accept_t> {
            struct type : async_accept_t {
                explicit type(async_accept_t s) noexcept : async_accept_t(s) {}

                socket_native_handle_type accepted = invalid_socket_handle;
                std::byte output_buffer[2 * (sizeof(::sockaddr_storage) + 16)]{};
            };
        };

        template<typename Sexpr>
        class iocp_state_base_for :
            private iocp_sexpr_wrapper<Sexpr>::type,
            public iocp_node {
        private:
            using native_type = typename iocp_sexpr_wrapper<Sexpr>::type;

        public:
            // The driver reference is part of the generic io_state contract but unused here: iocp ops
            // issue their own overlapped syscalls; the driver only reaps.
            iocp_state_base_for(iocp_driver& /*driver*/, iocp_driver::io_ref ref, Sexpr sexpr) noexcept
                : native_type(std::move(sexpr)), iocp_node(ref.handle) {}

        protected:
            auto do_start() noexcept -> bool {
                static_assert(always_false<Sexpr>, "this operation isn't supported");
                unreachable();
            }

            auto complete(::DWORD, ::DWORD) noexcept -> void final {
                static_assert(always_false<Sexpr>, "this operation isn't supported");
            }

            // Generic io_sender hooks. try_cancel: fire CancelIoEx and let the aborted packet complete
            // through the port — never post from here (see iocp_node::request_cancel). on_finish: iocp
            // needs no pre-delivery bookkeeping.
            auto try_cancel() noexcept -> bool {
                this->request_cancel();
                return false;
            }
            static auto on_finish() noexcept -> void {}

        protected:
            async_result<typename Sexpr::value_signature, execution::set_error_t(std::error_code)> result;
        };

        /// async_read_some
        template<>
        auto iocp_state_base_for<async_read_some_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_read_some_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_write_some
        template<>
        auto iocp_state_base_for<async_write_some_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_write_some_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_read_some_at
        template<>
        auto iocp_state_base_for<async_read_some_at_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_read_some_at_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_write_some_at
        template<>
        auto iocp_state_base_for<async_write_some_at_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_write_some_at_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_receive
        template<>
        auto iocp_state_base_for<async_receive_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_receive_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_send (stream + datagram share the WSASend impl; separate types so stoppability is deduced)
        template<>
        auto iocp_state_base_for<async_stream_send_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_stream_send_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        template<>
        auto iocp_state_base_for<async_datagram_send_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_datagram_send_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_receive_from
        template<>
        auto iocp_state_base_for<async_receive_from_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_receive_from_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_send_to
        template<>
        auto iocp_state_base_for<async_send_to_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_send_to_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_accept
        template<>
        auto iocp_state_base_for<async_accept_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_accept_t>::complete(::DWORD, ::DWORD) noexcept -> void;

        /// async_connect
        template<>
        auto iocp_state_base_for<async_connect_t>::do_start() noexcept -> bool;

        template<>
        auto iocp_state_base_for<async_connect_t>::complete(::DWORD, ::DWORD) noexcept -> void;
    }

    // A scheduler mixin (not a standalone scheduler): adds iocp's io senders on top of Base — which, via
    // iocp_driver::scheduler_mixin, is the shared timer mixin, so timed scheduling is inherited rather
    // than reimplemented. The default Base keeps the plain `iocp_scheduler<Ex>` spelling equal to
    // iocp_context's composed scheduler type.
    template<typename Executor, typename Base>
    class iocp_scheduler : public Base {
    public:
        using scheduler_concept = detail::io_scheduler_tag;
        // The opaque handle this backend hands out: bits <-> the raw HANDLE/SOCKET (identity codec).
        using native_handle_type = detail::native_handle;
        using Base::Base;

        // Internal, facaded by socket/file (held as their impl_). Registers the handle with the port on
        // construction. Precondition: the owning facade — and hence this io_handle — outlives every
        // operation issued on it (a suspended op's awaiting coroutine already holds the facade by
        // reference). Teardown cancel() is fire-and-forget (CancelIoEx): aborted packets complete through
        // the port, and each op-state owns its OVERLAPPED — there is no shared per-handle state to
        // reclaim (contrast epoll's per_fd_data), so teardown is legal from any thread.
        class io_handle {
            friend iocp_scheduler;
        public:
            io_handle(Executor& ctx, ::HANDLE handle) : ctx_(&ctx), handle_(handle) {
                // NOTE: `handle` must be opened with FILE_FLAG_OVERLAPPED / WSA_FLAG_OVERLAPPED.
                if (handle_ != INVALID_HANDLE_VALUE and handle_ != nullptr) {
                    offset_ = detail::iocp_initial_file_offset(handle_);
                    ctx.template get_driver<capability::io>().register_handle(handle_);
                }
            }

            io_handle(const io_handle&) = delete;

            io_handle(io_handle&& other) noexcept :
                ctx_(other.ctx_),
                handle_(std::exchange(other.handle_, INVALID_HANDLE_VALUE)),
                offset_(std::exchange(other.offset_, 0)) {}

            ~io_handle() { cancel(); }

            auto operator= (io_handle other) noexcept -> io_handle& {
                swap(other);
                return *this;
            }

            auto swap(io_handle& other) noexcept -> void {
                std::ranges::swap(ctx_, other.ctx_);
                std::ranges::swap(handle_, other.handle_);
                std::ranges::swap(offset_, other.offset_);
            }

            friend auto swap(io_handle& lhs, io_handle& rhs) noexcept -> void { lhs.swap(rhs); }

            [[nodiscard]] auto get_io_scheduler() const noexcept -> iocp_scheduler {
                COIO_ASSERT(ctx_ != nullptr);
                return iocp_scheduler{*ctx_};
            }

            // Public accessors speak the opaque handle; the raw HANDLE stays a backend-internal detail.
            [[nodiscard]] auto native_handle() const noexcept -> detail::native_handle {
                return detail::to_handle(std::bit_cast<detail::native_fd>(handle_));
            }

            auto release() -> detail::native_handle {
                if (handle_ != INVALID_HANDLE_VALUE) {
                    cancel();
                    iocp_driver::deregister_handle(handle_);
                }
                offset_ = 0;
                return detail::to_handle(std::bit_cast<detail::native_fd>(std::exchange(handle_, INVALID_HANDLE_VALUE)));
            }

            auto cancel() -> void {
                if (handle_ == INVALID_HANDLE_VALUE) return;
                iocp_driver::cancel_handle(handle_);
            }

            // Blocking file ops against the tracked offset (see detail::iocp_file_*).
            auto file_resize(std::size_t new_size) -> void {
                detail::iocp_file_resize(handle_, new_size, offset_);
            }
            auto file_seek(std::size_t offset, detail::seek_whence whence) -> std::size_t {
                return offset_ = detail::iocp_file_seek(handle_, offset, whence, offset_);
            }
            auto file_read(std::span<std::byte> buffer) -> std::size_t {
                return detail::iocp_file_read(handle_, offset_, buffer);
            }
            auto file_write(std::span<const std::byte> buffer) -> std::size_t {
                return detail::iocp_file_write(handle_, offset_, buffer);
            }

        private:
            // The op-facing borrow of this registration (see iocp_driver::io_ref).
            [[nodiscard]] auto ref() const noexcept -> iocp_driver::io_ref { return {handle_}; }

            Executor* ctx_;
            ::HANDLE handle_ = INVALID_HANDLE_VALUE;
            std::size_t offset_ = 0; // for stream_file (see transform_sexpr)
        };

        [[nodiscard]] auto make_io_handle(detail::native_handle handle) const -> io_handle {
            return io_handle{*this->ctx_, std::bit_cast<::HANDLE>(detail::to_native(handle))}; // decode at the boundary
        }

        // An OVERLAPPED handle has no usable moving file pointer: plain read_some/write_some become
        // positioned ops at the io_handle's tracked offset, which then advances. Identity for all else.
        template<typename Sexpr>
        COIO_ALWAYS_INLINE static auto transform_sexpr(io_handle& obj, Sexpr sexpr) noexcept {
            if constexpr (not std::same_as<Sexpr, detail::async_read_some_t> and not std::same_as<Sexpr, detail::async_write_some_t>) {
                return std::move(sexpr);
            }
            else {
                using result_t = std::conditional_t<
                    std::same_as<Sexpr, detail::async_read_some_t>,
                    detail::async_read_some_at_t,
                    detail::async_write_some_at_t
                >;
                return result_t{
                    .offset = std::exchange(obj.offset_, obj.offset_ + sexpr.buffer.size()),
                    .buffer = sexpr.buffer
                };
            }
        }

        // One generic sender serves every io op (detail::io_sender); this driver contributes io_ref +
        // io_state<IoOp>. Unstoppable descriptions (datagram send) skip the stop_when wrap there;
        // transform_sexpr is identity for sends, so the trait survives onto the transformed type.
        template<typename Sexpr>
        [[nodiscard]] auto schedule_io(io_handle& obj, Sexpr sexpr) const noexcept {
            return detail::schedule_io(*this->ctx_, obj.ref(), transform_sexpr(obj, std::move(sexpr)));
        }
    };

    // The public io context type: a single-owner executor whose one driver is the completion port.
    using iocp_context = executor<iocp_driver>;
}

#include <coio/detail/suppress_pop.h> // IWYU pragma: keep
