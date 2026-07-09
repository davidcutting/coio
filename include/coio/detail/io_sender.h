#pragma once
#include <concepts>
#include <system_error>
#include <utility>
#include <coio/execution_context.h>
#include <coio/detail/concepts.h>
#include <coio/detail/io_descriptions.h>
#include <coio/utils/stop_token.h>

namespace coio::detail {
    template<typename Executor>
    using io_driver_of_t = std::remove_reference_t<decltype(std::declval<Executor&>().template get_driver<capability::io>())>;

    // The contract a backend's io_handle owes the facades (socket/file/pipe hold one as their impl_):
    // move-only ownership of a registration, the opaque-handle accessors, and best-effort teardown via
    // cancel(). The facades static_assert this against make_io_handle's result, so a new backend gets a
    // checklist instead of archaeology.
    //
    // Deliberately NOT part of the contract:
    //   - ref() — the op-facing borrow is a private detail between the handle and its own scheduler's
    //     schedule_io; nothing generic touches it.
    //   - the teardown DISCIPLINE. Each backend's is dictated by its kernel primitive and does not
    //     generalize: epoll pull-cancels registered ops and defers the shared per_fd_data free to the
    //     owner thread; uring routes the cancel to the single-issuer ring, gated on its inflight count;
    //     iocp fires a thread-safe CancelIoEx and lets aborted packets drain through the port. Unifying
    //     these was considered and rejected once all three existed — the shared skeleton would be the
    //     union of three special cases. See each io_handle for its own correctness argument.
    template<typename Handle>
    concept io_backend_handle =
        std::movable<Handle> and not std::copyable<Handle> and
        requires (Handle& handle, const Handle& chandle) {
            { chandle.native_handle() } noexcept -> std::same_as<native_handle>;
            { chandle.get_io_scheduler() } noexcept -> execution::scheduler;
            { handle.release() } -> std::same_as<native_handle>;
            handle.cancel();
        };

    // The one io sender shape shared by every backend. The driver contributes, per capability::io:
    //   io_ref            — what one in-flight op needs to target a registration (a non-owning borrow of
    //                       the scheduler's io_handle, valid only while it lives). Ferried opaquely here.
    //   io_state<IoOp>    — the per-op state base (kernel-facing do_start/result plumbing), constructed
    //                       as (driver&, io_ref, IoOp) and additionally providing:
    //     try_cancel()    — detach from the driver; true if the op was still registered and the caller
    //                       should post it back (to finish as stopped). A backend whose cancellation
    //                       completes through its own completion flow (uring) requests it and returns false.
    //     on_finish()     — bookkeeping before the result is delivered (uring: inflight decrement).
    //     result          — async_result<IoOp::value_signature, set_error_t(std::error_code)>.
    //   supports<IoOp>    — explicit per-driver op support; schedule_io static_asserts against it.
    // Everything else — completion signatures, stop plumbing, connect/get_env — is universal and lives here.
    template<typename Executor, typename IoOp>
    struct io_sender {
        using driver_type = io_driver_of_t<Executor>;
        using io_ref = typename driver_type::io_ref;

        using sender_concept = execution::sender_tag;
        using completion_signatures = execution::completion_signatures<
            typename IoOp::value_signature, execution::set_error_t(std::error_code), execution::set_stopped_t()>;

        template<typename Rcvr>
        struct state_base : driver_type::template io_state<IoOp> {
            // Unstoppable descriptions (is_unstoppable — datagram send) opt out of the stop-callback in
            // operation_state; the policy lives on the description, not at the call site.
            static constexpr bool coio_unstoppable = is_unstoppable<IoOp>;

            state_base(Executor& ctx, io_ref ref, IoOp op, Rcvr rcvr) noexcept
                : driver_type::template io_state<IoOp>(ctx.template get_driver<capability::io>(), ref, std::move(op)),
                  context_(ctx), rcvr_(std::move(rcvr)) {}

            COIO_ALWAYS_INLINE auto do_finish(bool) noexcept -> void {
                this->on_finish();
                this->result.forward_to(std::move(rcvr_));
            }
            auto do_cancel() -> void {
                if (this->try_cancel()) context_.submit(*this);
            }

            Executor& context_; // NOLINT(*-avoid-const-or-ref-data-members)
            Rcvr rcvr_;
        };
        template<typename Rcvr>
        using state = operation_state<state_base<Rcvr>>;

        template<execution::receiver Rcvr>
        COIO_ALWAYS_INLINE auto connect(Rcvr rcvr) && {
            COIO_ASSERT(context != nullptr);
            return state<Rcvr>{*std::exchange(context, nullptr), std::exchange(ref, {}), std::move(op), std::move(rcvr)};
        }
        template<similar_to<io_sender>, typename...>
        static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
        COIO_ALWAYS_INLINE auto get_env() const noexcept -> exec_env<Executor> { return {*context}; }

        Executor* context;
        io_ref ref;
        IoOp op;
    };

    // Backend schedule_io delegates here with the io_handle's ref. Unstoppable descriptions skip the
    // stop_when wrap (they complete promptly; run() drains them on its own — see the backend notes).
    template<typename Executor, typename IoOp>
    [[nodiscard]] auto schedule_io(Executor& ctx, typename io_driver_of_t<Executor>::io_ref ref, IoOp op) noexcept {
        static_assert(io_driver_of_t<Executor>::template supports<IoOp>,
                      "this executor's io driver does not support this operation");
        io_sender<Executor, IoOp> sender{&ctx, ref, std::move(op)};
        if constexpr (is_unstoppable<IoOp>) return sender;
        else return stop_when(std::move(sender), ctx.get_stop_token());
    }
}
