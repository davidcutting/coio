#pragma once
#include <concepts>
#include <system_error>
#include <utility>
#include <kioto/io/execution_context.h>
#include <kioto/base/concepts.h>
#include <kioto/io/io_descriptions.h>
#include <kioto/exec/stop_token.h>

namespace kioto::detail {
    template<typename Executor>
    using io_driver_of_t = std::remove_reference_t<decltype(std::declval<Executor&>().template get_driver<capability::io>())>;

    // The contract a driver's io_handle owes the facades (their impl_): move-only registration ownership,
    // opaque-handle accessors, best-effort cancel(). Facades static_assert make_io_handle against it.
    // NOT in the contract: ref() (a private op-borrow between handle and its scheduler), and the teardown
    // DISCIPLINE — each driver's is dictated by its kernel primitive (epoll defers the per_fd_data free to
    // the owner; uring routes cancel to the ring gated on inflight; iocp CancelIoEx + drains the port) and
    // does not generalize; unifying was considered and rejected. See each io_handle's own argument.
    template<typename Handle>
    concept io_driver_handle =
        std::movable<Handle> and not std::copyable<Handle> and
        requires (Handle& handle, const Handle& chandle) {
            { chandle.native_handle() } noexcept -> std::same_as<native_handle>;
            { chandle.get_io_scheduler() } noexcept -> execution::scheduler;
            { handle.release() } -> std::same_as<native_handle>;
            handle.cancel();
        };

    // The one io sender shape shared by every driver. Per capability::io the driver contributes:
    //   io_ref          — opaque non-owning borrow of the scheduler's io_handle targeting a registration.
    //   io_state<IoOp>  — per-op state base (driver&, io_ref, IoOp), providing: try_cancel() (detach; true
    //                     if the caller should post it back to finish stopped — uring cancels via its own
    //                     completion flow and returns false), on_finish() (pre-delivery bookkeeping), and
    //                     result (async_result<IoOp::value_signature, set_error_t(error_code)>).
    //   supports<IoOp>  — per-driver op support; schedule_io static_asserts against it.
    // Everything else (completion sigs, stop plumbing, connect/get_env) is universal and lives here.
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
            static constexpr bool kioto_unstoppable = is_unstoppable<IoOp>;

            state_base(Executor& ctx, io_ref ref, IoOp op, Rcvr rcvr) noexcept
                : driver_type::template io_state<IoOp>(ctx.template get_driver<capability::io>(), ref, std::move(op)),
                  context_(ctx), rcvr_(std::move(rcvr)) {}

            KIOTO_ALWAYS_INLINE auto do_finish(bool) noexcept -> void {
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
        KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && {
            KIOTO_ASSERT(context != nullptr);
            return state<Rcvr>{*std::exchange(context, nullptr), std::exchange(ref, {}), std::move(op), std::move(rcvr)};
        }
        template<similar_to<io_sender>, typename...>
        static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
        KIOTO_ALWAYS_INLINE auto get_env() const noexcept -> exec_env<Executor> { return {*context}; }

        Executor* context;
        io_ref ref;
        IoOp op;
    };

    // Driver schedule_io delegates here with the io_handle's ref. Unstoppable descriptions skip the
    // stop_when wrap (they complete promptly; run() drains them on its own — see the driver notes).
    template<typename Executor, typename IoOp>
    [[nodiscard]] auto schedule_io(Executor& ctx, typename io_driver_of_t<Executor>::io_ref ref, IoOp op) noexcept {
        static_assert(io_driver_of_t<Executor>::template supports<IoOp>,
                      "this executor's io driver does not support this operation");
        io_sender<Executor, IoOp> sender{&ctx, ref, std::move(op)};
        if constexpr (is_unstoppable<IoOp>) return sender;
        else return stop_when(std::move(sender), ctx.get_stop_token());
    }
}
