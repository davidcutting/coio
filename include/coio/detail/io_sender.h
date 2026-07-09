#pragma once
#include <system_error>
#include <utility>
#include <coio/execution_context.h>
#include <coio/detail/concepts.h>
#include <coio/detail/io_descriptions.h>
#include <coio/utils/stop_token.h>

namespace coio::detail {
    template<typename Executor>
    using io_driver_of_t = std::remove_reference_t<decltype(std::declval<Executor&>().template get_driver<capability::io>())>;

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
