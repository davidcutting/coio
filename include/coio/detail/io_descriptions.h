#pragma once
#include <chrono>
#include <concepts>
#include <cstddef>
#include <coio/detail/error.h>
#include <coio/detail/execution.h>
#include <coio/net/basic.h>

namespace coio::detail {
    // An operation description may declare `static constexpr bool unstoppable = true` to opt out of
    // cancellation: the op completes promptly regardless of the peer (a datagram send), so it needs no
    // shutdown stop-hook. schedule_io skips the stop_when wrap and io_sender sets coio_unstoppable from
    // this, so the policy lives on the operation (where it's a semantic fact) not at the call site.
    template<typename Sexpr>
    inline constexpr bool is_unstoppable = [] {
        if constexpr (requires { { Sexpr::unstoppable } -> std::convertible_to<bool>; }) return Sexpr::unstoppable;
        else return false;
    }();

    // A stream read/recv description declares `static constexpr bool eof_on_zero = true` to mean "0 bytes
    // returned on a non-empty buffer is end-of-stream." The backend completion folds this in via
    // deliver_read_result below, so the facade needs no wrapping sender — replacing the old per-read
    // let_value(...) EOF adaptor (one fewer sender + op-state + variant-visit per read). Semantic fact of
    // the op, so (like unstoppable) it lives on the description.
    template<typename Sexpr>
    inline constexpr bool is_eof_on_zero = [] {
        if constexpr (requires { { Sexpr::eof_on_zero } -> std::convertible_to<bool>; }) return Sexpr::eof_on_zero;
        else return false;
    }();

    // Deliver a stream read/recv byte count into `result`, folding the EOF-on-zero convention per the
    // description. Called by every backend at its result.set_value site (uring on_completion, epoll/iocp
    // do_perform), so the EOF semantics stay identical to the old facade let_value across backends, with
    // no extra sender in the hot path. `Result` is the op's async_result (deduced to avoid the include).
    template<typename IoOp, typename Result>
    COIO_ALWAYS_INLINE auto deliver_read_result(Result& result, std::size_t bytes, bool buffer_empty) noexcept -> void {
        if constexpr (is_eof_on_zero<IoOp>) {
            if (bytes == 0 and not buffer_empty) [[unlikely]] { result.set_error(coio::error::eof); return; }
        }
        result.set_value(bytes);
    }

    struct async_read_some_t {
        using value_signature = execution::set_value_t(std::size_t);
        static constexpr bool eof_on_zero = true;
        std::span<std::byte> buffer;
    };

    struct async_write_some_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::span<const std::byte> buffer;
    };

    struct async_read_some_at_t {
        using value_signature = execution::set_value_t(std::size_t);
        static constexpr bool eof_on_zero = true;
        std::size_t offset = 0;
        std::span<std::byte> buffer;
    };

    struct async_write_some_at_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::size_t offset = 0;
        std::span<const std::byte> buffer;
    };

    // A datagram receive: 0 bytes on a non-empty buffer is a legitimate empty datagram, NOT end-of-stream,
    // so NO eof_on_zero (unlike the stream receive below). Wire op is recv(), same as async_stream_receive_t
    // — separate types so the EOF semantic is deduced from the description, not the call site (mirrors the
    // stream/datagram send split).
    struct async_receive_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::span<std::byte> buffer;
    };

    // A stream (TCP) receive: recv() of 0 on a non-empty buffer == peer closed == end-of-stream.
    struct async_stream_receive_t {
        using value_signature = execution::set_value_t(std::size_t);
        static constexpr bool eof_on_zero = true;
        std::span<std::byte> buffer;
    };

    // A stream write: it can partially complete and block on send-buffer backpressure, so it stays
    // cancellable. Wire op is send()/prep_send, same as a datagram send — but the semantics differ.
    struct async_stream_send_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::span<const std::byte> buffer;
    };

    // A datagram send: atomic message, completes promptly regardless of the peer -> unstoppable. Distinct
    // from async_stream_send_t only in that semantic (identical wire op today); the split is what lets
    // stoppability be deduced from the description instead of passed at the call site.
    struct async_datagram_send_t {
        using value_signature = execution::set_value_t(std::size_t);
        static constexpr bool unstoppable = true;
        std::span<const std::byte> buffer;
    };

    struct async_receive_from_t {
        using value_signature = execution::set_value_t(endpoint, std::size_t);
        std::span<std::byte> buffer;
    };

    struct async_send_to_t {
        using value_signature = execution::set_value_t(std::size_t);
        static constexpr bool unstoppable = true; // datagram sendmsg: prompt, no cancellation needed
        std::span<const std::byte> buffer;
        endpoint peer;
    };

    struct async_accept_t {
        // Yields the freshly-minted connection as an opaque handle; the backend wraps the raw fd at the
        // CQE boundary (uring_state_base_for<async_accept_t>::complete), so nothing above sees a raw fd.
        using value_signature = execution::set_value_t(native_handle);
    };

    struct async_connect_t {
        using value_signature = execution::set_value_t();
        endpoint peer;
    };

    // A1: a timer is just an io op. On io_uring its prepare() emits an IORING_OP_TIMEOUT SQE, so it
    // reaps and cancels through the same machinery as every other op — no separate timer subsystem.
    struct async_sleep_t {
        using value_signature = execution::set_value_t();
        std::chrono::steady_clock::time_point deadline;
    };
}
