#pragma once
#include <chrono>
#include <concepts>
#include <cstddef>
#include <kioto/base/error.h>
#include <kioto/exec/execution.h>
#include <kioto/base/basic.h>

namespace kioto::detail {
    // Opt out of cancellation via `static constexpr bool unstoppable = true`: the op completes promptly
    // regardless of the peer (datagram send), so schedule_io skips the stop_when wrap. On the description
    // because it's a semantic fact of the op, not a call-site choice.
    template<typename Sexpr>
    inline constexpr bool is_unstoppable = [] {
        if constexpr (requires { { Sexpr::unstoppable } -> std::convertible_to<bool>; }) return Sexpr::unstoppable;
        else return false;
    }();

    // `static constexpr bool eof_on_zero = true`: 0 bytes into a non-empty buffer == end-of-stream. The
    // driver folds it via deliver_read_result below, so no wrapping EOF sender per read.
    template<typename Sexpr>
    inline constexpr bool is_eof_on_zero = [] {
        if constexpr (requires { { Sexpr::eof_on_zero } -> std::convertible_to<bool>; }) return Sexpr::eof_on_zero;
        else return false;
    }();

    // Delivers a byte count, folding EOF-on-zero per the description. Called at every driver's set_value
    // site. `Result` is the op's async_result (deduced to avoid the include).
    template<typename IoOp, typename Result>
    KIOTO_ALWAYS_INLINE auto deliver_read_result(Result& result, std::size_t bytes, bool buffer_empty) noexcept -> void {
        if constexpr (is_eof_on_zero<IoOp>) {
            if (bytes == 0 and not buffer_empty) [[unlikely]] { result.set_error(kioto::error::eof); return; }
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

    // Datagram receive: 0 bytes on a non-empty buffer is a valid empty datagram, NOT EOF -> no eof_on_zero.
    // Same wire op (recv) as async_stream_receive_t; split so the EOF semantic is deduced, not passed.
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

    // Stream write: can partially complete and block on send-buffer backpressure -> stays cancellable.
    struct async_stream_send_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::span<const std::byte> buffer;
    };

    // Datagram send: atomic, prompt -> unstoppable. Same wire op as async_stream_send_t; split so
    // stoppability is deduced from the description.
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
        // Yields the connection as an opaque handle; the driver wraps the raw fd at the CQE boundary.
        using value_signature = execution::set_value_t(native_handle);
    };

    struct async_connect_t {
        using value_signature = execution::set_value_t();
        endpoint peer;
    };

    // A timer is just an io op: on io_uring prepare() emits IORING_OP_TIMEOUT, reaped/cancelled through
    // the same machinery as every op — no separate timer subsystem.
    struct async_sleep_t {
        using value_signature = execution::set_value_t();
        std::chrono::steady_clock::time_point deadline;
    };
}
