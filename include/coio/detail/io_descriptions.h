#pragma once
#include <chrono>
#include <concepts>
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

    struct async_read_some_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::span<std::byte> buffer;
    };

    struct async_write_some_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::span<const std::byte> buffer;
    };

    struct async_read_some_at_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::size_t offset = 0;
        std::span<std::byte> buffer;
    };

    struct async_write_some_at_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::size_t offset = 0;
        std::span<const std::byte> buffer;
    };

    struct async_receive_t {
        using value_signature = execution::set_value_t(std::size_t);
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
