#pragma once
#include <chrono>
#include <coio/detail/execution.h>
#include <coio/net/basic.h>

namespace coio::detail {
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

    struct async_send_t {
        using value_signature = execution::set_value_t(std::size_t);
        std::span<const std::byte> buffer;
    };

    struct async_receive_from_t {
        using value_signature = execution::set_value_t(endpoint, std::size_t);
        std::span<std::byte> buffer;
    };

    struct async_send_to_t {
        using value_signature = execution::set_value_t(std::size_t);
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
