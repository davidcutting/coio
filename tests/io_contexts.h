#pragma once
// Platform selection for the backend-generic tests: on Linux they run against epoll + uring; on
// Windows against iocp. Windows CI running these tests IS the validation of the iocp backend — do not
// simply exclude io tests on WIN32.
#include <cstddef>
#include <memory>
#include <coio/detail/config.h>
#include <coio/runtime.h>
#if COIO_HAS_EPOLL
#include <coio/asyncio/epoll_context.h>
#endif
#if COIO_HAS_IO_URING
#include <coio/asyncio/uring_context.h>
#include <coio/uring_runtime.h>
#endif
#if COIO_HAS_IOCP
#include <coio/asyncio/iocp_context.h>
#endif

// The platform's io-context list, for TEST_CASE_TEMPLATE over backends.
#if COIO_HAS_EPOLL and COIO_HAS_IO_URING
#define COIO_TEST_IO_CONTEXTS coio::epoll_context, coio::uring_context
#elif COIO_HAS_EPOLL
#define COIO_TEST_IO_CONTEXTS coio::epoll_context
#elif COIO_HAS_IOCP
#define COIO_TEST_IO_CONTEXTS coio::iocp_context
#endif

namespace coio_test {
    // Any single-owner io-capable context, for tests that only need "a worker".
#if COIO_HAS_IO_URING
    using default_io_context = coio::uring_context;
#elif COIO_HAS_EPOLL
    using default_io_context = coio::epoll_context;
#elif COIO_HAS_IOCP
    using default_io_context = coio::iocp_context;
#endif

    template<typename Context>
    [[nodiscard]] auto make_runtime(std::size_t workers) {
        return coio::basic_runtime<Context>{workers, [](std::size_t) { return std::make_unique<Context>(); }};
    }

    // The platform's convenience runtime, with uring_runtime's ctor shape everywhere.
#if COIO_HAS_IO_URING
    using default_runtime = coio::uring_runtime;
#else
    struct default_runtime : coio::basic_runtime<default_io_context> {
        explicit default_runtime(std::size_t workers)
            : basic_runtime(workers, [](std::size_t) { return std::make_unique<default_io_context>(); }) {}
    };
#endif
}
