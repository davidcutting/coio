#pragma once
// Platform selection for the backend-generic tests: on Linux they run against epoll + uring; on
// Windows against iocp. Windows CI running these tests IS the validation of the iocp backend — do not
// simply exclude io tests on WIN32.
#include <cstddef>
#include <memory>
#include <kioto/base/config.h>
#include <kioto/runtime/runtime.h>
#if KIOTO_HAS_EPOLL
#include <kioto/io/driver/epoll_context.h>
#endif
#if KIOTO_HAS_IO_URING
#include <kioto/io/driver/uring_context.h>
#include <kioto/runtime/uring_runtime.h>
#endif
#if KIOTO_HAS_IOCP
#include <kioto/io/driver/iocp_context.h>
#endif

// The platform's io-context list, for TEST_CASE_TEMPLATE over backends.
#if KIOTO_HAS_EPOLL and KIOTO_HAS_IO_URING
#define KIOTO_TEST_IO_CONTEXTS kioto::epoll_context, kioto::uring_context
#elif KIOTO_HAS_EPOLL
#define KIOTO_TEST_IO_CONTEXTS kioto::epoll_context
#elif KIOTO_HAS_IOCP
#define KIOTO_TEST_IO_CONTEXTS kioto::iocp_context
#endif

// The file-capable contexts (capability::file: io_uring/IOCP only — epoll can't serve regular files).
#if KIOTO_HAS_IO_URING
#define KIOTO_TEST_FILE_CONTEXTS kioto::uring_context
#elif KIOTO_HAS_IOCP
#define KIOTO_TEST_FILE_CONTEXTS kioto::iocp_context
#endif

namespace kioto_test {
    // Any single-owner io-capable context, for tests that only need "a worker".
#if KIOTO_HAS_IO_URING
    using default_io_context = kioto::uring_context;
#elif KIOTO_HAS_EPOLL
    using default_io_context = kioto::epoll_context;
#elif KIOTO_HAS_IOCP
    using default_io_context = kioto::iocp_context;
#endif

    template<typename Context>
    [[nodiscard]] auto make_runtime(std::size_t workers) {
        return kioto::basic_runtime<Context>{workers, [](std::size_t) { return std::make_unique<Context>(); }};
    }

    // The platform's convenience runtime, with uring_runtime's ctor shape everywhere.
#if KIOTO_HAS_IO_URING
    using default_runtime = kioto::uring_runtime;
#else
    struct default_runtime : kioto::basic_runtime<default_io_context> {
        explicit default_runtime(std::size_t workers)
            : basic_runtime(workers, [](std::size_t) { return std::make_unique<default_io_context>(); }) {}
    };
#endif
}
