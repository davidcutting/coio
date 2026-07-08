#pragma once
#include <cstddef>
#include <memory>
#include <coio/asyncio/uring_context.h>
#include <coio/runtime.h>

namespace coio {
    class uring_runtime : public basic_runtime<uring_context> {
    public:
        template<thread_launcher Launcher = default_thread_launcher>
        explicit uring_runtime(
            std::size_t workers = basic_runtime<uring_context>::default_worker_count(),
            std::size_t entries = 512,
            Launcher launch = {}
        ) : basic_runtime<uring_context>(
                workers,
                [entries](std::size_t) { return std::make_unique<uring_context>(entries); },
                std::move(launch)
            ) {}
    };
}
