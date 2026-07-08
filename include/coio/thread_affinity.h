#pragma once
#include <concepts>
#include <cstddef>
#include <thread>
#include <utility>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace coio {
#if defined(__linux__)
    struct pin_to_core {
        template<std::invocable Entry>
        auto operator() (std::size_t index, Entry entry) const -> std::jthread {
            return std::jthread{[index, entry = std::move(entry)]() mutable {
                const unsigned ncpu = std::thread::hardware_concurrency();
                ::cpu_set_t set;
                CPU_ZERO(&set);
                CPU_SET(static_cast<int>(index % (ncpu == 0 ? 1u : ncpu)), &set);
                static_cast<void>(::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set));
                std::move(entry)();
            }};
        }
    };
#endif
}
