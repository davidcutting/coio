#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <thread>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/runtime/runtime.h>
#include <kioto/io/io.h>
#include <kioto/io/pipe.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    template<typename Sched>
    auto pinned_blocking_read(Sched ws, std::atomic<int>* started, std::atomic<int>* cancelled)
        -> kioto::task<> {
        auto [reader, writer] = kioto::make_pipe(ws);
        char buf[16];
        started->fetch_add(1, std::memory_order_relaxed);
        auto result = co_await kioto::execution::stopped_as_optional(
            reader.async_read_some(kioto::as_writable_bytes(buf))
        );
        if (not result.has_value()) cancelled->fetch_add(1, std::memory_order_relaxed);
        static_cast<void>(writer);
    }

    template<typename Runtime>
    auto spawn_pinned_reads(Runtime& rt, std::atomic<int>& started, std::atomic<int>& cancelled, int n)
        -> void {
        for (int i = 0; i < n; ++i) {
            rt.spawn(kioto::just() | kioto::then([&rt, &started, &cancelled] {
                auto ws = *Runtime::current_scheduler();
                rt.spawn_on(ws, pinned_blocking_read(ws, &started, &cancelled));
            }));
        }
    }

    template<typename Factory>
    auto run_teardown_cancel(Factory make_runtime) -> void {
        constexpr int n = 6;
        std::atomic<int> started{0};
        std::atomic<int> cancelled{0};
        {
            auto rt = make_runtime();
            spawn_pinned_reads(rt, started, cancelled, n);
            while (started.load(std::memory_order_relaxed) < n) std::this_thread::yield();
            std::this_thread::sleep_for(30ms);
        }
        CHECK_EQ(started.load(), n);
        CHECK_EQ(cancelled.load(), n);
    }
}

#if KIOTO_HAS_IO_URING
TEST_CASE("uring runtime: pinned in-flight reads are cancelled cross-thread at teardown") {
    run_teardown_cancel([] { return kioto::uring_runtime{3}; });
}
#endif

#if KIOTO_HAS_EPOLL
TEST_CASE("epoll runtime: pinned in-flight reads are cancelled cross-thread at teardown") {
    run_teardown_cancel([] { return kioto_test::make_runtime<kioto::epoll_context>(3); });
}
#endif

#if KIOTO_HAS_IOCP
TEST_CASE("iocp runtime: pinned in-flight reads are cancelled cross-thread at teardown") {
    run_teardown_cancel([] { return kioto_test::make_runtime<kioto::iocp_context>(3); });
}
#endif
