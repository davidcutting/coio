#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <thread>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/runtime.h>
#include <coio/uring_runtime.h>
#include <coio/asyncio/io.h>
#include <coio/asyncio/pipe.h>
#include <coio/asyncio/epoll_context.h>

using namespace std::chrono_literals;

namespace {
    template<typename Sched>
    auto pinned_blocking_read(Sched ws, std::atomic<int>* started, std::atomic<int>* cancelled)
        -> coio::task<> {
        auto [reader, writer] = coio::make_pipe(ws);
        char buf[16];
        started->fetch_add(1, std::memory_order_relaxed);
        auto result = co_await coio::execution::stopped_as_optional(
            reader.async_read_some(coio::as_writable_bytes(buf))
        );
        if (not result.has_value()) cancelled->fetch_add(1, std::memory_order_relaxed);
        static_cast<void>(writer);
    }

    template<typename Runtime>
    auto spawn_pinned_reads(Runtime& rt, std::atomic<int>& started, std::atomic<int>& cancelled, int n)
        -> void {
        for (int i = 0; i < n; ++i) {
            rt.spawn(coio::just() | coio::then([&rt, &started, &cancelled] {
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

TEST_CASE("uring runtime: pinned in-flight reads are cancelled cross-thread at teardown") {
    run_teardown_cancel([] { return coio::uring_runtime{3}; });
}

TEST_CASE("epoll runtime: pinned in-flight reads are cancelled cross-thread at teardown") {
    run_teardown_cancel([] {
        return coio::basic_runtime<coio::epoll_context>{
            3, [](std::size_t) { return std::make_unique<coio::epoll_context>(); }
        };
    });
}
