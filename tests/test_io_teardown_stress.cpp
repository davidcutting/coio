// Stress test for off-owner io_object teardown — the hardened path from the driver refactor.
//
// The real-world hazard (case #1): a handle is used correctly on its owner worker, but its last owning
// reference (here a shared_ptr) is dropped by a DIFFERENT thread after the I/O is done. uring must not
// touch its single-issuer ring off-owner; epoll must not free per_fd_data under a racing teardown. Both
// are supposed to be safe for an *idle* handle now.
//
// Each roundtrip does a real pipe write+read on the worker (so the handle genuinely registered/submitted
// and then went idle), then hands the shared_ptr to a sink drained by a non-owner thread — so whichever
// thread wins the last-ref race destroys the handle, often off-owner. Work is batched to keep the live fd
// count well under typical ulimits. Run under -fsanitize=thread/address to turn a latent race into a
// failure; even without a sanitizer a broken teardown asserts or corrupts and fails the data check.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/runtime.h>
#include <coio/uring_runtime.h>
#include <coio/asyncio/io.h>
#include <coio/asyncio/pipe.h>
#include <coio/asyncio/epoll_context.h>

using namespace std::chrono_literals;

namespace {
    COIO_ALWAYS_INLINE auto dispatch_result(std::error_code ec, std::size_t n) noexcept {
        coio::async_result<coio::execution::set_value_t(std::size_t), coio::execution::set_error_t(std::error_code)> r;
        if (ec) { if (ec == std::errc::operation_canceled) r.set_stopped(); else r.set_error(ec); }
        else r.set_value(n);
        return r;
    }
    inline const auto as_throwing = coio::execution::let_value(dispatch_result);

    // Type-erased stash. Whichever thread calls drain() destroys the handles it pops — deliberately not the
    // owner worker, so idle off-owner teardown is exercised.
    class handle_sink {
    public:
        auto push(std::shared_ptr<void> h) -> void {
            std::scoped_lock lk{m_};
            items_.push_back(std::move(h));
        }
        auto drain() -> void {
            std::vector<std::shared_ptr<void>> local;
            { std::scoped_lock lk{m_}; local.swap(items_); }
            // `local` drops here -> handles destroyed on the CALLER's thread.
        }
        [[nodiscard]] auto empty() -> bool {
            std::scoped_lock lk{m_};
            return items_.empty();
        }
    private:
        std::mutex m_;
        std::vector<std::shared_ptr<void>> items_;
    };

    template<typename Sched>
    auto roundtrip(std::shared_ptr<std::pair<coio::pipe_reader<Sched>, coio::pipe_writer<Sched>>> p,
                   handle_sink& sink, std::atomic<int>& ok) -> coio::task<> {
        static constexpr std::string_view msg = "ping";
        char buf[8];
        co_await (coio::async_write(p->second, coio::as_bytes(msg)) | as_throwing);       // fits the pipe buffer
        const auto n = co_await p->first.async_read_some(coio::as_writable_bytes(buf));
        if (n == msg.size()) ok.fetch_add(1, std::memory_order_relaxed);
        sink.push(std::move(p));   // hand the last-ref race to the draining thread; the frame drops its ref on return
    }

    template<typename Runtime>
    auto spawn_batch(Runtime& rt, handle_sink& sink, std::atomic<int>& ok, int n) -> void {
        for (int i = 0; i < n; ++i) {
            rt.spawn(coio::just() | coio::then([&rt, &sink, &ok] {
                auto ws = *Runtime::current_scheduler();
                using Sched = decltype(ws);
                auto [reader, writer] = coio::make_pipe(ws);
                auto p = std::make_shared<std::pair<coio::pipe_reader<Sched>, coio::pipe_writer<Sched>>>(
                    std::move(reader), std::move(writer));
                rt.spawn_on(ws, roundtrip<Sched>(std::move(p), sink, ok));
            }));
        }
    }

    auto wait_until = [](auto pred) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (not pred()) {
            REQUIRE(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(1ms);
        }
    };

    template<typename Factory>
    auto run_off_owner_teardown_stress(Factory make_runtime) -> void {
        constexpr int rounds = 32;
        constexpr int batch = 32;   // <= ~64 live fds at a time -> safe under default ulimits
        std::atomic<int> ok{0};
        handle_sink sink;
        std::atomic<bool> draining{true};
        {
            auto rt = make_runtime();
            // A dedicated non-owner thread hammers off-owner destruction concurrently with the live reactor.
            std::thread drainer([&] {
                while (draining.load(std::memory_order_relaxed)) { sink.drain(); std::this_thread::yield(); }
            });
            for (int r = 0; r < rounds; ++r) {
                const int target = (r + 1) * batch;
                spawn_batch(rt, sink, ok, batch);
                wait_until([&] { return ok.load(std::memory_order_relaxed) >= target; });
                wait_until([&] { return sink.empty(); });   // let this batch's fds close before the next
            }
            draining.store(false, std::memory_order_relaxed);
            drainer.join();
            sink.drain();   // final sweep, also off-owner (main thread), while workers are still alive
        } // rt destroyed -> workers stop
        CHECK_EQ(ok.load(), rounds * batch);
    }
}

TEST_CASE("uring: idle handles dropped off-owner during a live reactor tear down cleanly") {
    run_off_owner_teardown_stress([] { return coio::uring_runtime{4}; });
}

TEST_CASE("epoll: idle handles dropped off-owner during a live reactor tear down cleanly") {
    run_off_owner_teardown_stress([] {
        return coio::basic_runtime<coio::epoll_context>{
            4, [](std::size_t) { return std::make_unique<coio::epoll_context>(); }
        };
    });
}
