// Timer semantics, templated over backends (uring TIMEOUT op / epoll timerfd / iocp deadline-heap driving
// the GQCS timeout). test_time_loop only exercises cross-thread posting — this covers the timers.
#include <chrono>
#include <cstddef>
#include <string_view>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/io/pipe.h>
#include "io_contexts.h"

using namespace std::chrono_literals;
using namespace std::string_view_literals;

namespace {
    KIOTO_ALWAYS_INLINE auto dispatch_result(std::error_code ec, std::size_t n) noexcept {
        kioto::async_result<kioto::execution::set_value_t(std::size_t), kioto::execution::set_error_t(std::error_code)> r;
        if (ec) { if (ec == std::errc::operation_canceled) r.set_stopped(); else r.set_error(ec); }
        else r.set_value(n);
        return r;
    }
    inline const auto as_throwing = kioto::execution::let_value(dispatch_result);

    template<typename Sndr, typename Sched>
    auto with_timeout(Sndr sndr, Sched sched, std::chrono::milliseconds ms) {
        return kioto::when_any(
            std::move(sndr),
            sched.schedule_after(ms) | kioto::let_value([]() noexcept { return kioto::just_stopped(); }));
    }
}

TEST_CASE_TEMPLATE("multiple schedule_after timers fire in deadline order", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    Ctx ctx;
    kioto::async_scope scope;
    std::vector<int> order;   // single-owner context -> pushes are race-free

    // spawned out of deadline order; must complete in deadline order (10, 20, 30)
    for (auto d : {30, 10, 20}) {
        scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, int ms, std::vector<int>& out)
                                               -> typename Ctx::template task<> {
            co_await sched.schedule_after(std::chrono::milliseconds{ms});
            out.push_back(ms);
        }(ctx.get_scheduler(), d, order));
    }

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    REQUIRE(order.size() == 3);
    CHECK(order[0] == 10);
    CHECK(order[1] == 20);
    CHECK(order[2] == 30);
}

TEST_CASE_TEMPLATE("schedule_at in the past fires promptly", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    Ctx ctx;
    kioto::async_scope scope;
    bool fired = false;

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, bool& out)
                                           -> typename Ctx::template task<> {
        co_await sched.schedule_at(sched.now() - 1s);   // already-elapsed deadline
        out = true;
    }(ctx.get_scheduler(), fired));

    const auto start = std::chrono::steady_clock::now();
    ctx.run();
    kioto::this_thread::sync_wait(scope.join());
    const auto elapsed = std::chrono::steady_clock::now() - start;

    CHECK(fired);
    CHECK(elapsed < 500ms);   // a past deadline must not stall the loop
}

TEST_CASE_TEMPLATE("cancelling a timer removes it from the heap (does not wait its deadline)", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    Ctx ctx;
    kioto::async_scope scope;
    bool stopped = false;

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, bool& out)
                                           -> typename Ctx::template task<> {
        // a 10s timer cancelled after 20ms: if it weren't removed from the heap, run() would block ~10s.
        // Timers complete set_value() (void), so observe the cancellation via upon_stopped, not
        // stopped_as_optional (which requires a one-argument value completion).
        co_await (with_timeout(sched.schedule_after(10s), sched, 20ms)
                  | kioto::upon_stopped([&out]() noexcept { out = true; }));
    }(ctx.get_scheduler(), stopped));

    const auto start = std::chrono::steady_clock::now();
    ctx.run();
    kioto::this_thread::sync_wait(scope.join());
    const auto elapsed = std::chrono::steady_clock::now() - start;

    CHECK(stopped);
    CHECK(elapsed < 2s);   // proves the long timer was removed, not awaited
}

TEST_CASE_TEMPLATE("many interleaved timers and io on one loop all complete", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    Ctx ctx;
    auto [reader, writer] = kioto::make_pipe(ctx.get_scheduler());
    kioto::async_scope scope;

    constexpr int n_timers = 24;
    int timers_fired = 0;
    std::string received;

    // a spread of interleaved deadlines
    for (int i = 0; i < n_timers; ++i) {
        scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, int ms, int& count)
                                               -> typename Ctx::template task<> {
            co_await sched.schedule_after(std::chrono::milliseconds{ms});
            ++count;
        }(ctx.get_scheduler(), 1 + (i * 7) % 40, timers_fired));
    }

    // a pipe roundtrip sharing the same loop
    scope.spawn([](kioto::pipe_reader<typename Ctx::scheduler> r, std::string& out) -> kioto::task<> {
        char buf[16];
        const auto n = co_await r.async_read_some(kioto::as_writable_bytes(buf));
        out.assign(buf, n);
    }(std::move(reader), received));
    scope.spawn([](kioto::pipe_writer<typename Ctx::scheduler> w) -> kioto::task<> {
        co_await (kioto::async_write(w, kioto::as_bytes("io+timers"sv)) | as_throwing);
    }(std::move(writer)));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(timers_fired == n_timers);
    CHECK(received == "io+timers");
}
