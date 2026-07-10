// Async signal delivery (signal_wait) -- the graceful-shutdown primitive every server example awaits
// (co_await kioto::signal_wait(SIGINT, SIGTERM)). Backend-independent: it lives in base/ with its own
// watchdog thread, so this is not templated over io backends. Delivery is choreographed to avoid a race:
// SIGUSR1/SIGUSR2 default to Term, so we must never raise before the waiter has installed its handler --
// a helper thread spins until the handler is armed, then delivers. Raising while the main thread is
// blocked in run() also exercises the loop's EINTR resilience (a signal interrupts epoll_wait/io_uring).
//
// POSIX-only (SIGUSR#/sigaction); on Windows this TU compiles to zero tests.
#include <kioto/base/config.h>

#if KIOTO_OS_LINUX

#include <csignal>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <system_error>
#include <thread>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/base/signal_wait.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    using context_t = kioto_test::default_io_context;

    template<typename Sndr, typename Sched>
    auto with_timeout(Sndr sndr, Sched sched, std::chrono::milliseconds ms) {
        return kioto::when_any(
            std::move(sndr),
            sched.schedule_after(ms) | kioto::let_value([]() noexcept { return kioto::just_stopped(); }));
    }

    [[nodiscard]] auto handler_is_default(int sig) -> bool {
        struct ::sigaction cur{};
        ::sigaction(sig, nullptr, &cur);
        return cur.sa_handler == SIG_DFL;
    }

    // Spin until kioto has installed its handler for `sig` (proving the waiter is armed), then deliver it
    // process-wide. Arming is what makes delivery safe -- before it, SIGUSR#'s default action terminates
    // the process. A bounded spin; on the (bug) path where arming never happens we still deliver so the
    // failure is loud rather than a hang.
    void deliver_when_armed(int sig) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        for (;;) {
            if (not handler_is_default(sig)) break;
            if (std::chrono::steady_clock::now() >= deadline) break;
            std::this_thread::yield();
        }
        ::kill(::getpid(), sig);
    }
}

// signal_wait registers with the external signal-watchdog thread, not the io_context, so it places no op
// in the loop -- a bare ctx.run() with ONLY a parked signal_wait would see itself as idle and return before
// the signal ever fires (in real servers the loop is kept alive by other work, e.g. a live accept loop).
// So the delivery tests run on a runtime whose worker stays alive until stopped, and block on sync_wait.
TEST_CASE("signal_wait completes with the signal number when the signal is delivered") {
    auto rt = kioto_test::make_runtime<context_t>(1);

    std::thread deliverer{deliver_when_armed, SIGUSR1};
    auto [received] = kioto::this_thread::sync_wait(
        kioto::starts_on(rt.get_scheduler(), kioto::signal_wait(SIGUSR1))).value();
    deliverer.join();

    CHECK(received == SIGUSR1);
    CHECK(handler_is_default(SIGUSR1));   // delivery restores the disposition it saved
}

TEST_CASE("signal_wait over multiple signals completes with whichever fires (the shutdown pattern)") {
    auto rt = kioto_test::make_runtime<context_t>(1);

    std::thread deliverer{deliver_when_armed, SIGUSR2};
    auto [received] = kioto::this_thread::sync_wait(   // the co_await used by every server example
        kioto::starts_on(rt.get_scheduler(), kioto::signal_wait(SIGUSR1, SIGUSR2))).value();
    deliverer.join();

    CHECK(received == SIGUSR2);
    CHECK(handler_is_default(SIGUSR1));   // the sibling wait was cancelled and cleaned up...
    CHECK(handler_is_default(SIGUSR2));   // ...and the fired one restored its disposition
}

TEST_CASE("a signal_wait after an earlier delivery re-arms rather than firing spuriously") {
    auto rt = kioto_test::make_runtime<context_t>(1);
    const auto sched = rt.get_scheduler();

    // Two consecutive deliveries on the same signal. The second is the regression: pre-fix, pop_all left the
    // list latched at head == nullptr, so the second signal_wait returned set_value(SIGUSR1) IMMEDIATELY --
    // without re-installing a handler and without waiting for a new signal. We prove each wait genuinely
    // waited for its OWN delivery: the deliverer sets `raised` only once it observes the handler armed, just
    // before kill(). A short-circuit completes before the handler is ever installed, leaving raised == false.
    for (int i = 0; i < 2; ++i) {
        std::atomic<bool> raised{false};
        std::thread deliverer{[&raised] {
            const auto deadline = std::chrono::steady_clock::now() + 5s;
            struct ::sigaction cur{};
            for (;;) {
                ::sigaction(SIGUSR1, nullptr, &cur);
                if (cur.sa_handler != SIG_DFL) { raised.store(true); ::kill(::getpid(), SIGUSR1); return; }
                if (std::chrono::steady_clock::now() >= deadline) return;   // never armed (a regression)
                std::this_thread::yield();
            }
        }};
        auto [v] = kioto::this_thread::sync_wait(kioto::starts_on(sched, kioto::signal_wait(SIGUSR1))).value();
        deliverer.join();
        CHECK(v == SIGUSR1);
        CHECK(raised.load());   // completed only after its own delivery -> no latched short-circuit
    }
}

TEST_CASE("a cancelled signal_wait completes stopped and restores the signal disposition") {
    context_t ctx;
    kioto::async_scope scope;
    bool stopped = false;

    scope.spawn_on(ctx.get_scheduler(), [](context_t::scheduler sched, bool& out) -> context_t::task<> {
        // nothing raises SIGUSR1 -> the 30ms timeout cancels the wait. signal_wait completes set_value(int),
        // so stopped_as_optional observes the cancellation as an empty optional.
        auto r = co_await kioto::execution::stopped_as_optional(
            with_timeout(kioto::signal_wait(SIGUSR1), sched, 30ms));
        out = not r.has_value();
    }(ctx.get_scheduler(), stopped));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(stopped);
    CHECK(handler_is_default(SIGUSR1));   // cancellation must unregister and put the handler back to SIG_DFL
}

TEST_CASE("waiting on an invalid signal number surfaces invalid_argument") {
    context_t ctx;
    kioto::async_scope scope;
    std::error_code ec;

    scope.spawn_on(ctx.get_scheduler(), [](std::error_code& out) -> context_t::task<> {
        try { co_await kioto::signal_wait(99999); }   // out of [0, NSIG) -> rejected before any handler is set
        catch (const std::system_error& e) { out = e.code(); }
    }(ec));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(ec == std::errc::invalid_argument);
}

TEST_CASE("signal metadata helpers") {
    CHECK(kioto::detail::is_available_signal(SIGUSR1));
    CHECK(kioto::detail::is_available_signal(SIGINT));
    CHECK_FALSE(kioto::detail::is_available_signal(-1));
    CHECK_FALSE(kioto::detail::is_available_signal(99999));
    CHECK_FALSE(kioto::strsignal(SIGINT).empty());
}

#endif // KIOTO_OS_LINUX
