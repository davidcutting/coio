#include <chrono>
#include <string>
#include <string_view>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/io/pipe.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    KIOTO_ALWAYS_INLINE auto dispatch_result(std::error_code ec, std::size_t n) noexcept {
        kioto::async_result<kioto::execution::set_value_t(std::size_t), kioto::execution::set_error_t(std::error_code)> r;
        if (ec) {
            if (ec == std::errc::operation_canceled) r.set_stopped();
            else r.set_error(ec);
        }
        else r.set_value(n);
        return r;
    }
    inline const auto as_throwing = kioto::execution::let_value(dispatch_result);

    template<typename Sndr, typename Sched>
    auto with_timeout(Sndr sndr, Sched sched, std::chrono::milliseconds ms) {
        return kioto::when_any(
            std::move(sndr),
            sched.schedule_after(ms) | kioto::let_value([]() noexcept { return kioto::just_stopped(); })
        );
    }
}

TEST_CASE_TEMPLATE("pipe write/read roundtrip drives the reactor completion path", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    Ctx context;
    auto [reader, writer] = kioto::make_pipe(context.get_scheduler());
    kioto::async_scope scope;

    constexpr std::string_view payload = "hello-reactor-io\n";
    std::string received;

    scope.spawn([](kioto::pipe_reader<typename Ctx::scheduler> r, std::string& out) -> kioto::task<> {
        char buf[8];
        while (true) {
            const auto n = co_await r.async_read_some(kioto::as_writable_bytes(buf));
            out.append(buf, n);
            if (out.ends_with('\n')) break;
        }
    }(std::move(reader), received));

    scope.spawn([](kioto::pipe_writer<typename Ctx::scheduler> w, std::string_view msg) -> kioto::task<> {
        co_await (kioto::async_write(w, kioto::as_bytes(msg)) | as_throwing);
    }(std::move(writer), payload));

    context.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(received == payload);
}

TEST_CASE_TEMPLATE("a blocking pipe read is cancelled by a timeout", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    Ctx context;
    auto [reader, writer] = kioto::make_pipe(context.get_scheduler());
    kioto::async_scope scope;

    bool cancelled = false;
    scope.spawn([](kioto::pipe_reader<typename Ctx::scheduler> r,
                   typename Ctx::scheduler sched, bool& out) -> kioto::task<> {
        char buf[64];
        auto result = co_await kioto::execution::stopped_as_optional(
            with_timeout(r.async_read_some(kioto::as_writable_bytes(buf)), sched, 30ms)
        );
        out = not result.has_value();
    }(std::move(reader), context.get_scheduler(), cancelled));

    context.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(cancelled);
    static_cast<void>(writer);
}
