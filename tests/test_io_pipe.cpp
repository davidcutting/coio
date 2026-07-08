#include <chrono>
#include <string>
#include <string_view>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/asyncio/io.h>
#include <coio/asyncio/pipe.h>
#include <coio/asyncio/epoll_context.h>
#include <coio/asyncio/uring_context.h>

using namespace std::chrono_literals;

namespace {
    COIO_ALWAYS_INLINE auto dispatch_result(std::error_code ec, std::size_t n) noexcept {
        coio::async_result<coio::execution::set_value_t(std::size_t), coio::execution::set_error_t(std::error_code)> r;
        if (ec) {
            if (ec == std::errc::operation_canceled) r.set_stopped();
            else r.set_error(ec);
        }
        else r.set_value(n);
        return r;
    }
    inline const auto as_throwing = coio::execution::let_value(dispatch_result);

    template<typename Sndr, typename Sched>
    auto with_timeout(Sndr sndr, Sched sched, std::chrono::milliseconds ms) {
        return coio::when_any(
            std::move(sndr),
            sched.schedule_after(ms) | coio::let_value([]() noexcept { return coio::just_stopped(); })
        );
    }
}

TEST_CASE_TEMPLATE("pipe write/read roundtrip drives the reactor completion path", Ctx,
                   coio::epoll_context, coio::uring_context) {
    Ctx context;
    auto [reader, writer] = coio::make_pipe(context.get_scheduler());
    coio::async_scope scope;

    constexpr std::string_view payload = "hello-reactor-io\n";
    std::string received;

    scope.spawn([](coio::pipe_reader<typename Ctx::scheduler> r, std::string& out) -> coio::task<> {
        char buf[8];
        while (true) {
            const auto n = co_await r.async_read_some(coio::as_writable_bytes(buf));
            out.append(buf, n);
            if (out.ends_with('\n')) break;
        }
    }(std::move(reader), received));

    scope.spawn([](coio::pipe_writer<typename Ctx::scheduler> w, std::string_view msg) -> coio::task<> {
        co_await (coio::async_write(w, coio::as_bytes(msg)) | as_throwing);
    }(std::move(writer), payload));

    context.run();
    coio::this_thread::sync_wait(scope.join());

    CHECK(received == payload);
}

TEST_CASE_TEMPLATE("a blocking pipe read is cancelled by a timeout", Ctx,
                   coio::epoll_context, coio::uring_context) {
    Ctx context;
    auto [reader, writer] = coio::make_pipe(context.get_scheduler());
    coio::async_scope scope;

    bool cancelled = false;
    scope.spawn([](coio::pipe_reader<typename Ctx::scheduler> r,
                   typename Ctx::scheduler sched, bool& out) -> coio::task<> {
        char buf[64];
        auto result = co_await coio::execution::stopped_as_optional(
            with_timeout(r.async_read_some(coio::as_writable_bytes(buf)), sched, 30ms)
        );
        out = not result.has_value();
    }(std::move(reader), context.get_scheduler(), cancelled));

    context.run();
    coio::this_thread::sync_wait(scope.join());

    CHECK(cancelled);
    static_cast<void>(writer);
}
