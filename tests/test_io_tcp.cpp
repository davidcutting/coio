#include <chrono>
#include <string>
#include <string_view>
#include <doctest/doctest.h>
#include <coio/core.h>
#include <coio/asyncio/io.h>
#include <coio/net/socket.h>
#include <coio/net/tcp.h>
#include "io_contexts.h"

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

TEST_CASE_TEMPLATE("tcp loopback echo roundtrip drives accept/connect/read/write", Ctx,
                   COIO_TEST_IO_CONTEXTS) {
    using acceptor_t = coio::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = coio::tcp::socket<typename Ctx::scheduler>;

    Ctx context;
    acceptor_t acceptor{context.get_scheduler(), coio::endpoint{coio::ipv4_address::loopback(), 0}};
    const coio::endpoint ep = acceptor.local_endpoint();
    coio::async_scope scope;
    std::string echoed;

    scope.spawn_on(context.get_scheduler(), [](acceptor_t& acc) -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        char buf[64];
        const auto n = co_await sock.async_read_some(coio::as_writable_bytes(buf));
        co_await (coio::async_write(sock, coio::as_bytes(buf, n)) | as_throwing);
    }(acceptor));

    scope.spawn_on(context.get_scheduler(),
                   [](typename Ctx::scheduler sched, coio::endpoint peer, std::string& out)
                       -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        constexpr std::string_view msg = "tcp-loopback-hello";
        co_await (coio::async_write(sock, coio::as_bytes(msg)) | as_throwing);
        char buf[64];
        const auto n = co_await sock.async_read_some(coio::as_writable_bytes(buf));
        out.assign(buf, n);
    }(context.get_scheduler(), ep, echoed));

    context.run();
    coio::this_thread::sync_wait(scope.join());

    CHECK(echoed == "tcp-loopback-hello");
}

TEST_CASE_TEMPLATE("a server-side tcp read is cancelled by a timeout while the peer stays idle", Ctx,
                   COIO_TEST_IO_CONTEXTS) {
    using acceptor_t = coio::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = coio::tcp::socket<typename Ctx::scheduler>;

    Ctx context;
    acceptor_t acceptor{context.get_scheduler(), coio::endpoint{coio::ipv4_address::loopback(), 0}};
    const coio::endpoint ep = acceptor.local_endpoint();
    coio::async_scope scope;
    bool cancelled = false;

    scope.spawn_on(context.get_scheduler(),
                   [](acceptor_t& acc, typename Ctx::scheduler sched, bool& out)
                       -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        char buf[64];
        auto result = co_await coio::execution::stopped_as_optional(
            with_timeout(sock.async_read_some(coio::as_writable_bytes(buf)), sched, 30ms)
        );
        out = not result.has_value();
    }(acceptor, context.get_scheduler(), cancelled));

    scope.spawn_on(context.get_scheduler(),
                   [](typename Ctx::scheduler sched, coio::endpoint peer) -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await sched.schedule_after(100ms);
    }(context.get_scheduler(), ep));

    context.run();
    coio::this_thread::sync_wait(scope.join());

    CHECK(cancelled);
}
