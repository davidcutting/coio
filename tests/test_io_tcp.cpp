#include <chrono>
#include <string>
#include <string_view>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
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

TEST_CASE_TEMPLATE("tcp loopback echo roundtrip drives accept/connect/read/write", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;

    Ctx context;
    acceptor_t acceptor{context.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;
    std::string echoed;

    scope.spawn_on(context.get_scheduler(), [](acceptor_t& acc) -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        char buf[64];
        const auto n = co_await sock.async_read_some(kioto::as_writable_bytes(buf));
        co_await (kioto::async_write(sock, kioto::as_bytes(buf, n)) | as_throwing);
    }(acceptor));

    scope.spawn_on(context.get_scheduler(),
                   [](typename Ctx::scheduler sched, kioto::endpoint peer, std::string& out)
                       -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        constexpr std::string_view msg = "tcp-loopback-hello";
        co_await (kioto::async_write(sock, kioto::as_bytes(msg)) | as_throwing);
        char buf[64];
        const auto n = co_await sock.async_read_some(kioto::as_writable_bytes(buf));
        out.assign(buf, n);
    }(context.get_scheduler(), ep, echoed));

    context.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(echoed == "tcp-loopback-hello");
}

TEST_CASE_TEMPLATE("a server-side tcp read is cancelled by a timeout while the peer stays idle", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;

    Ctx context;
    acceptor_t acceptor{context.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;
    bool cancelled = false;

    scope.spawn_on(context.get_scheduler(),
                   [](acceptor_t& acc, typename Ctx::scheduler sched, bool& out)
                       -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        char buf[64];
        auto result = co_await kioto::execution::stopped_as_optional(
            with_timeout(sock.async_read_some(kioto::as_writable_bytes(buf)), sched, 30ms)
        );
        out = not result.has_value();
    }(acceptor, context.get_scheduler(), cancelled));

    scope.spawn_on(context.get_scheduler(),
                   [](typename Ctx::scheduler sched, kioto::endpoint peer) -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await sched.schedule_after(100ms);
    }(context.get_scheduler(), ep));

    context.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(cancelled);
}
