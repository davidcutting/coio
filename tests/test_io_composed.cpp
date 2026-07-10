// Composed / high-level stream ops (async_read / async_write / async_read_until) — the primary user-facing
// API, whose loop + buffer + EOF logic the one-shot tests never exercise. These complete
// set_value(std::error_code, std::size_t) (they don't throw), so success paths use `| as_throwing` and the
// EOF-partial case captures the (ec, n) pair directly. Payloads force MULTIPLE underlying read/write ops.
#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/base/error.h>
#include <kioto/base/flat_buffer.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
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

    auto make_pattern(std::size_t n) -> std::vector<std::byte> {
        std::vector<std::byte> v(n);
        for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<std::byte>('A' + (i % 26));
        return v;
    }
    auto to_string(std::span<const std::byte> b) -> std::string {
        return {reinterpret_cast<const char*>(b.data()), b.size()};
    }
}

TEST_CASE_TEMPLATE("async_read fills a large buffer across many read_somes while async_write drains it", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    constexpr std::size_t total = 256 * 1024;   // >> one send/recv -> forces the read AND write loops
    const auto payload = make_pattern(total);
    std::vector<std::byte> got(total, std::byte{0});
    std::size_t nread = 0;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, std::span<std::byte> out, std::size_t& nr)
                                           -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        nr = co_await (kioto::async_read(sock, out) | as_throwing);   // reads exactly out.size()
    }(acceptor, std::span(got), nread));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer,
                                           std::span<const std::byte> data) -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await (kioto::async_write(sock, data) | as_throwing);       // writes all of data
    }(ctx.get_scheduler(), ep, std::span<const std::byte>(payload)));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(nread == total);
    CHECK(got == payload);
}

TEST_CASE_TEMPLATE("async_read into a dynamic buffer reads exactly N bytes", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    constexpr std::size_t n = 96 * 1024;
    const auto payload = make_pattern(n);
    kioto::flat_buffer buf{n + 64};
    std::size_t final_size = 0;
    std::string content;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, kioto::flat_buffer& b, std::size_t want,
                                           std::size_t& sz, std::string& out) -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        co_await (kioto::async_read(sock, b, want) | as_throwing);
        sz = b.size();
        out = to_string(b.data());
    }(acceptor, buf, n, final_size, content));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer,
                                           std::span<const std::byte> data) -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await (kioto::async_write(sock, data) | as_throwing);
    }(ctx.get_scheduler(), ep, std::span<const std::byte>(payload)));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(final_size == n);
    CHECK(content == to_string(std::span<const std::byte>(payload)));
}

TEST_CASE_TEMPLATE("async_read_until finds a delimiter split across chunk boundaries", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    kioto::flat_buffer buf{4096};
    std::string line;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, kioto::flat_buffer& b, std::string& out)
                                           -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        const auto n = co_await (kioto::async_read_until(sock, b, "\r\n"sv) | as_throwing);
        out = to_string(b.data().first(n));   // n = bytes up to and including the delimiter
    }(acceptor, buf, line));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await (kioto::async_write(sock, kioto::as_bytes("AAAAA\r"sv)) | as_throwing);   // ends on the '\r'
        co_await sched.schedule_after(20ms);                                               // chunk boundary
        co_await (kioto::async_write(sock, kioto::as_bytes("\nBBBBB"sv)) | as_throwing);    // '\n' completes it
        co_await sched.schedule_after(20ms);
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(line == "AAAAA\r\n");
}

TEST_CASE_TEMPLATE("async_read_until with an empty delimiter completes immediately with zero bytes", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    kioto::flat_buffer buf{4096};
    std::size_t n = 999;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, kioto::flat_buffer& b, std::size_t& out)
                                           -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        out = co_await (kioto::async_read_until(sock, b, ""sv) | as_throwing);   // empty delim short-circuits
    }(acceptor, buf, n));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await sched.schedule_after(20ms);
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(n == 0);
}

TEST_CASE_TEMPLATE("async_read_until returns zero when EOF arrives before the delimiter", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    kioto::flat_buffer buf{4096};
    std::size_t n = 999;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, kioto::flat_buffer& b, std::size_t& out)
                                           -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        // EOF before the delimiter surfaces as (error, 0) — capture the pair (don't throw it away)
        auto [ec, rn] = co_await kioto::async_read_until(sock, b, "\r\n"sv);
        static_cast<void>(ec);
        out = rn;
    }(acceptor, buf, n));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await (kioto::async_write(sock, kioto::as_bytes("no-delimiter-here"sv)) | as_throwing);
        // coroutine ends -> close -> EOF reaches read_until before it finds "\r\n"
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(n == 0);
}

TEST_CASE_TEMPLATE("async_read_until reports overflow (not abort) when the buffer fills before the delimiter",
                   Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    kioto::flat_buffer buf{64};   // smaller than the incoming data; delimiter never arrives -> must overflow, not abort
    std::error_code ec;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, kioto::flat_buffer& b, std::error_code& out)
                                           -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        auto [rec, rn] = co_await kioto::async_read_until(sock, b, "\r\n"sv);
        static_cast<void>(rn);
        out = rec;
    }(acceptor, buf, ec));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        std::vector<std::byte> data(200, std::byte{'x'});   // 200 bytes, no "\r\n"
        co_await (kioto::async_write(sock, std::span<const std::byte>(data)) | as_throwing);
        co_await sched.schedule_after(30ms);
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(ec == kioto::error::overflow);
}

TEST_CASE_TEMPLATE("async_read hitting EOF mid-composition returns partial bytes + eof", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    std::vector<std::byte> big(64 * 1024, std::byte{0});
    std::error_code ec;
    std::size_t got = 0;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, std::span<std::byte> out,
                                           std::error_code& e, std::size_t& n) -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        // capture the (ec, n) pair directly: async_read yields set_value(error_code, size_t)
        auto [rec, rn] = co_await kioto::async_read(sock, out);
        e = rec;
        n = rn;
    }(acceptor, std::span(big), ec, got));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        co_await (kioto::async_write(sock, kioto::as_bytes("short-payload"sv)) | as_throwing);
        // coroutine ends -> socket closes -> the server's async_read sees EOF after 13 bytes
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(got == std::string_view("short-payload").size());   // 13 partial bytes delivered
    CHECK(ec == kioto::error::eof);                           // and the EOF surfaced
}
