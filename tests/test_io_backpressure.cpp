// Partial I/O and send-buffer backpressure — untested until now (every other test uses payloads small
// enough to complete in one syscall). Small SO_SNDBUF/SO_RCVBUF + a large payload forces write_some to
// partially complete repeatedly. Also pins the STREAM-send stoppability: a stream send blocked on a full
// buffer IS cancellable (the direct contrast to the unstoppable datagram send in test_io_udp).
#include <chrono>
#include <cstddef>
#include <span>
#include <vector>
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

    auto make_pattern(std::size_t n) -> std::vector<std::byte> {
        std::vector<std::byte> v(n);
        for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<std::byte>('A' + (i % 26));
        return v;
    }
}

TEST_CASE_TEMPLATE("async_write drains a payload far larger than the send buffer (partial writes loop)", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    constexpr std::size_t total = 512 * 1024;   // >> the 8 KiB buffers -> dozens of partial write_somes
    const auto payload = make_pattern(total);
    std::vector<std::byte> got(total, std::byte{0});
    std::size_t nread = 0;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, std::span<std::byte> out, std::size_t& nr)
                                           -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        sock.set_option(typename socket_t::receive_buffer_size{8192});
        nr = co_await (kioto::async_read(sock, out) | as_throwing);
    }(acceptor, std::span(got), nread));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer,
                                           std::span<const std::byte> data) -> typename Ctx::template task<> {
        socket_t sock{sched};
        sock.open();
        sock.set_option(typename socket_t::send_buffer_size{8192});
        co_await sock.async_connect(peer);
        co_await (kioto::async_write(sock, data) | as_throwing);
    }(ctx.get_scheduler(), ep, std::span<const std::byte>(payload)));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(nread == total);
    CHECK(got == payload);
}

TEST_CASE_TEMPLATE("a stream send blocked on a full buffer is cancellable", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    bool cancelled = false;

    // Server accepts, shrinks its receive buffer, then NEVER reads — so the connection stays full.
    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, typename Ctx::scheduler sched)
                                           -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        sock.set_option(typename socket_t::receive_buffer_size{4096});
        co_await sched.schedule_after(120ms);   // hold the connection open, draining nothing
    }(acceptor, ctx.get_scheduler()));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer, bool& out)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        sock.open();
        sock.set_option(typename socket_t::send_buffer_size{4096});
        co_await sock.async_connect(peer);

        // A 4 MiB write with nobody reading fills both socket buffers, then blocks inside a write_some.
        // The timeout cancels the blocked send. async_write yields (ec, n), so map it to a plain value
        // completion and observe the cancellation via upon_stopped.
        std::vector<std::byte> huge(4 * 1024 * 1024, std::byte{0x5a});
        co_await (with_timeout(kioto::async_write(sock, std::span<const std::byte>(huge))
                                   | kioto::then([](std::error_code, std::size_t) noexcept {}),
                               sched, 40ms)
                  | kioto::upon_stopped([&out]() noexcept { out = true; }));
    }(ctx.get_scheduler(), ep, cancelled));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(cancelled);
}
