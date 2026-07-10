// Cross-backend IO error contract: the SAME std::error_code must surface through set_error on every
// backend (epoll/uring/iocp). The TEST_CASE_TEMPLATE instantiation is the contract check — this is
// where the backends most plausibly disagree. Errors surface from a co_awaited op as std::system_error
// whose .code() is the error_code the driver delivered.
#include <chrono>
#include <cstdint>
#include <system_error>
#include <utility>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/base/error.h>
#include <kioto/io/io.h>
#include <kioto/io/pipe.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    // Spawn one task, drive the context to completion, join. The task owns its own error capture.
    template<typename Ctx, typename Task>
    void run_one(Ctx& ctx, Task task) {
        kioto::async_scope scope;
        scope.spawn_on(ctx.get_scheduler(), std::move(task));
        ctx.run();
        kioto::this_thread::sync_wait(scope.join());
    }

    // Bind an ephemeral loopback port, then release it: nothing listens there afterwards.
    template<typename Ctx>
    auto reserve_dead_port(Ctx& ctx) -> std::uint16_t {
        kioto::tcp::acceptor<typename Ctx::scheduler> a{
            ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
        return a.local_endpoint().port();
    }
}

TEST_CASE_TEMPLATE("connect to a port with no listener surfaces connection_refused", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    const kioto::endpoint dead{kioto::ipv4_address::loopback(), reserve_dead_port(ctx)};

    std::error_code ec;
    run_one(ctx, [](typename Ctx::scheduler sched, kioto::endpoint peer, std::error_code& out)
                     -> typename Ctx::template task<> {
        socket_t sock{sched};
        try { co_await sock.async_connect(peer); }
        catch (const std::system_error& e) { out = e.code(); }
    }(ctx.get_scheduler(), dead, ec));

    CHECK(ec == std::errc::connection_refused);
}

TEST_CASE_TEMPLATE("a peer closing the connection surfaces EOF (or reset) on a pending read", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    std::error_code ec;
    bool got_error = false;
    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, std::error_code& out, bool& flagged)
                                            -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        char buf[64];
        try { co_await sock.async_read_some(kioto::as_writable_bytes(buf)); }
        catch (const std::system_error& e) { out = e.code(); flagged = true; }
    }(acceptor, ec, got_error));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);
        // coroutine ends -> sock destroyed -> connection closed under the server's read
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(got_error);
    CHECK((ec == kioto::error::eof or ec == std::errc::connection_reset));
}

TEST_CASE_TEMPLATE("writing to a closed peer surfaces broken_pipe (or reset)", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    std::error_code ec;
    bool got_error = false;
    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, typename Ctx::scheduler sched,
                                           std::error_code& out, bool& flagged)
                                            -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        co_await sched.schedule_after(30ms);   // let the peer's close + RST land first
        char buf[4096] = {};
        try {
            for (int i = 0; i < 20000; ++i) co_await sock.async_write_some(kioto::as_bytes(buf));
        }
        catch (const std::system_error& e) { out = e.code(); flagged = true; }
    }(acceptor, ctx.get_scheduler(), ec, got_error));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t sock{sched};
        co_await sock.async_connect(peer);   // then close immediately
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(got_error);
    CHECK((ec == std::errc::broken_pipe or ec == std::errc::connection_reset));
}

TEST_CASE_TEMPLATE("reading a pipe whose write end was destroyed surfaces EOF", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    Ctx ctx;
    auto [reader, writer] = kioto::make_pipe(ctx.get_scheduler());
    { auto dropped = std::move(writer); }   // close the write end before the read runs

    std::error_code ec;
    bool got_error = false;
    run_one(ctx, [](kioto::pipe_reader<typename Ctx::scheduler> r, std::error_code& out, bool& flagged)
                     -> typename Ctx::template task<> {
        char buf[64];
        try { co_await r.async_read_some(kioto::as_writable_bytes(buf)); }
        catch (const std::system_error& e) { out = e.code(); flagged = true; }
    }(std::move(reader), ec, got_error));

    CHECK(got_error);
    CHECK(ec == kioto::error::eof);
}

TEST_CASE_TEMPLATE("binding to an in-use address surfaces address_in_use", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    Ctx ctx;
    // a1 holds a bound + listening loopback port.
    acceptor_t a1{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const std::uint16_t port = a1.local_endpoint().port();

    // Binding a second socket to the same active port fails synchronously.
    acceptor_t a2{ctx.get_scheduler()};
    a2.open();
    std::error_code ec;
    try { a2.bind(kioto::endpoint{kioto::ipv4_address::loopback(), port}); }
    catch (const std::system_error& e) { ec = e.code(); }

    CHECK(ec == std::errc::address_in_use);
}
