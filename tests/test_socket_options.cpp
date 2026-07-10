// Socket options, shutdown, and endpoint accessors — the synchronous syscall wrappers in socket.*.cpp
// that the async tests never touch. Templated over backends (the wrappers are shared, but the platform
// syscall layer differs).
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
#include <kioto/net/udp.h>
#include "io_contexts.h"

TEST_CASE_TEMPLATE("socket-level options round-trip through set_option/get_option", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using socket_t = kioto::udp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    socket_t sock{ctx.get_scheduler(), kioto::udp::v4()};

    SUBCASE("reuse_address (bool)") {
        sock.set_option(typename socket_t::reuse_address{true});
        typename socket_t::reuse_address got{false};
        sock.get_option(got);
        CHECK(got.get());
    }
    SUBCASE("broadcast (bool)") {
        sock.set_option(typename socket_t::broadcast{true});
        typename socket_t::broadcast got{false};
        sock.get_option(got);
        CHECK(got.get());
    }
    SUBCASE("receive_buffer_size (int)") {
        sock.set_option(typename socket_t::receive_buffer_size{1 << 20});
        typename socket_t::receive_buffer_size got{0};
        sock.get_option(got);
        CHECK(got.get() > 0);   // kernel may round/double; just confirm it took
    }
    SUBCASE("send_buffer_size (int)") {
        sock.set_option(typename socket_t::send_buffer_size{1 << 20});
        typename socket_t::send_buffer_size got{0};
        sock.get_option(got);
        CHECK(got.get() > 0);
    }
}

TEST_CASE_TEMPLATE("tcp no_delay option round-trips", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    socket_t sock{ctx.get_scheduler()};
    sock.open();

    sock.set_option(kioto::tcp::no_delay{true});
    kioto::tcp::no_delay got{false};
    sock.get_option(got);
    CHECK(got.get());
}

TEST_CASE_TEMPLATE("a connected socket reports endpoints and shuts down cleanly", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const kioto::endpoint ep = acceptor.local_endpoint();
    kioto::async_scope scope;

    bool endpoints_ok = false;
    bool shutdown_ok = false;
    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, kioto::endpoint listen_ep,
                                           bool& eok, bool& sok) -> typename Ctx::template task<> {
        auto sock = co_await acc.async_accept();
        const auto local = sock.local_endpoint();
        const auto remote = sock.remote_endpoint();
        // the server side's local port is the listener's; both ends are on loopback
        eok = local.port() == listen_ep.port()
              and local.ip().is_v4() and remote.ip().is_v4();
        try { sock.shutdown(socket_t::shutdown_both); sok = true; }
        catch (...) { sok = false; }
    }(acceptor, ep, endpoints_ok, shutdown_ok));

    scope.spawn_on(ctx.get_scheduler(), [](typename Ctx::scheduler sched, kioto::endpoint peer)
                                           -> typename Ctx::template task<> {
        socket_t client{sched};
        co_await client.async_connect(peer);
        co_await sched.schedule_after(std::chrono::milliseconds{20});
    }(ctx.get_scheduler(), ep));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(endpoints_ok);
    CHECK(shutdown_ok);
}
