// UDP datagram send/recv, templated over backends. Previously UDP was reached only through uring
// multishot (async_receive_sequence); the plain single-datagram path had no test. Also pins the
// unstoppable-send policy: a datagram send is prompt+atomic, so a stop request must NOT drop its
// completion (async_datagram_send_t declares itself unstoppable; schedule_io skips the stop hook).
#include <cstddef>
#include <string>
#include <string_view>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/udp.h>
#include "io_contexts.h"

using namespace std::string_view_literals;

TEST_CASE_TEMPLATE("connected udp send/receive delivers a datagram", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using udp_socket = kioto::udp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    udp_socket a{ctx.get_scheduler(), kioto::udp::v4()};
    udp_socket b{ctx.get_scheduler(), kioto::udp::v4()};
    a.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    b.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    a.connect(b.local_endpoint());
    b.connect(a.local_endpoint());

    kioto::async_scope scope;
    constexpr std::string_view payload = "udp-datagram";
    std::string received;

    scope.spawn_on(ctx.get_scheduler(), [](udp_socket& b, std::string& out)
                                           -> typename Ctx::template task<> {
        char buf[64];
        const auto n = co_await b.async_receive(kioto::as_writable_bytes(buf));
        out.assign(buf, n);
    }(b, received));

    scope.spawn_on(ctx.get_scheduler(), [](udp_socket& a, std::string_view p)
                                           -> typename Ctx::template task<> {
        const auto n = co_await a.async_send(kioto::as_bytes(p));
        CHECK(n == p.size());
    }(a, payload));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(received == payload);
}

TEST_CASE_TEMPLATE("an unstoppable datagram send completes despite an active stop request", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using udp_socket = kioto::udp::socket<typename Ctx::scheduler>;
    Ctx ctx;
    udp_socket a{ctx.get_scheduler(), kioto::udp::v4()};
    udp_socket b{ctx.get_scheduler(), kioto::udp::v4()};
    a.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    b.bind(kioto::endpoint{kioto::ipv4_address::loopback(), 0});
    a.connect(b.local_endpoint());
    b.connect(a.local_endpoint());

    kioto::inplace_stop_source stop;
    stop.request_stop();   // stop is ALREADY requested before the send even starts

    kioto::async_scope scope;
    constexpr std::string_view payload = "unstoppable";
    std::string received;
    bool value_delivered = false;

    scope.spawn_on(ctx.get_scheduler(), [](udp_socket& b, std::string& out)
                                           -> typename Ctx::template task<> {
        char buf[64];
        const auto n = co_await b.async_receive(kioto::as_writable_bytes(buf));
        out.assign(buf, n);
    }(b, received));

    scope.spawn_on(ctx.get_scheduler(), [](udp_socket& a, kioto::inplace_stop_token tok,
                                           std::string_view p, bool& delivered)
                                           -> typename Ctx::template task<> {
        // A stoppable op under an already-stopped token completes set_stopped (empty optional). The
        // datagram send is unstoppable, so it ignores the stop and completes set_value(size).
        auto r = co_await kioto::execution::stopped_as_optional(
            kioto::stop_when(a.async_send(kioto::as_bytes(p)), tok));
        delivered = r.has_value() and *r == p.size();
    }(a, stop.get_token(), payload, value_delivered));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());

    CHECK(value_delivered);       // the send was NOT dropped by the stop
    CHECK(received == payload);   // and the datagram actually went out
}
