// Name resolution (resolver.*.cpp getaddrinfo path), previously untested. Uses a NUMERIC address +
// service so it's a pure local parse — no DNS/network dependency, safe in a sandbox/CI.
#include <string>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/net/resolver.h>
#include <kioto/net/tcp.h>
#include <kioto/net/udp.h>
#include "io_contexts.h"

TEST_CASE("tcp resolver resolves a numeric loopback address:port") {
    using resolver_t = kioto::tcp::resolver<kioto_test::default_io_context::scheduler>;
    resolver_t::query_t query;
    query.host_name = "127.0.0.1";
    query.service_name = "80";
    query.flags = resolver_t::query_t::numeric_host | resolver_t::query_t::numeric_service;

    int count = 0;
    kioto::endpoint found;
    for (auto&& result : resolver_t::resolve(query)) {
        found = result.endpoint;
        ++count;
    }

    CHECK(count >= 1);
    CHECK(found.ip().is_v4());
    CHECK(found.ip().v4() == kioto::ipv4_address::loopback());
    CHECK(found.port() == 80);
}

TEST_CASE("udp resolver resolves a numeric loopback address:port") {
    using resolver_t = kioto::udp::resolver<kioto_test::default_io_context::scheduler>;
    resolver_t::query_t query;
    query.host_name = "127.0.0.1";
    query.service_name = "53";
    query.flags = resolver_t::query_t::numeric_host | resolver_t::query_t::numeric_service;

    int count = 0;
    kioto::endpoint found;
    for (auto&& result : resolver_t::resolve(query)) {
        found = result.endpoint;
        ++count;
    }

    CHECK(count >= 1);
    CHECK(found.port() == 53);
}
