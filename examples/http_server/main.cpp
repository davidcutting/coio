#include <filesystem>
#include <coio/runtime.h>
#include <coio/utils/signal_wait.h>
#include "connection.h"
#include "define.h"
#include "router.h"
#include "../common.h"

// Set this to the source directory containing static files
#ifndef HTTP_SERVER_STATIC_DIR
#define HTTP_SERVER_STATIC_DIR "static"
#endif

constexpr std::uint16_t port = 8080;

// A homogeneous pool of io_executor worker threads — this is coio's runtime layer; the old hand-rolled
// io_context_pool was exactly this. pick_scheduler() hands back a worker's own scheduler so each accepted
// connection is created on, and handled by, the same worker (the socket is context-affine).
using server_runtime = coio::basic_runtime<http::io_executor>;

auto signal_watchdog(coio::async_scope& scope) -> coio::inline_task<> {
    const int signum = co_await coio::signal_wait(SIGINT, SIGTERM);
    ::println("server stop with signal: ({}){}", signum, coio::strsignal(signum));
    // Cancel the acceptor + every in-flight connection; the runtime stops/joins its worker threads at
    // teardown (its destructor), so we don't touch worker lifecycle from here.
    scope.request_stop();
}

auto start_server(server_runtime& pool, coio::async_scope& scope, http::router& router) -> http::io_executor::task<> try {
    http::tcp_acceptor acceptor(co_await coio::read_scheduler());
    acceptor.open(coio::tcp::v6());
    acceptor.set_option(http::tcp_acceptor::reuse_address(true));
    acceptor.set_option(http::tcp_acceptor::v6_only(false));
    acceptor.bind({coio::ipv6_address::any(), port});
    acceptor.listen();
    ::debug("server started at http://localhost:{}", port);
    while (true) {
        auto next_scheduler = pool.pick_scheduler();
        http::tcp_socket socket = co_await acceptor.async_accept(next_scheduler);
        auto endpoint = socket.remote_endpoint();
        scope.spawn_on(
            next_scheduler,
            http::connection(
                std::move(socket),
                endpoint,
                router
            )
        );
    }
}
catch (const std::exception& e) {
    ::debug("acceptor error: {}", e.what());
}

auto main() -> int try {
    http::router router{HTTP_SERVER_STATIC_DIR};
    server_runtime pool{4};
    coio::async_scope scope;
    scope.spawn(signal_watchdog(scope));
    scope.spawn_on(pool.pick_scheduler(), start_server(pool, scope, router));
    coio::this_thread::sync_wait(scope.join());
}
catch (const std::exception& e) {
    ::debug("[FATAL] {}", e.what());
    return EXIT_FAILURE;
}
