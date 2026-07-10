#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/udp.h>
#include <kioto/base/signal_wait.h>
#include "common.h"

#if KIOTO_OS_LINUX
#include <kioto/io/driver/epoll_context.h>
using io_context = kioto::epoll_context;
#elif KIOTO_OS_WINDOWS
#include <kioto/io/driver/iocp_context.h>
using io_context = kioto::iocp_context;
#endif

using udp_socket = kioto::udp::socket<io_context::scheduler>;

auto start_server() -> io_context::task<> try {
    io_context::scheduler sched = co_await kioto::read_scheduler();
    udp_socket socket{sched, kioto::udp::v4()};
    socket.bind(kioto::endpoint{kioto::ipv4_address::any(), 8087});

    ::debug("UDP echo server \"{}\" started...", socket.local_endpoint());

    char buffer[1024];
    while (true) {
        const auto [remote_endpoint, length] = co_await socket.async_receive_from(kioto::as_writable_bytes(buffer));
        co_await socket.async_send_to(kioto::as_bytes(buffer, length), remote_endpoint);
    }
}
catch (const std::system_error& e) {
    ::println("server error: {}", e.what());
}

auto signal_watchdog(io_context& context) -> kioto::inline_task<> {
    const int signum = co_await kioto::signal_wait(SIGINT, SIGTERM);
    ::debug("server stop with signal: ({}){}", signum, kioto::strsignal(signum));
    context.request_stop();
}

auto main() -> int {
    io_context context;
    kioto::async_scope scope;
    scope.spawn_on(context.get_scheduler(), start_server());
    scope.spawn(signal_watchdog(context));
    context.run();
    kioto::this_thread::sync_wait(scope.join());
}

