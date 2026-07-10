#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
#include <kioto/base/signal_wait.h>
#include "common.h"

#if KIOTO_OS_LINUX
#include <kioto/io/driver/epoll_context.h>
using io_context = kioto::epoll_context;
#elif KIOTO_OS_WINDOWS
#include <kioto/io/driver/iocp_context.h>
using io_context = kioto::iocp_context;
#endif

using tcp_socket = kioto::tcp::socket<io_context::scheduler>;
using tcp_acceptor = kioto::tcp::acceptor<io_context::scheduler>;

auto with_timeout(kioto::execution::sender auto sndr, io_context::scheduler sched, std::chrono::milliseconds ms) {
    return kioto::when_any(
        std::move(sndr),
        sched.schedule_after(ms) | kioto::let_value([]() noexcept { return kioto::just_stopped(); })
    );
}

auto handle_connection(tcp_socket socket) -> io_context::task<> {
    using namespace std::chrono_literals;
    auto remote_endpoint = socket.remote_endpoint();
    ::debug("new connection from [{}]", remote_endpoint);
    io_context::scheduler sched = co_await kioto::read_scheduler();
    try {
        char buffer[1024];
        while (true) {
            const auto length = co_await with_timeout(
                socket.async_read_some(kioto::as_writable_bytes(buffer)),
                sched,
                3s
            );
            ::debug("{}", std::string_view{buffer, length});
            co_await (kioto::async_write(socket, kioto::as_bytes(buffer, length)) | as_throwing);
        }
    }
    catch (const std::system_error& e) {
        ::debug("connection with [{}] broken because \"{}\"", remote_endpoint, e.what());
    }
}

auto start_server(kioto::async_scope& scope) -> io_context::task<> try {
    io_context::scheduler sched = co_await kioto::read_scheduler();
    tcp_acceptor acceptor{sched, kioto::endpoint{kioto::ipv4_address::any(), 8086}};
    ::debug("server \"{}\" start...", acceptor.local_endpoint());
    while (true) {
        scope.spawn_on(sched, handle_connection(co_await acceptor.async_accept()));
    }
}
catch (const std::system_error& e) {
    ::println("acceptor error: {}", e.what());
}

auto signal_watchdog(io_context& context) -> kioto::inline_task<> {
    const int signum = co_await kioto::signal_wait(SIGINT, SIGTERM);
    ::debug("server stop with signal: ({}){}", signum, kioto::strsignal(signum));
    context.request_stop();
}

auto main() -> int {
    io_context context;
    kioto::async_scope scope;
    scope.spawn_on(context.get_scheduler(), start_server(scope));
    scope.spawn(signal_watchdog(context));
    context.run();
    kioto::this_thread::sync_wait(scope.join());
}
