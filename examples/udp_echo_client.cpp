#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/udp.h>
#include "common.h"

#if KIOTO_OS_LINUX
#include <kioto/io/driver/epoll_context.h>
using io_context = kioto::epoll_context;
#elif KIOTO_OS_WINDOWS
#include <kioto/io/driver/iocp_context.h>
using io_context = kioto::iocp_context;
#endif

using udp_socket = kioto::udp::socket<io_context::scheduler>;

auto handle_connection() -> io_context::task<> try {
    io_context::scheduler sched = co_await kioto::read_scheduler();
    udp_socket socket{sched, kioto::udp::v4()};
    socket.bind(kioto::endpoint{kioto::ipv4_address::any(), 0});

    const kioto::endpoint remote_endpoint{kioto::ipv4_address::loopback(), 8087};

    ::println("local endpoint: {}", socket.local_endpoint());
    ::println("input messages to send to echo server (type 'exit' or 'quit' to quit):");

    char buffer[1024];
    while (true) {
        ::print(">> ");
        std::string content;
        std::getline(std::cin, content);

        if (content.empty()) continue;
        if (content == "exit" or content == "quit") break;

        co_await socket.async_send_to(kioto::as_bytes(content), remote_endpoint);
        const auto [_, length] = co_await socket.async_receive_from(kioto::as_writable_bytes(buffer));
        ::println("-- {}", std::string_view{buffer, length});
    }
}
catch (const std::exception& e) {
    println("error: {}", e.what());
}

auto main() -> int {
    io_context context;
    kioto::async_scope scope;
    scope.spawn_on(context.get_scheduler(), handle_connection());
    context.run();
    kioto::this_thread::sync_wait(scope.join());
}
