#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
#include <kioto/base/flat_buffer.h>
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

auto handle_connection() -> io_context::task<> try {
    io_context::scheduler sched = co_await kioto::read_scheduler();
    tcp_socket socket{sched};
    co_await socket.async_connect({kioto::ipv4_address::loopback(), 8086});
    ::println("local endpoint: {}", socket.local_endpoint());
    ::println("input messages to send to echo server (type 'exit' or 'quit' to quit):");
    while (true) {
        ::print(">> ");
        std::string content;
        std::getline(std::cin, content);
        if (content.empty()) continue;
        if (content == "exit" or content == "quit") break;
        co_await (kioto::async_write(socket, kioto::as_bytes(content)) | as_throwing);
        std::size_t content_length = content.size();
        kioto::flat_buffer buffer;
        co_await (kioto::async_read(socket, buffer, content_length) | as_throwing);
        const auto buffer_data = buffer.data();
        ::println("-- {}", std::string_view{reinterpret_cast<const char*>(buffer_data.data()), buffer_data.size()});
        buffer.consume(content_length);
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
