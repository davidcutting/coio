#pragma once
#include <kioto/net/tcp.h>

#if KIOTO_OS_LINUX
#include <kioto/io/driver/epoll_context.h>
namespace http {
    using io_executor = kioto::epoll_context;
}
#elif KIOTO_OS_WINDOWS
#include <kioto/io/driver/iocp_context.h>
namespace http {
    using io_executor = kioto::iocp_context;
}
#endif

namespace http {
    using tcp_socket = kioto::tcp::socket<io_executor::scheduler>;
    using tcp_acceptor = kioto::tcp::acceptor<io_executor::scheduler>;
    using tcp_resolver = kioto::tcp::resolver<io_executor::scheduler>;
}
