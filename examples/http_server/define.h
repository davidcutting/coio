#pragma once
#include <coio/net/tcp.h>

#if COIO_OS_LINUX
#include <coio/asyncio/epoll_context.h>
namespace http {
    using io_executor = coio::epoll_context;
}
#elif COIO_OS_WINDOWS
#include <coio/asyncio/iocp_context.h>
namespace http {
    using io_executor = coio::iocp_context;
}
#endif

namespace http {
    using tcp_socket = coio::tcp::socket<io_executor::scheduler>;
    using tcp_acceptor = coio::tcp::acceptor<io_executor::scheduler>;
    using tcp_resolver = coio::tcp::resolver<io_executor::scheduler>;
}
