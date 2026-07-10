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

class thread_pool {
public:
    explicit thread_pool(std::size_t thread_count) {
        work_guards_.reserve(thread_count);
        threads_.reserve(thread_count);
        for (std::size_t i = 0; i < thread_count; ++i) {
            work_guards_.emplace_back(context_);
        }
        for (std::size_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this] {
                ::debug("worker started");
                context_.run();
                ::debug("worker finished");
            });
        }
    }

    ~thread_pool() {
        stop();
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    auto stop() -> void {
        context_.request_stop();
        work_guards_.clear();
    }

    auto get_scheduler() noexcept -> io_context::scheduler {
        return context_.get_scheduler();
    }

private:
    io_context context_;
    std::vector<std::thread> threads_;
    std::vector<kioto::work_guard<io_context>> work_guards_;
};

auto handle_connection(tcp_socket socket) -> io_context::task<> {
    auto remote_endpoint = socket.remote_endpoint();
    ::debug("new connection from [{}]", remote_endpoint);
    try {
        char buffer[1024];
        while (true) {
            const auto length = co_await socket.async_read_some(kioto::as_writable_bytes(buffer));
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
    ::debug("acceptor error: {}", e.what());
}

auto signal_watchdog(thread_pool& pool) -> kioto::inline_task<> {
    const int signum = co_await kioto::signal_wait(SIGINT, SIGTERM);
    ::debug("server stop with signal: ({}){}", signum, kioto::strsignal(signum));
    pool.stop();
}

auto main() -> int {
    using namespace std::chrono_literals;
    thread_pool pool{4};
    kioto::async_scope scope;
    scope.spawn(signal_watchdog(pool));
    scope.spawn_on(pool.get_scheduler(), start_server(scope));
    kioto::this_thread::sync_wait(scope.join());
}
