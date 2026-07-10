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

class io_context_pool {
public:
    explicit io_context_pool(std::size_t count) {
        KIOTO_ASSERT(count > 0);
        io_contexts_.reserve(count);
        work_guards_.reserve(count);
        threads_.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            work_guards_.emplace_back(*io_contexts_.emplace_back(std::make_unique<io_context>()));
        }

        for (std::size_t i = 0; i < count; ++i) {
            threads_.emplace_back([this, i] {
                ::debug("worker started");
                io_contexts_[i]->run();
                ::debug("worker finished");
            });
        }
    }

    io_context_pool(const io_context_pool&) = delete;

    io_context_pool& operator= (const io_context_pool&) = delete;

    ~io_context_pool() {
        stop();
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    auto stop() -> void {
        for (auto& ctx : io_contexts_) {
            ctx->request_stop();
        }
        work_guards_.clear();
    }

    auto pick_scheduler() noexcept -> io_context::scheduler {
        return io_contexts_[std::exchange(next_, (next_ + 1) % io_contexts_.size())]->get_scheduler();
    }

private:
    std::size_t next_ = 0;
    std::vector<std::unique_ptr<io_context>> io_contexts_;
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

auto start_server(io_context_pool& pool, kioto::async_scope& scope) -> io_context::task<> try {
    tcp_acceptor acceptor{co_await kioto::read_scheduler(), kioto::endpoint{kioto::ipv4_address::any(), 8086}};
    ::debug("server \"{}\" start...", acceptor.local_endpoint());
    while (true) {
        auto next_scheduler = pool.pick_scheduler();
        scope.spawn_on(next_scheduler, handle_connection(co_await acceptor.async_accept(next_scheduler)));
    }
}
catch (const std::system_error& e) {
    ::debug("acceptor error: {}", e.what());
}

auto signal_watchdog(io_context_pool& pool) -> kioto::inline_task<> {
    const int signum = co_await kioto::signal_wait(SIGINT, SIGTERM);
    ::debug("server stop with signal: ({}){}", signum, kioto::strsignal(signum));
    pool.stop();
}

auto main() -> int {
    io_context_pool pool{4};
    kioto::async_scope scope;
    scope.spawn(signal_watchdog(pool));
    scope.spawn_on(pool.pick_scheduler(), start_server(pool, scope));
    kioto::this_thread::sync_wait(scope.join());
}
