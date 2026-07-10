#include <thread>
#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/io/pipe.h>
#include "common.h"

#if KIOTO_OS_LINUX
#include <kioto/io/driver/epoll_context.h>
using io_context = kioto::epoll_context;
#elif KIOTO_OS_WINDOWS
#include <kioto/io/driver/iocp_context.h>
using io_context = kioto::iocp_context;
#endif

auto main() -> int {
    io_context context;
    auto [reader, writer] = kioto::make_pipe(context.get_scheduler());
    kioto::async_scope scope;
    scope.spawn([](kioto::pipe_reader<io_context::scheduler> r) -> kioto::task<> {
        try {
            char buffer[128];
            while (true) {
                auto n = co_await r.async_read_some(kioto::as_writable_bytes(buffer));
                std::string_view message{buffer, n};
                std::clog << message;
                if (message.ends_with('\n')) break;
            }
        }
        catch (std::system_error& e) {
            if (e.code() != kioto::error::eof) {
                ::println("connection broken because of {}", e.what());
            }
        }
    }(std::move(reader)));

    scope.spawn([](kioto::pipe_writer<io_context::scheduler> w) -> kioto::task<> {
        std::string_view messages[]{
          "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
          "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
          "Ut enim ad minim veniam, quis nostrud exercitation ullamco",
          "laboris nisi ut aliquip ex ea commodo consequat.",
          "Duis aute irure dolor in reprehenderit in voluptate velit esse",
          "cillum dolore eu fugiat nulla pariatur.",
          "Excepteur sint occaecat cupidatat non proident",
          "sunt in culpa qui officia deserunt mollit anim id est laborum.",
          "\n"
        };
        for (std::string_view message : messages) {
            co_await (kioto::async_write(w, kioto::as_bytes(message)) | as_throwing);
        }
    }(std::move(writer)));

    context.run();

    kioto::this_thread::sync_wait(scope.join());
}
