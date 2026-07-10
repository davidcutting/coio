#include <kioto/core.h>
#include <kioto/io/io.h>
#include <kioto/io/file.h>
#include "common.h"

#if KIOTO_OS_LINUX
#include <kioto/io/driver/uring_context.h>
using io_context = kioto::uring_context;
#elif KIOTO_OS_WINDOWS
#include <kioto/io/driver/iocp_context.h>
using io_context = kioto::iocp_context;
#endif
using stream_file = kioto::stream_file<io_context::scheduler>;

auto cat(kioto::zstring_view path) -> io_context::task<> {
    try {
        stream_file file{co_await kioto::execution::read_env(kioto::execution::get_scheduler), path, stream_file::read_only};
        ::println("this file has {} byte(s)", file.size());
        char buffer[1024];
        while (true) {
            const auto n = co_await file.async_read_some(kioto::as_writable_bytes(buffer));
            ::print("{}", std::string_view{buffer, n});
        }
    }
    catch (std::system_error& e) {
        if (e.code() == kioto::error::eof) {
            co_return;
        }
        ::println("[FATAL] {}", e.what());
    }
    catch (std::exception& e) {
        ::println("[FATAL] {}", e.what());
    }
}

auto main(int argc, char** argv) -> int {
    if (argc != 2) {
        ::println("Usage: {} <file-path>", argv[0]);
        return EXIT_FAILURE;
    }
    io_context context;
    kioto::this_thread::sync_wait(kioto::when_all(
        kioto::starts_on(context.get_scheduler(), cat(argv[1])),
        [&]() -> kioto::task<> {
            context.run(); co_return;
        }()
    ));
}
