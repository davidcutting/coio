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

using random_access_file = kioto::random_access_file<io_context::scheduler>;

constexpr std::size_t block_size = 1024;

auto async_copy_file(kioto::zstring_view src, kioto::zstring_view dst) -> io_context::task<> {
    auto scheduler = co_await kioto::execution::read_env(kioto::execution::get_scheduler);
    random_access_file src_file{
        scheduler,
        src,
        random_access_file::read_only
    };

    random_access_file dst_file{
        scheduler,
        dst,
        random_access_file::write_only | random_access_file::create | random_access_file::truncate
    };

    const std::size_t total_size = src_file.size();
    dst_file.resize(total_size);

    try {
        std::byte buffer[block_size];
        std::size_t offset = 0;
        while (offset < total_size) {
            const auto n = co_await src_file.async_read_some_at(offset, kioto::as_writable_bytes(buffer));
            co_await kioto::async_write_at(dst_file, offset, kioto::as_bytes(buffer, n));
            offset += n;
        }
    }
    catch (const std::system_error& e) {
        if (e.code() != kioto::error::eof) throw;
    }

    dst_file.sync_all();
}

auto main(int argc, char** argv) -> int try {
    if (argc != 3) {
        ::println("  {} <src> <dst>", argv[0]);
        return EXIT_FAILURE;
    }

    io_context context;
    kioto::this_thread::sync_wait(kioto::when_all(
        kioto::starts_on(context.get_scheduler(), async_copy_file(argv[1], argv[2])),
        [&]() -> kioto::task<> {
            context.run();
            co_return;
        }()
    ));
}
catch (const std::exception& e) {
    ::println("[FATAL] {}", e.what());
    return EXIT_FAILURE;
}
