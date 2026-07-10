// Resource-exhaustion / fault-injection: the error paths that only fire under pressure and are otherwise
// never exercised (this is the class the read_until overflow-abort came from). We exhaust the process fd
// table with the OS itself (no shim) and assert that fd-hungry operations surface a clean error_code
// instead of crashing/hanging — AND that the reactor recovers once fds are freed.
//
// POSIX-only (the exhaustion mechanism is dup(2)); on Windows this TU compiles to zero tests.
#include <kioto/base/config.h>

#if KIOTO_OS_LINUX

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>
#include <utility>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/io/file.h>
#include <kioto/io/io.h>
#include <kioto/io/pipe.h>
#include <kioto/net/socket.h>
#include <kioto/net/tcp.h>
#include "io_contexts.h"

using namespace std::chrono_literals;

namespace {
    // Consumes the whole process fd table by dup()-ing until EMFILE, so the NEXT fd allocation fails.
    // Releases everything on destruction. Construct AFTER the runtime/sockets are already open.
    class fd_exhauster {
    public:
        fd_exhauster() {
            for (;;) {
                const int fd = ::dup(0);      // stdin is open in the test binary
                if (fd < 0) break;            // EMFILE -> table full
                fds_.push_back(fd);
            }
        }
        fd_exhauster(const fd_exhauster&) = delete;
        auto operator=(const fd_exhauster&) -> fd_exhauster& = delete;
        ~fd_exhauster() { for (const int fd : fds_) ::close(fd); }

        [[nodiscard]] auto exhausted() const -> bool { return not fds_.empty(); }
        auto release_one() -> void { if (not fds_.empty()) { ::close(fds_.back()); fds_.pop_back(); } }
    private:
        std::vector<int> fds_;
    };

    // Blocking connect to a loopback port: the kernel completes the handshake and queues the connection,
    // so a later accept() has something to dequeue. Returns the client fd (kept alive by the caller).
    auto connect_one(std::uint16_t port) -> int {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        ::sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);
        ::connect(fd, reinterpret_cast<const ::sockaddr*>(&addr), sizeof(addr));
        return fd;
    }
}

TEST_CASE_TEMPLATE("opening a socket out of file descriptors fails cleanly", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    using socket_t = kioto::tcp::socket<typename Ctx::scheduler>;
    Ctx ctx;                       // runtime's own fds are allocated first
    socket_t sock{ctx.get_scheduler()};

    fd_exhauster ex;               // now the table is full
    REQUIRE(ex.exhausted());

    std::error_code ec;
    try { sock.open(); }
    catch (const std::system_error& e) { ec = e.code(); }
    CHECK(ec == std::errc::too_many_files_open);
}

TEST_CASE_TEMPLATE("creating a pipe out of file descriptors fails cleanly", Ctx, KIOTO_TEST_IO_CONTEXTS) {
    Ctx ctx;
    fd_exhauster ex;
    REQUIRE(ex.exhausted());

    std::error_code ec;
    try { auto ends = kioto::make_pipe(ctx.get_scheduler()); static_cast<void>(ends); }
    catch (const std::system_error& e) { ec = e.code(); }
    CHECK(ec == std::errc::too_many_files_open);
}

TEST_CASE_TEMPLATE("opening a file out of file descriptors fails cleanly", Ctx, KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::stream_file<typename Ctx::scheduler>;
    Ctx ctx;
    fd_exhauster ex;
    REQUIRE(ex.exhausted());

    std::error_code ec;
    try { file_t f{ctx.get_scheduler(), "/dev/null", file_t::read_only}; static_cast<void>(f); }
    catch (const std::system_error& e) { ec = e.code(); }
    CHECK(ec == std::errc::too_many_files_open);
}

TEST_CASE_TEMPLATE("an accept that runs out of fds errors cleanly and the acceptor recovers", Ctx,
                   KIOTO_TEST_IO_CONTEXTS) {
    using acceptor_t = kioto::tcp::acceptor<typename Ctx::scheduler>;
    Ctx ctx;
    acceptor_t acceptor{ctx.get_scheduler(), kioto::endpoint{kioto::ipv4_address::loopback(), 0}};
    const auto port = acceptor.local_endpoint().port();

    const int client = connect_one(port);   // one connection queued (created before exhausting)
    fd_exhauster ex;
    REQUIRE(ex.exhausted());

    kioto::async_scope scope;
    std::error_code ec;
    bool recovered = false;

    scope.spawn_on(ctx.get_scheduler(), [](acceptor_t& acc, fd_exhauster& ex, std::error_code& out, bool& rec)
                                           -> typename Ctx::template task<> {
        // out of fds: the accept must complete with too_many_files_open, not spin or crash
        try { auto sock = co_await acc.async_accept(); static_cast<void>(sock); }
        catch (const std::system_error& e) { out = e.code(); }
        // free a descriptor; the still-queued connection must now accept successfully -> reactor recovered
        ex.release_one();
        auto sock = co_await acc.async_accept();
        rec = sock.is_open();
    }(acceptor, ex, ec, recovered));

    ctx.run();
    kioto::this_thread::sync_wait(scope.join());
    ::close(client);

    CHECK(ec == std::errc::too_many_files_open);
    CHECK(recovered);
}

#endif // KIOTO_OS_LINUX
