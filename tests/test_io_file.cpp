// Regular-file I/O — previously untested (only the manual cat/cp examples). File-capable backends only
// (capability::file: io_uring / IOCP, not epoll). The sequential-offset test in particular exercises
// iocp's transform_sexpr position tracking, which Windows CI validates.
#include <atomic>
#include <cstddef>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/core.h>
#include <kioto/base/error.h>
#include <kioto/base/flat_buffer.h>
#include <kioto/io/file.h>
#include <kioto/io/io.h>
#include "io_contexts.h"

using namespace std::string_view_literals;

namespace {
    KIOTO_ALWAYS_INLINE auto dispatch_result(std::error_code ec, std::size_t n) noexcept {
        kioto::async_result<kioto::execution::set_value_t(std::size_t), kioto::execution::set_error_t(std::error_code)> r;
        if (ec) { if (ec == std::errc::operation_canceled) r.set_stopped(); else r.set_error(ec); }
        else r.set_value(n);
        return r;
    }
    inline const auto as_throwing = kioto::execution::let_value(dispatch_result);

    auto make_pattern(std::size_t n) -> std::vector<std::byte> {
        std::vector<std::byte> v(n);
        for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<std::byte>('A' + (i % 26));
        return v;
    }

    template<typename Ctx, typename Task>
    void run_one(Ctx& ctx, Task task) {
        kioto::async_scope scope;
        scope.spawn_on(ctx.get_scheduler(), std::move(task));
        ctx.run();
        kioto::this_thread::sync_wait(scope.join());
    }

    // A unique scratch path in the system temp dir, removed on scope exit.
    struct scratch_file {
        std::filesystem::path path;
        scratch_file() {
            static std::atomic<unsigned> counter{0};
            path = std::filesystem::temp_directory_path() /
                   ("kioto_test_" + std::to_string(counter.fetch_add(1)) + ".tmp");
            std::filesystem::remove(path);
        }
        ~scratch_file() { std::error_code ec; std::filesystem::remove(path, ec); }
        [[nodiscard]] auto str() const -> std::string { return path.string(); }
    };
}

TEST_CASE_TEMPLATE("stream_file write/close/reopen/read roundtrip", Ctx, KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::stream_file<typename Ctx::scheduler>;
    Ctx ctx;
    scratch_file scratch;
    constexpr std::string_view payload = "hello-regular-file";

    {   // write phase
        file_t f{ctx.get_scheduler(), scratch.str(), file_t::write_only | file_t::create | file_t::truncate};
        run_one(ctx, [](file_t& f, std::string_view p) -> typename Ctx::template task<> {
            const auto n = co_await f.async_write_some(kioto::as_bytes(p));
            CHECK(n == p.size());
        }(f, payload));
    }

    std::string got;
    {   // read phase (fresh open)
        file_t f{ctx.get_scheduler(), scratch.str(), file_t::read_only};
        run_one(ctx, [](file_t& f, std::string& out) -> typename Ctx::template task<> {
            char buf[64];
            const auto n = co_await f.async_read_some(kioto::as_writable_bytes(buf));
            out.assign(buf, n);
        }(f, got));
    }

    CHECK(got == payload);
}

TEST_CASE_TEMPLATE("random_access_file positional read_at/write_at hit the right offsets", Ctx,
                   KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::random_access_file<typename Ctx::scheduler>;
    Ctx ctx;
    scratch_file scratch;

    file_t f{ctx.get_scheduler(), scratch.str(), file_t::read_write | file_t::create | file_t::truncate};
    std::string at_100, at_0;
    run_one(ctx, [](file_t& f, std::string& a100, std::string& a0) -> typename Ctx::template task<> {
        co_await f.async_write_some_at(0, kioto::as_bytes("AAAA"sv));
        co_await f.async_write_some_at(100, kioto::as_bytes("BBBB"sv));
        char b[4];
        auto n = co_await f.async_read_some_at(100, kioto::as_writable_bytes(b));
        a100.assign(b, n);
        n = co_await f.async_read_some_at(0, kioto::as_writable_bytes(b));
        a0.assign(b, n);
    }(f, at_100, at_0));

    CHECK(at_100 == "BBBB");
    CHECK(at_0 == "AAAA");
}

TEST_CASE_TEMPLATE("stream_file sequential read_some advances the position across chunks", Ctx,
                   KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::stream_file<typename Ctx::scheduler>;
    Ctx ctx;
    scratch_file scratch;

    std::string payload;
    for (int i = 0; i < 300; ++i) payload.push_back(static_cast<char>('a' + (i % 26)));

    {
        file_t f{ctx.get_scheduler(), scratch.str(), file_t::write_only | file_t::create | file_t::truncate};
        run_one(ctx, [](file_t& f, const std::string& p) -> typename Ctx::template task<> {
            std::size_t off = 0;
            while (off < p.size()) off += co_await f.async_write_some(kioto::as_bytes(std::string_view{p}.substr(off)));
        }(f, payload));
    }

    std::string got;
    {
        file_t f{ctx.get_scheduler(), scratch.str(), file_t::read_only};
        run_one(ctx, [](file_t& f, std::string& out) -> typename Ctx::template task<> {
            char buf[64];   // smaller than the file -> forces multiple position-advancing reads
            for (;;) {
                std::size_t n = 0;
                try { n = co_await f.async_read_some(kioto::as_writable_bytes(buf)); }
                catch (const std::system_error& e) { if (e.code() == kioto::error::eof) break; throw; }
                if (n == 0) break;
                out.append(buf, n);
            }
        }(f, got));
    }

    CHECK(got == payload);
}

TEST_CASE_TEMPLATE("reading past end of file surfaces EOF", Ctx, KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::stream_file<typename Ctx::scheduler>;
    Ctx ctx;
    scratch_file scratch;

    {
        file_t f{ctx.get_scheduler(), scratch.str(), file_t::write_only | file_t::create | file_t::truncate};
        run_one(ctx, [](file_t& f) -> typename Ctx::template task<> {
            co_await f.async_write_some(kioto::as_bytes("abc"sv));
        }(f));
    }

    std::size_t first = 0;
    std::error_code ec;
    {
        file_t f{ctx.get_scheduler(), scratch.str(), file_t::read_only};
        run_one(ctx, [](file_t& f, std::size_t& n1, std::error_code& out) -> typename Ctx::template task<> {
            char buf[64];
            n1 = co_await f.async_read_some(kioto::as_writable_bytes(buf));   // reads the 3 bytes
            try { co_await f.async_read_some(kioto::as_writable_bytes(buf)); }  // now at EOF
            catch (const std::system_error& e) { out = e.code(); }
        }(f, first, ec));
    }

    CHECK(first == 3);
    CHECK(ec == kioto::error::eof);
}

TEST_CASE_TEMPLATE("stream_file size / seek / sync are usable (synchronous paths)", Ctx,
                   KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::stream_file<typename Ctx::scheduler>;
    Ctx ctx;
    scratch_file scratch;

    file_t f{ctx.get_scheduler(), scratch.str(), file_t::read_write | file_t::create | file_t::truncate};
    const auto written = f.write_some(kioto::as_bytes("0123456789"sv));   // synchronous write
    CHECK(written == 10);
    f.sync_all();    // must not throw
    f.sync_data();   // must not throw

    CHECK(f.size() == 10);

    const auto pos = f.seek(5, file_t::seek_set);
    CHECK(pos == 5);
    char buf[16];
    const auto n = f.read_some(kioto::as_writable_bytes(buf));   // reads from the sought position
    CHECK(std::string(buf, n) == "56789");
}

TEST_CASE_TEMPLATE("random_access_file resize changes the reported size", Ctx, KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::random_access_file<typename Ctx::scheduler>;
    Ctx ctx;
    scratch_file scratch;

    file_t f{ctx.get_scheduler(), scratch.str(), file_t::read_write | file_t::create | file_t::truncate};
    f.write_some_at(0, kioto::as_bytes("hello world"sv));   // synchronous positional write
    CHECK(f.size() == 11);

    f.resize(4);
    CHECK(f.size() == 4);
}

TEST_CASE_TEMPLATE("async_write_at / async_read_at compose positional file I/O", Ctx,
                   KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::random_access_file<typename Ctx::scheduler>;
    Ctx ctx;
    scratch_file scratch;

    constexpr std::size_t n = 64 * 1024;
    const auto payload = make_pattern(n);
    file_t f{ctx.get_scheduler(), scratch.str(), file_t::read_write | file_t::create | file_t::truncate};

    run_one(ctx, [](file_t& f, std::span<const std::byte> data) -> typename Ctx::template task<> {
        co_await (kioto::async_write_at(f, 0, data) | as_throwing);        // composed positional write
    }(f, std::span<const std::byte>(payload)));

    std::vector<std::byte> got(n, std::byte{0});
    run_one(ctx, [](file_t& f, std::span<std::byte> out) -> typename Ctx::template task<> {
        co_await (kioto::async_read_at(f, 0, out) | as_throwing);          // composed positional read
    }(f, std::span(got)));
    CHECK(got == payload);

    kioto::flat_buffer buf{n + 64};
    run_one(ctx, [](file_t& f, kioto::flat_buffer& b, std::size_t want) -> typename Ctx::template task<> {
        co_await (kioto::async_read_at(f, 0, b, want) | as_throwing);      // positional read into a dynamic buffer
    }(f, buf, n));
    CHECK(buf.size() == n);
}

TEST_CASE_TEMPLATE("opening a nonexistent file for reading fails", Ctx, KIOTO_TEST_FILE_CONTEXTS) {
    using file_t = kioto::stream_file<typename Ctx::scheduler>;
    Ctx ctx;
    const auto missing = (std::filesystem::temp_directory_path() / "kioto_no_such_file_12345.tmp").string();
    std::filesystem::remove(missing);

    std::error_code ec;
    try {
        file_t f{ctx.get_scheduler(), missing, file_t::read_only};   // open() throws synchronously
        static_cast<void>(f);
    }
    catch (const std::system_error& e) { ec = e.code(); }

    CHECK((ec == std::errc::no_such_file_or_directory or ec == std::error_code{kioto::error::not_found}));
}
