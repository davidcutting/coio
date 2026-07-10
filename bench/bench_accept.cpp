// Does io_uring MULTISHOT accept beat a single-shot accept LOOP on the server's accept-DRAIN path?
// Clean isolation: pre-fill the listener's accept queue with B established connections (blocking connect
// returns once the kernel completes the handshake — no accept() needed), THEN time the server draining all
// B via each lowering. No concurrent client storm during the timed region => no handshake-latency / feedback
// -loop / teardown-deadlock confounds; the timed cost is purely the server's per-accept path.
//
//   accept_multishot_bench -- one armed multishot accept SQE, a CQE per queued conn (no per-op ceremony)
//   accept_loop_bench      -- re-issued single-shot accept (one SQE + op-state + coroutine resume per conn)
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <liburing.h>
#include <cstdint>
#include <utility>
#include <vector>
#include <benchmark/benchmark.h>
#include <coio/core.h>
#include <coio/net/socket.h>                 // detail::accept_sequence_loop, to_handle/to_native/native_handle
#include <coio/asyncio/uring_context.h>      // accept_multishot, make_io_handle

namespace {
    using io_context = coio::uring_context;
    using scheduler = io_context::scheduler;

    constexpr int B = 3500;   // established connections pre-queued (< somaxconn 4096, so all queue cleanly)

    auto make_listener() -> std::pair<int, std::uint16_t> {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        ::sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(fd, reinterpret_cast<::sockaddr*>(&addr), sizeof(addr));
        ::listen(fd, 4096);
        ::socklen_t len = sizeof(addr);
        ::getsockname(fd, reinterpret_cast<::sockaddr*>(&addr), &len);
        return {fd, ntohs(addr.sin_port)};
    }

    auto rst_close(int fd) noexcept -> void {   // SO_LINGER{1,0}: RST close, no TIME_WAIT pileup
        ::linger lg{1, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        ::close(fd);
    }

    // Blocking connect to the listener; the kernel completes the handshake and queues the ESTABLISHED
    // connection in the accept queue, so this returns without the server accepting. Returns the client fd.
    auto connect_one(std::uint16_t port) -> int {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        ::sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);
        ::connect(fd, reinterpret_cast<::sockaddr*>(&addr), sizeof(addr));
        return fd;
    }

    template<typename Seq>
    auto consume(Seq seq, coio::inplace_stop_token tok) -> io_context::task<> {
        co_await coio::stop_when(std::move(seq), tok);
    }

    void run_drain(benchmark::State& state, bool multishot) {
        for (auto _ : state) {
            state.PauseTiming();
            io_context ctx;
            auto sched = ctx.get_scheduler();
            auto [lfd, port] = make_listener();

            std::vector<int> client_fds;                 // pre-fill: B established, queued connections
            client_fds.reserve(B);
            for (int i = 0; i < B; ++i) client_fds.push_back(connect_one(port));

            int accepted = 0;
            coio::inplace_stop_source stop;
            auto sink = [&accepted, &stop](coio::detail::native_handle h) noexcept {
                rst_close(coio::detail::to_native(h));   // same per-accept cost for both lowerings (a constant)
                if (++accepted >= B) stop.request_stop();
            };
            {
                auto io_handle = sched.make_io_handle(coio::detail::to_handle(lfd));
                coio::async_scope scope;
                if (multishot)
                    scope.spawn_on(sched, consume(sched.accept_multishot(io_handle, sink), stop.get_token()));
                else
                    scope.spawn_on(sched, consume(
                        coio::detail::accept_sequence_loop<scheduler>(io_handle, sched, sink), stop.get_token()));
                state.ResumeTiming();

                ctx.run();   // TIMED: drain all B queued connections, then the sink trips stop -> run() exits

                state.PauseTiming();
                coio::this_thread::sync_wait(scope.join());
            }

            for (int c : client_fds) rst_close(c);
            ::close(lfd);
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * B);
    }

    void accept_multishot_bench(benchmark::State& s) { run_drain(s, true); }
    void accept_loop_bench(benchmark::State& s) { run_drain(s, false); }
    BENCHMARK(accept_multishot_bench)->UseRealTime();
    BENCHMARK(accept_loop_bench)->UseRealTime();

    // Init a raw ring with coio's EXACT setup (uring_context.cpp init_uring): single-issuer + defer/coop
    // taskrun + submit-all + R_DISABLED, then enable + register the ring fd — so the raw baseline below is
    // a fair apples-to-apples vs coio's ring, isolating ONLY coio's sender/coroutine machinery.
    auto init_like_coio(::io_uring& ring) -> void {
        for (const unsigned flags : {
                 unsigned{IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SUBMIT_ALL | IORING_SETUP_R_DISABLED},
                 unsigned{IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SUBMIT_ALL | IORING_SETUP_R_DISABLED}}) {
            if (::io_uring_queue_init(4096, &ring, flags) == 0) break;
        }
        ::io_uring_enable_rings(&ring);
        ::io_uring_register_ring_fd(&ring);
    }

    // Raw multishot accept on a coio-configured ring, doing the SAME per-accept work as coio's sink
    // (rst_close). The gap vs accept_multishot_bench (coio) is purely the sender/coroutine/schedule layer.
    void raw_coio_config_bench(benchmark::State& state) {
        for (auto _ : state) {
            state.PauseTiming();
            ::io_uring ring{};
            init_like_coio(ring);
            auto [lfd, port] = make_listener();
            std::vector<int> client_fds;
            client_fds.reserve(B);
            for (int i = 0; i < B; ++i) client_fds.push_back(connect_one(port));

            ::io_uring_sqe* sqe = ::io_uring_get_sqe(&ring);
            ::io_uring_prep_multishot_accept(sqe, lfd, nullptr, nullptr, 0);
            state.ResumeTiming();

            ::io_uring_submit(&ring);
            int accepted = 0;
            while (accepted < B) {
                ::io_uring_cqe* cqe = nullptr;
                ::io_uring_wait_cqe(&ring, &cqe);
                if (cqe->res >= 0) { rst_close(cqe->res); ++accepted; }   // same as coio's sink
                ::io_uring_cqe_seen(&ring, cqe);
            }

            state.PauseTiming();
            for (int c : client_fds) rst_close(c);
            ::io_uring_queue_exit(&ring);
            ::close(lfd);
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * B);
    }
    BENCHMARK(raw_coio_config_bench)->UseRealTime();
}
