// UDP throughput ("hose") microbenchmark for coio, on the single-owner uring reactor.
//
//   udp_hose -- blast datagrams over connected localhost UDP and measure packets/s + bytes/s.
//               Depth D is modelled as D INDEPENDENT socket pairs (the datagram socket API allows
//               only one async_receive in flight per socket, so concurrency comes from multiple
//               pairs, not multiple receives on one socket). All pairs run on ONE uring_context,
//               so the ring carries ~2D send/recv SQEs at once and one wait reaps a batch -- the
//               same batch-reap effect the sticky-read bench exercises, but for datagrams.
//
// Connected UDP (both ends connect()ed) is the fast path: no per-packet sockaddr, no route lookup.
// UDP is lossy, so termination is loss-robust: senders push a fixed count, then after a short drain
// grace the receivers' blocked async_receive is cancelled via close(). We count what actually
// arrived and report the loss rate as a counter, so drops are visible rather than hanging the run.
//
// Run: bench_udp [--benchmark_filter=...]
#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <system_error>
#include <vector>
#include <benchmark/benchmark.h>
#include <coio/core.h>
#include <coio/asyncio/io.h>
#include <coio/asyncio/uring_context.h>
#include <coio/net/socket.h>
#include <coio/net/udp.h>

namespace {
    using io_context = coio::uring_context;
    using udp_socket = coio::udp::socket<io_context::scheduler>;

    struct flow {
        udp_socket tx;
        udp_socket rx;
        std::vector<std::byte> rxbuf;
    };

    // One send in flight at a time on this socket; blast `count` datagrams back-to-back. The LAST
    // sender to finish (single-threaded context -> the counter is race-free) drives teardown: a short
    // grace to let receivers drain the kernel buffer, then close every rx to release its blocked
    // async_receive. Flat structure on purpose -- co_await'ing a nested scope.join() from inside a
    // task doesn't compile, because io_context::task<> can't absorb the join's set_stopped.
    auto sender(io_context::scheduler sched, udp_socket& tx, std::span<const std::byte> payload,
                long count, std::span<flow> flows, long& senders_left, const long& received)
        -> io_context::task<> {
        for (long i = 0; i < count; ++i) {
            co_await tx.async_send(payload);
        }
        if (--senders_left == 0) {
            // Drain until idle: poll `received` across a grace window and close only once it stops
            // climbing (kernel buffers emptied). This avoids counting still-buffered datagrams as
            // loss (teardown truncation), yet stays loss-robust -- on drops, `received` just stalls
            // sooner. loss% then reflects REAL flood drops, not the teardown.
            long last = -1;
            while (received != last) {
                last = received;
                co_await sched.schedule_after(std::chrono::milliseconds{5});
            }
            for (auto& f : flows) f.rx.close();
        }
    }

    // Receive until the socket is closed under us (close() cancels the pending async_receive, which
    // surfaces as a system_error -> we stop). Every arrival bumps the shared counter.
    auto receiver(udp_socket& rx, std::span<std::byte> buf, long& received) -> io_context::task<> {
        try {
            for (;;) {
                co_await rx.async_receive(buf);
                ++received;
            }
        }
        catch (const std::system_error&) {
            // cancelled by close() at end of run, or a receive error -- either way, drain done.
        }
    }

    void udp_hose(benchmark::State& state) {
        const long depth = state.range(0);
        const long payload_size = state.range(1);
        constexpr long total = 200'000;          // datagrams per benchmark iteration
        const long per_flow = total / depth;     // depths chosen to divide `total` evenly
        constexpr int sock_buf = 1 << 22;        // 4 MiB SO_SNDBUF/SO_RCVBUF to soak loopback bursts

        const std::vector<std::byte> payload(static_cast<std::size_t>(payload_size), std::byte{0x5a});

        long total_received = 0; // accumulated across benchmark iterations (metric set once, after)
        for (auto _ : state) {
            state.PauseTiming();
            std::optional<io_context> ctx{std::in_place};
            auto sched = ctx->get_scheduler();

            std::vector<flow> flows;
            flows.reserve(static_cast<std::size_t>(depth));
            for (long i = 0; i < depth; ++i) {
                udp_socket tx{sched, coio::udp::v4()};
                udp_socket rx{sched, coio::udp::v4()};
                tx.bind(coio::endpoint{coio::ipv4_address::loopback(), 0});
                rx.bind(coio::endpoint{coio::ipv4_address::loopback(), 0});
                tx.set_option(udp_socket::send_buffer_size{sock_buf});
                rx.set_option(udp_socket::receive_buffer_size{sock_buf});
                // Connect both ends so async_send/async_receive skip the per-packet address.
                tx.connect(rx.local_endpoint());
                rx.connect(tx.local_endpoint());
                flows.push_back(flow{std::move(tx), std::move(rx),
                                     std::vector<std::byte>(static_cast<std::size_t>(payload_size))});
            }

            long received = 0;
            long senders_left = depth;
            coio::async_scope scope;
            state.ResumeTiming();

            for (auto& f : flows) scope.spawn_on(sched, receiver(f.rx, f.rxbuf, received));
            for (auto& f : flows) {
                scope.spawn_on(sched, sender(sched, f.tx, payload, per_flow, flows, senders_left, received));
            }
            ctx->run(); // timed region: drive every send + receive to completion

            state.PauseTiming();
            coio::this_thread::sync_wait(scope.join());
            total_received += received;
            flows.clear();
            ctx.reset();
            state.ResumeTiming();
        }

        // Headline items/s + bytes/s = SEND rate: how fast coio hoses datagrams out (clean, no
        // receiver-side loopback artifacts). recv/s = delivered rate; loss% = fraction the loopback
        // dropped under the flood. Metrics set once, over the whole run.
        const long total_sent = per_flow * depth * state.iterations();
        state.SetItemsProcessed(total_sent);
        state.SetBytesProcessed(total_sent * payload_size);
        state.counters["recv/s"] = benchmark::Counter(
            static_cast<double>(total_received), benchmark::Counter::kIsRate);
        state.counters["loss%"] = benchmark::Counter(
            total_sent == 0 ? 0.0 : 100.0 * static_cast<double>(total_sent - total_received) / static_cast<double>(total_sent));
    }
    // depth {1,8,64} x payload {64, 512, 1400} bytes (sub-MTU). 200000 / depth is integral for each.
    BENCHMARK(udp_hose)
        ->ArgsProduct({{1, 8, 64}, {64, 512, 1400}})
        ->UseRealTime();

    // Same hose, but each rx drains via ONE armed IORING_RECV_MULTISHOT (callback per datagram) instead
    // of a per-op async_receive loop. Head-to-head with udp_hose: does amortizing the per-datagram
    // op-state + coroutine across one armed op raise delivered PPS?
    auto ms_consumer(udp_socket& rx, coio::buffer_ring& bufs, long& received,
                     coio::inplace_stop_token tok) -> io_context::task<> {
        co_await coio::stop_when(
            rx.async_receive_multishot(bufs, [&](std::span<std::byte>) noexcept { ++received; }), tok);
    }

    auto ms_sender(io_context::scheduler sched, udp_socket& tx, std::span<const std::byte> payload,
                   long count, long& senders_left, const long& received,
                   coio::inplace_stop_source& stop) -> io_context::task<> {
        for (long i = 0; i < count; ++i) co_await tx.async_send(payload);
        if (--senders_left == 0) {
            long last = -1;
            while (received != last) { last = received; co_await sched.schedule_after(std::chrono::milliseconds{5}); }
            stop.request_stop();  // release every armed multishot
        }
    }

    void udp_hose_multishot(benchmark::State& state) {
        const long depth = state.range(0);
        const long payload_size = state.range(1);
        constexpr long total = 200'000;
        const long per_flow = total / depth;
        constexpr int sock_buf = 1 << 22;

        const std::vector<std::byte> payload(static_cast<std::size_t>(payload_size), std::byte{0x5a});

        long total_received = 0;
        for (auto _ : state) {
            state.PauseTiming();
            std::optional<io_context> ctx{std::in_place};
            auto sched = ctx->get_scheduler();

            std::vector<flow> flows;
            std::vector<std::unique_ptr<coio::buffer_ring>> rings;
            flows.reserve(static_cast<std::size_t>(depth));
            for (long i = 0; i < depth; ++i) {
                udp_socket tx{sched, coio::udp::v4()};
                udp_socket rx{sched, coio::udp::v4()};
                tx.bind(coio::endpoint{coio::ipv4_address::loopback(), 0});
                rx.bind(coio::endpoint{coio::ipv4_address::loopback(), 0});
                tx.set_option(udp_socket::send_buffer_size{sock_buf});
                rx.set_option(udp_socket::receive_buffer_size{sock_buf});
                tx.connect(rx.local_endpoint());
                rx.connect(tx.local_endpoint());
                rings.push_back(std::make_unique<coio::buffer_ring>(*ctx, 1024u,
                    static_cast<unsigned>(payload_size), static_cast<int>(i)));
                flows.push_back(flow{std::move(tx), std::move(rx), {}});
            }

            long received = 0;
            long senders_left = depth;
            coio::inplace_stop_source stop;
            coio::async_scope scope;
            state.ResumeTiming();

            for (long i = 0; i < depth; ++i) {
                scope.spawn_on(sched, ms_consumer(flows[i].rx, *rings[i], received, stop.get_token()));
            }
            for (auto& f : flows) {
                scope.spawn_on(sched, ms_sender(sched, f.tx, payload, per_flow, senders_left, received, stop));
            }
            ctx->run();

            state.PauseTiming();
            coio::this_thread::sync_wait(scope.join());
            total_received += received;
            flows.clear();
            rings.clear();
            ctx.reset();
            state.ResumeTiming();
        }

        const long total_sent = per_flow * depth * state.iterations();
        state.SetItemsProcessed(total_sent);
        state.SetBytesProcessed(total_sent * payload_size);
        state.counters["recv/s"] = benchmark::Counter(static_cast<double>(total_received), benchmark::Counter::kIsRate);
        state.counters["loss%"] = benchmark::Counter(
            total_sent == 0 ? 0.0 : 100.0 * static_cast<double>(total_sent - total_received) / static_cast<double>(total_sent));
    }
    BENCHMARK(udp_hose_multishot)
        ->ArgsProduct({{1, 8, 64}, {64, 512, 1400}})
        ->UseRealTime();
}
