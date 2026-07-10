// Cross-core UDP recv hotpath isolation: profile the SINGLE-CORE multishot receiver under a
// real, saturating cross-core send flood.
//
// Topology (two runtimes):
//   send tier -- 4 dedicated flood threads, each blasting connected-UDP datagrams with a raw
//                blocking send() loop. Raw on purpose: the flood must saturate the receiver's
//                core WITHOUT itself becoming the bottleneck, and it must keep no coio machinery
//                on the send side so the receiver's profile is pure coio recv.
//   recv tier -- ONE coio uring_context, 1 core, draining every flow via an armed multishot recv
//                (async_receive_sequence -> IORING_RECV_MULTISHOT into a kernel buf_ring). This
//                thread is the subject: perf record it and confirm the per-datagram cost is the
//                kernel + the multishot callback, NOT an op-state alloc / coroutine resume / a
//                schedule hop per datagram.
//
// Profile it:
//   perf record -m 1 -g -e cycles ./build/bench/bench_udp_crosscore --benchmark_filter=recv_flood/512
//   perf report --tui   # focus the "coio-recv" thread; look for on_completion / the sink, and
//                        # verify NO coroutine_handle::resume / operator new per datagram.
// (-m 1 works around io_uring vs RLIMIT_MEMLOCK ENOMEM under perf mmap, as with bench_io.)
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>
#include <thread>
#include <utility>
#include <vector>
#include <benchmark/benchmark.h>
#include <coio/core.h>
#include <coio/asyncio/uring_context.h>
#include <coio/net/socket.h>
#include <coio/net/udp.h>

namespace {
    using io_context = coio::uring_context;
    using udp_socket = coio::udp::socket<io_context::scheduler>;

    constexpr int N_FLOWS = 8;     // rx sockets the receiver drains concurrently (fixed)

    auto bind_loopback(int fd) -> ::sockaddr_in {
        constexpr int buf = 1 << 22;                 // 4 MiB so loopback bursts don't drop before we drain
        ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
        ::sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port = 0;
        ::bind(fd, reinterpret_cast<::sockaddr*>(&a), sizeof(a));
        ::socklen_t len = sizeof(a);
        ::getsockname(fd, reinterpret_cast<::sockaddr*>(&a), &len);
        return a;
    }

    // An UNCONNECTED rx socket, so an arbitrary number of tx sockets can target it -- this is what lets
    // the sender sweep push past N_FLOWS senders to actually find the receiver-core ceiling.
    auto make_rx() -> std::pair<int, ::sockaddr_in> {
        const int rx = ::socket(AF_INET, SOCK_DGRAM, 0);
        return {rx, bind_loopback(rx)};
    }

    // A tx socket connected to one rx addr (connected => no per-packet addr on send()).
    auto make_tx(const ::sockaddr_in& rxa) -> int {
        const int tx = ::socket(AF_INET, SOCK_DGRAM, 0);
        bind_loopback(tx);
        ::connect(tx, reinterpret_cast<const ::sockaddr*>(&rxa), sizeof(rxa));
        return tx;
    }

    auto pin_to(int cpu) -> void {
        ::cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
    }

    // One flood thread: round-robin a fixed payload over its tx fds until told to stop. Pure raw
    // send(); a full send buffer just blocks/EAGAIN-drops -- we don't care, the receiver counts truth.
    void flood_thread(int cpu, std::vector<int> tx_fds, std::span<const std::byte> payload,
                      const std::atomic<bool>& stop, std::atomic<long>& sent) {
        ::pthread_setname_np(::pthread_self(), "coio-flood");
        pin_to(cpu);                                  // senders spread across cores 1..N-1; recv owns core 0
        long local = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            for (const int fd : tx_fds) {
                if (::send(fd, payload.data(), payload.size(), MSG_DONTWAIT) > 0) ++local;
            }
        }
        sent.fetch_add(local, std::memory_order_relaxed);
    }

    // The armed multishot recv for one rx socket: a callback per datagram, no per-op ceremony. Every
    // arrival bumps the (single-threaded, race-free) counter; the target-th arrival trips the stop that
    // releases every armed multishot and lets run() return.
    auto rx_consumer(udp_socket& rx, coio::buffer_ring& bufs, long& received, long target,
                     coio::inplace_stop_source& stop) -> io_context::task<> {
        co_await coio::stop_when(
            rx.async_receive_sequence(bufs, [&](std::span<std::byte>) noexcept {
                if (++received == target) stop.request_stop();
            }),
            stop.get_token());
    }

    void recv_flood(benchmark::State& state) {
        const long payload_size = state.range(0);
        const int n_senders = static_cast<int>(state.range(1));   // flood threads (sweep to find the ceiling)
        constexpr long target = 400'000;             // datagrams to drain per timed iteration (steady state)
        const std::vector<std::byte> payload(static_cast<std::size_t>(payload_size), std::byte{0x5a});

        long total_received = 0;
        std::atomic<long> total_sent{0};
        for (auto _ : state) {
            state.PauseTiming();

            io_context ctx;                           // the 1-core recv runtime (the subject)
            auto sched = ctx.get_scheduler();
            std::vector<udp_socket> rxs;
            std::vector<::sockaddr_in> rx_addr;
            std::vector<std::unique_ptr<coio::buffer_ring>> rings;
            rxs.reserve(N_FLOWS);
            rx_addr.reserve(N_FLOWS);
            for (int i = 0; i < N_FLOWS; ++i) {
                auto [rx, addr] = make_rx();
                rx_addr.push_back(addr);
                rxs.emplace_back(sched, coio::detail::to_handle(rx));
                rings.push_back(std::make_unique<coio::buffer_ring>(
                    ctx, 1024u, static_cast<unsigned>(payload_size), i));
            }

            // Flood tier: n_senders threads, each with its OWN tx socket aimed at one rx (round-robin).
            // Senders can exceed N_FLOWS -> keep piling on until the receiver core is the bottleneck.
            std::atomic<bool> stop_senders{false};
            std::vector<int> tx_fds;
            std::vector<std::thread> senders;
            tx_fds.reserve(n_senders);
            senders.reserve(n_senders);
            const int n_cpus = std::max(2, static_cast<int>(std::thread::hardware_concurrency()));
            for (int s = 0; s < n_senders; ++s) {
                tx_fds.push_back(make_tx(rx_addr[s % N_FLOWS]));
                senders.emplace_back(flood_thread, 1 + (s % (n_cpus - 1)), std::vector<int>{tx_fds.back()},
                                     std::span<const std::byte>(payload),
                                     std::cref(stop_senders), std::ref(total_sent));
            }

            long received = 0;
            coio::inplace_stop_source stop;
            coio::async_scope scope;
            ::pthread_setname_np(::pthread_self(), "coio-recv");
            pin_to(0);                                // receiver owns core 0, uncontended
            state.ResumeTiming();

            for (int i = 0; i < N_FLOWS; ++i)
                scope.spawn_on(sched, rx_consumer(rxs[i], *rings[i], received, target, stop));
            ctx.run();                                // TIMED: drain `target` datagrams on this one core

            state.PauseTiming();
            coio::this_thread::sync_wait(scope.join());
            stop_senders.store(true, std::memory_order_relaxed);
            for (auto& t : senders) t.join();
            for (const int fd : tx_fds) ::close(fd);          // rx fds owned by rxs -> closed on destruction
            total_received += received;
            rxs.clear();
            rings.clear();
            state.ResumeTiming();
        }

        state.SetItemsProcessed(total_received);
        state.SetBytesProcessed(total_received * payload_size);
        // recv/s = the receiver-core ceiling. loss% high & flat as senders scale => the RECEIVER is the
        // bottleneck (core saturated); if recv/s climbs with senders, the flood was the limit, not us.
        state.counters["recv/s"] = benchmark::Counter(
            static_cast<double>(total_received), benchmark::Counter::kIsRate);
        const long sent = total_sent.load();
        state.counters["loss%"] = benchmark::Counter(
            sent == 0 ? 0.0 : 100.0 * static_cast<double>(sent - total_received) / static_cast<double>(sent));
    }
    // Sweep sender count at a fixed 512B payload to locate the receiver-core ceiling: keep piling on
    // flood threads until recv/s plateaus and loss% climbs -- that knee is the receiver's true ceiling.
    // Sub-MTU payload: size mostly shifts memcpy, not the coio recv path.
    // 2..8 senders scale near-linearly at ~0 loss (flood-limited); the knee is 8->10, where recv/s
    // plateaus ~2.1M and loss jumps to ~20% (receiver core saturated). >10 senders only add contention.
    BENCHMARK(recv_flood)->ArgsProduct({{512}, {2, 4, 6, 8, 10}})->UseRealTime();
}
