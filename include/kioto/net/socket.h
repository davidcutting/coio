// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <span>
#include <kioto/core.h>
#include <kioto/base/error.h>
#include <kioto/io/io_descriptions.h>
#include <kioto/io/io_sender.h>
#include <kioto/exec/async_result.h>

struct linger;

namespace kioto {
    namespace detail::socket {
        struct linger_storage {
            #if KIOTO_OS_WINDOWS
            using linger_integral = unsigned short;
#else
            using linger_integral = int;
#endif
            linger_integral l_onoff;
            linger_integral l_linger;
        };

        enum class shutdown_type : short {
            shutdown_send,
            shutdown_receive,
            shutdown_both,
        };

        auto set_sockopt(native_fd handle, int level, int option_name, std::span<const std::byte> value) -> void;

        auto get_sockopt(native_fd handle, int level, int option_name, std::span<std::byte> value) -> void;

        auto sol_socket_v() noexcept -> int;

        auto ipproto_ipv6_v() noexcept -> int;

        auto ipproto_tcp_v() noexcept -> int;

        template<typename ValueType>
        struct sock_option_traits {
            using storage = ValueType;

            static auto from_value(const ValueType& value) noexcept -> storage {
                return value;
            }

            static auto to_value(const storage& storage) noexcept -> ValueType {
                return storage;
            }
        };

        template<>
        struct sock_option_traits<bool> {
            using storage = int;

            static auto from_value(bool value) noexcept -> storage {
                return static_cast<int>(value);
            }

            static auto to_value(int storage) noexcept -> bool {
                return static_cast<bool>(storage);
            }
        };

        template<>
        struct sock_option_traits<::linger> {
            using storage = linger_storage;

            static auto from_value(const ::linger& value) noexcept -> linger_storage;

            static auto to_value(const linger_storage& storage) noexcept -> ::linger;
        };

        template<typename ValueType, int(*Level)() noexcept>
        class sock_option {
        private:
            using traits = sock_option_traits<ValueType>;
            using storage_type = typename traits::storage;
            static constexpr std::size_t length = sizeof(storage_type);

        public:
            sock_option() noexcept = default;

            explicit sock_option(ValueType value) noexcept : storage_(value) {}

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto data() noexcept -> std::span<std::byte> {
                return {reinterpret_cast<std::byte*>(&storage_), length};
            }

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto data() const noexcept -> std::span<const std::byte> {
                return {reinterpret_cast<const std::byte*>(&storage_), length};
            }

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE static auto level() noexcept -> int {
                return Level();
            }

            [[nodiscard]]
            auto get() const noexcept -> ValueType {
                return traits::to_value(storage_);
            }

            auto set(const ValueType& value) noexcept -> void {
                storage_ = traits::from_value(value);
            }

        protected:
            storage_type storage_;
        };

        // socket options
        struct debug : sock_option<bool, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct do_not_route : sock_option<bool, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct broadcast : sock_option<bool, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct keep_alive : sock_option<bool, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct linger : sock_option<::linger, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct out_of_band_inline : sock_option<bool, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct receive_buffer_size : sock_option<int, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct receive_low_watermark : sock_option<int, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct reuse_address : sock_option<bool, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct send_buffer_size : sock_option<int, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        struct send_low_watermark : sock_option<int, sol_socket_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        // ip options
        struct v6_only : sock_option<bool, ipproto_ipv6_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        // tcp options
        struct no_delay : sock_option<bool, ipproto_tcp_v> {
            using sock_option::sock_option;

            [[nodiscard]]
            static auto name() noexcept -> int;
        };

        [[nodiscard]]
        auto open(int family, int type, int protocol_id) -> native_fd;

        auto close(native_fd handle) -> void;

        [[nodiscard]]
        auto max_backlog() noexcept -> std::size_t;

        [[nodiscard]]
        auto local_endpoint(native_fd handle) -> endpoint;

        [[nodiscard]]
        auto remote_endpoint(native_fd handle) -> endpoint;

        auto shutdown(native_fd handle, shutdown_type how) -> void;

        auto bind(native_fd handle, const endpoint& local_endpoint) -> void;

        auto listen(native_fd handle, std::size_t backlog) -> void;

        auto connect(native_fd handle, const endpoint& peer) -> void;

        auto accept(native_fd handle) -> native_fd;

        auto receive(native_fd handle, std::span<std::byte> buffer) -> std::size_t;

        auto send(native_fd handle, std::span<const std::byte> buffer) -> std::size_t;

        auto receive_from(native_fd handle, std::span<std::byte> buffer) -> std::pair<endpoint, size_t>;

        auto send_to(native_fd handle, std::span<const std::byte> buffer, const endpoint& dest) -> std::size_t;
    }

    template<typename Protocol, io_scheduler IoScheduler>
    class basic_socket {
    private:
        using implementation_type = decltype(std::declval<IoScheduler&>().make_io_handle(std::declval<typename IoScheduler::native_handle_type>()));
        static_assert(detail::io_driver_handle<implementation_type>,
                      "IoScheduler::make_io_handle must yield a conforming io handle (see detail::io_driver_handle)");

    public:
        using protocol_type = Protocol;
        using scheduler_type = IoScheduler;
        // Opaque driver handle; the facade never assumes it's an fd. Raw fd only via detail::to_native().
        using native_handle_type = typename IoScheduler::native_handle_type;
        using shutdown_type = detail::socket::shutdown_type;
        using enum shutdown_type;

        // Socket options
        using broadcast = detail::socket::broadcast;
        using debug = detail::socket::debug;
        using do_not_route = detail::socket::do_not_route;
        using keep_alive = detail::socket::keep_alive;
        using linger = detail::socket::linger;
        using out_of_band_inline = detail::socket::out_of_band_inline;
        using receive_buffer_size = detail::socket::receive_buffer_size;
        using receive_low_watermark = detail::socket::receive_low_watermark;
        using reuse_address = detail::socket::reuse_address;
        using send_buffer_size = detail::socket::send_buffer_size;
        using send_low_watermark = detail::socket::send_low_watermark;

        // Ip options
        using v6_only = detail::socket::v6_only;

    public:
        explicit basic_socket(scheduler_type scheduler) noexcept :
            basic_socket(std::move(scheduler), native_handle_type{}) {}

        basic_socket(scheduler_type scheduler, native_handle_type handle) :
            impl_(scheduler.make_io_handle(handle)) {}

        basic_socket(scheduler_type scheduler, const protocol_type& protocol) : basic_socket(std::move(scheduler)) {
            this->open(protocol);
        }

        basic_socket(const basic_socket&) = delete;

        basic_socket(basic_socket&& other) = default;

        ~basic_socket() noexcept {
            close();
        }

        auto operator= (basic_socket other) noexcept -> basic_socket& {
            std::ranges::swap(impl_, other.impl_);
            return *this;
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto get_io_scheduler() const noexcept -> scheduler_type {
            return impl_.get_io_scheduler();
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto native_handle() const noexcept -> native_handle_type {
            return impl_.native_handle();
        }

        // Throws error::already_open if already open.
        KIOTO_ALWAYS_INLINE auto open(const protocol_type& protocol = protocol_type()) -> void {
            if (is_open()) throw std::system_error{error::already_open, "open"};
            impl_ = get_io_scheduler().make_io_handle(detail::to_handle(detail::socket::open(protocol.family(), protocol.type(), protocol.protocol_id())));
        }

        // close/release/cancel all cancel in-flight send/receive/connect immediately.
        KIOTO_ALWAYS_INLINE auto close() -> void {
            detail::socket::close(detail::to_native(release()));
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto release() -> native_handle_type {
            return impl_.release();
        }

        KIOTO_ALWAYS_INLINE auto cancel() -> void {
            impl_.cancel();
        }

        // Disable sends and/or receives (shutdown_send / shutdown_receive / shutdown_both).
        KIOTO_ALWAYS_INLINE auto shutdown(shutdown_type how) -> void {
            return detail::socket::shutdown(detail::to_native(native_handle()), how);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto is_open() const noexcept -> bool {
            return native_handle() != native_handle_type{};
        }

        KIOTO_ALWAYS_INLINE explicit operator bool() const noexcept {
            return is_open();
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto local_endpoint() const -> endpoint {
            return detail::socket::local_endpoint(detail::to_native(native_handle()));
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto remote_endpoint() const -> endpoint {
            return detail::socket::remote_endpoint(detail::to_native(native_handle()));
        }

        template<typename SocketOption>
        KIOTO_ALWAYS_INLINE auto set_option(const SocketOption& option) -> void {
            detail::socket::set_sockopt(detail::to_native(native_handle()), option.level(), option.name(), option.data());
        }

        template<typename SocketOption>
        KIOTO_ALWAYS_INLINE auto get_option(SocketOption& option) const -> void {
            detail::socket::get_sockopt(detail::to_native(native_handle()), option.level(), option.name(), option.data());
        }

        KIOTO_ALWAYS_INLINE auto bind(const endpoint& local_endpoint) -> void {
            detail::socket::bind(detail::to_native(native_handle()), local_endpoint);
        }

        KIOTO_ALWAYS_INLINE auto connect(const endpoint& peer) -> void {
            if (not is_open()) open();
            detail::socket::connect(detail::to_native(native_handle()), peer);
        }

        KIOTO_ALWAYS_INLINE auto async_connect(const endpoint& peer) {
            if (not is_open()) open();
            return get_io_scheduler().schedule_io(impl_, detail::async_connect_t{peer});
        }

    protected:
        implementation_type impl_;
    };

    namespace detail {
        // Generic (driver-neutral) fallback for async_accept_sequence: a coroutine bound to Sched that
        // re-issues single-shot accept and hands each freshly-minted fd (as a native_handle) to `sink`,
        // until stopped (unwinds -> set_stopped) or a fatal error (-> set_error). Uses only schedule_io +
        // async_accept_t, which every driver supports, so epoll/iocp get the "keep accepting" API for
        // free; io_uring overrides it with multishot accept (uring_scheduler::accept_multishot). Bound to
        // Sched so the co_awaits affine + propagate the stop token exactly like any task on that scheduler.
        template<typename Sched, typename Handle, typename HandleSink>
        auto accept_sequence_loop(Handle& impl, Sched sched, HandleSink sink) -> kioto::task<void, void, Sched> {
            for (;;) {
                auto handle = co_await sched.schedule_io(impl, detail::async_accept_t{});
                sink(handle);
            }
        }

        // Generic (driver-neutral) fallback for async_receive_sequence: a coroutine bound to Sched that
        // recvs one datagram at a time into `bufs`'s buffer and hands each to `sink` (span valid only for
        // the call), until stopped (-> set_stopped) or a fatal error. io_uring overrides this with multishot
        // recv (uring_scheduler::receive_multishot into a kernel buf_ring). One-at-a-time recv gives the
        // same synchronous-sink backpressure as multishot: the next recv waits until the sink returns.
        template<typename Sched, typename Handle, typename Bufs, typename Sink>
        auto receive_sequence_loop(Handle& impl, Sched sched, Bufs& bufs, Sink sink) -> kioto::task<void, void, Sched> {
            for (;;) {
                const std::size_t n = co_await sched.schedule_io(impl, detail::async_receive_t{bufs.buffer()});
                sink(bufs.buffer().first(n));
            }
        }

        // A loop coroutine fails with set_error(exception_ptr); io_senders use set_error(error_code). Map it.
        [[nodiscard]] inline auto exception_to_error_code(std::exception_ptr ep) noexcept -> std::error_code {
            try { std::rethrow_exception(ep); }
            catch (const std::system_error& e) { return e.code(); }
            catch (...) { return std::make_error_code(std::errc::io_error); }
        }

        // Rewrites the loop's set_error(exception_ptr) -> set_error(error_code) so both sequence lowerings
        // advertise the same sigs <set_value_t(), set_error_t(error_code), set_stopped_t()>. Sigs are declared
        // EXPLICITLY, not via let_error (which needs a non-empty env; the await path queries with none).
        template<typename Child>
        struct error_code_sequence_sender {
            using sender_concept = execution::sender_tag;
            using completion_signatures = execution::completion_signatures<
                execution::set_value_t(), execution::set_error_t(std::error_code), execution::set_stopped_t()>;

            template<typename Rcvr>
            struct state {
                using operation_state_concept = execution::operation_state_tag;

                // Separate holder (à la stop_when) so `receiver` points at a COMPLETE type; referencing the
                // enclosing state before it's instantiated is ill-formed. env forwarded unchanged so the
                // child task still resolves its bound scheduler from downstream.
                struct data_t { Rcvr rcvr; };

                struct receiver {
                    using receiver_concept = execution::receiver_tag;
                    auto get_env() const noexcept { return execution::get_env(d->rcvr); }
                    auto set_value() && noexcept -> void { execution::set_value(std::move(std::exchange(d, nullptr)->rcvr)); }
                    auto set_error(std::exception_ptr ep) && noexcept -> void {
                        execution::set_error(std::move(std::exchange(d, nullptr)->rcvr), exception_to_error_code(ep));
                    }
                    auto set_stopped() && noexcept -> void { execution::set_stopped(std::move(std::exchange(d, nullptr)->rcvr)); }
                    data_t* d;
                };

                using inner_t = execution::connect_result_t<Child, receiver>;
                state(Child child, Rcvr rcvr) : data{std::move(rcvr)}, inner(execution::connect(std::move(child), receiver{&data})) {}
                auto start() & noexcept -> void { execution::start(inner); }

                data_t data;
                inner_t inner;
            };

            template<similar_to<error_code_sequence_sender>, typename...>
            static consteval auto get_completion_signatures() noexcept -> completion_signatures { return {}; }
            template<execution::receiver Rcvr>
            auto connect(Rcvr rcvr) && -> state<Rcvr> { return state<Rcvr>{std::move(child), std::move(rcvr)}; }

            Child child;
        };

        template<typename Child>
        [[nodiscard]] auto as_error_code_sequence(Child child) {
            return error_code_sequence_sender<Child>{std::move(child)};
        }
    }

    template<typename Protocol, io_scheduler IoScheduler>
    class basic_socket_acceptor : public basic_socket<Protocol, IoScheduler> {
    private:
        template<io_scheduler OtherScheduler>
        using protocol_socket_ = typename Protocol::template socket<OtherScheduler>;
        using base = basic_socket<Protocol, IoScheduler>;

    public:
        template<io_scheduler OtherScheduler>
        using rebind_scheduler = basic_socket_acceptor<Protocol, OtherScheduler>;
        using typename base::scheduler_type;
        using typename base::protocol_type;
        using typename base::native_handle_type;

    public:
        using base::base;

        basic_socket_acceptor(
            scheduler_type scheduler,
            const endpoint& local_endpoint,
            std::size_t backlog = max_backlog(),
            bool reuse_addr = true
        ) : basic_socket_acceptor(scheduler) {
            this->open(local_endpoint.ip().is_v4() ? protocol_type::v4() : protocol_type::v6());
            this->set_option(detail::socket::reuse_address{reuse_addr});
            this->bind(local_endpoint);
            this->listen(backlog);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE static auto max_backlog() noexcept -> std::size_t {
            return detail::socket::max_backlog();
        }

        KIOTO_ALWAYS_INLINE auto listen(std::size_t backlog = max_backlog()) -> void {
           detail::socket::listen(detail::to_native(this->native_handle()), backlog);
        }

        template<io_scheduler OtherScheduler>
        KIOTO_ALWAYS_INLINE auto accept(protocol_socket_<OtherScheduler>& peer) -> void {
            peer = this->accept(peer.get_io_scheduler());
        }

        // The accepted socket is bound to peer_scheduler (may differ from the acceptor's).
        template<io_scheduler OtherScheduler>
        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto accept(OtherScheduler peer_scheduler) -> protocol_socket_<OtherScheduler> {
            return protocol_socket_<OtherScheduler>(peer_scheduler, detail::to_handle(detail::socket::accept(detail::to_native(this->native_handle()))));
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto accept() -> protocol_socket_<scheduler_type> {
            return this->accept(this->get_io_scheduler());
        }

        // INVARIANT for every async_ op on a socket: at most one in flight, and never initiate concurrently
        // from two threads on the same socket (UB). Applies to accept/send/receive/connect alike.
        template<io_scheduler OtherScheduler>
        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_accept(protocol_socket_<OtherScheduler>& peer) {
            return then(
                this->get_io_scheduler().schedule_io(this->impl_, detail::async_accept_t{}),
                [&peer](native_handle_type handle) noexcept {
                    peer = protocol_socket_<OtherScheduler>(peer.get_io_scheduler(), handle);
                }
            );
        }

        template<io_scheduler OtherScheduler>
        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_accept(OtherScheduler other_scheduler) {
            return then(
                this->get_io_scheduler().schedule_io(this->impl_, detail::async_accept_t{}),
                [other_scheduler](native_handle_type handle) noexcept {
                    return protocol_socket_<OtherScheduler>(other_scheduler, handle);
                }
            );
        }

        // Keep accepting into `sink` (invoked on the owner thread, one socket per connection, synchronous)
        // until stopped; sequence-shaped sender — compose with stop_when(...) to bound it. Driver-specific
        // lowering: io_uring arms one multishot accept (fast path), epoll/iocp loop single-shot accept.
        template<typename Sink>
        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_accept_sequence(Sink sink) {
            auto sched = this->get_io_scheduler();
            // Wrap the minted handle into a socket here (not in the driver) so async_accept_t stays driver-clean.
            auto handle_sink = [sink = std::move(sink), sched](native_handle_type h) mutable {
                sink(protocol_socket_<scheduler_type>{sched, h});
            };
            if constexpr (requires { sched.accept_multishot(this->impl_, handle_sink); }) {
                return sched.accept_multishot(this->impl_, std::move(handle_sink));
            }
            else {
                // Normalize the loop's exception_ptr error to error_code so both lowerings share one sig set.
                return detail::as_error_code_sequence(
                    detail::accept_sequence_loop<scheduler_type>(this->impl_, sched, std::move(handle_sink)));
            }
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_accept() {
            return this->async_accept(this->get_io_scheduler());
        }
    };

    template<typename Protocol, io_scheduler IoScheduler>
    class basic_stream_socket : public basic_socket<Protocol, IoScheduler> {
    private:
        using base = basic_socket<Protocol, IoScheduler>;

    public:
        template<io_scheduler OtherScheduler>
        using rebind_scheduler = basic_stream_socket<Protocol, OtherScheduler>;
        using typename base::scheduler_type;
        using typename base::protocol_type;
        using typename base::native_handle_type;

    public:
        using base::base;

        // read_some/receive: 0 bytes into a non-empty buffer means peer closed -> throws error::eof.
        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto read_some(std::span<std::byte> buffer) -> std::size_t {
            const auto bytes_transferred = detail::socket::receive(detail::to_native(this->native_handle()), buffer);
            if (bytes_transferred == 0 and not buffer.empty()) [[unlikely]] throw std::system_error{error::eof, "read_some"};
            return bytes_transferred;
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto write_some(std::span<const std::byte> buffer) -> std::size_t {
            return detail::socket::send(detail::to_native(this->native_handle()), buffer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto receive(std::span<std::byte> buffer) -> std::size_t {
            return read_some(buffer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto send(std::span<const std::byte> buffer) -> std::size_t {
            return write_some(buffer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_read_some(std::span<std::byte> buffer) {
            // EOF-on-zero folded into the driver completion (async_stream_receive_t::eof_on_zero) — no
            // wrapping let_value, one fewer sender/op-state per recv. Stream recv (TCP): 0 == peer closed.
            return this->get_io_scheduler().schedule_io(this->impl_, detail::async_stream_receive_t{buffer});
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_write_some(std::span<const std::byte> buffer) {
            return this->get_io_scheduler().schedule_io(this->impl_, detail::async_stream_send_t{buffer});
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_receive(std::span<std::byte> buffer) {
            return async_read_some(buffer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_send(std::span<const std::byte> buffer) {
            return async_write_some(buffer);
        }
    };

    template<typename Protocol, io_scheduler IoScheduler>
    class basic_datagram_socket : public basic_socket<Protocol, IoScheduler> {
    private:
        using base = basic_socket<Protocol, IoScheduler>;

    public:
        template<io_scheduler OtherScheduler>
        using rebind_scheduler = basic_datagram_socket<Protocol, OtherScheduler>;
        using typename base::scheduler_type;
        using typename base::protocol_type;
        using typename base::native_handle_type;

    public:
        using base::base;

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto receive(std::span<std::byte> buffer) -> std::size_t {
            return detail::socket::receive(detail::to_native(this->native_handle()), buffer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto send(std::span<const std::byte> buffer) -> std::size_t {
            return detail::socket::send(detail::to_native(this->native_handle()), buffer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto receive_from(std::span<std::byte> buffer) -> std::pair<endpoint, std::size_t> {
            return detail::socket::receive_from(detail::to_native(this->native_handle()), buffer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto send_to(std::span<const std::byte> buffer, const endpoint& peer) -> std::size_t {
            return detail::socket::send_to(detail::to_native(this->native_handle()), buffer, peer);
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_receive(std::span<std::byte> buffer) {
            return this->get_io_scheduler().schedule_io(
                this->impl_,
                detail::async_receive_t{buffer}
            );
        }

        // Keep receiving datagrams into `sink` (owner thread, one per datagram, span valid only for the call,
        // synchronous) until stopped; sequence-shaped — compose with stop_when(...). `bufs` is the provided-
        // buffer pool (buffer_pool{ctx, count, size, bgid}: a kernel buf_ring on io_uring, one reused buffer
        // on epoll). Driver-specific lowering: io_uring multishot recv (fast path) vs single-shot loop.
        // Connected-socket only.
        template<typename BufferPool, typename Sink>
            requires std::same_as<BufferPool, typename IoScheduler::buffer_pool>
        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_receive_sequence(BufferPool& bufs, Sink sink) {
            auto sched = this->get_io_scheduler();
            if constexpr (requires { sched.receive_multishot(this->impl_, bufs, sink); }) {
                return sched.receive_multishot(this->impl_, bufs, std::move(sink));
            }
            else {
                return detail::as_error_code_sequence(
                    detail::receive_sequence_loop<scheduler_type>(this->impl_, sched, bufs, std::move(sink)));
            }
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_send(std::span<const std::byte> buffer) {
            // Datagram send is prompt+atomic, so async_datagram_send_t is unstoppable and schedule_io skips
            // the shutdown cancellation hook — the description carries the policy, not the call site.
            return this->get_io_scheduler().schedule_io(this->impl_, detail::async_datagram_send_t{buffer});
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_receive_from(std::span<std::byte> buffer) {
            return this->get_io_scheduler().schedule_io(
                this->impl_,
                detail::async_receive_from_t{buffer}
            );
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto async_send_to(std::span<const std::byte> buffer, const endpoint& peer) {
            // async_send_to_t is unstoppable (prompt datagram) -> schedule_io skips the cancellation hook.
            return this->get_io_scheduler().schedule_io(
                this->impl_,
                detail::async_send_to_t{buffer, peer}
            );
        }
    };

}
