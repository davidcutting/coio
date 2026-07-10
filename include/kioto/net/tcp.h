#pragma once
#include <kioto/net/socket.h>
#include <kioto/net/resolver.h>

namespace kioto {
    class tcp {
    public:
        template<io_scheduler IoScheduler>
        using acceptor = basic_socket_acceptor<tcp, IoScheduler>;

        template<io_scheduler IoScheduler>
        using socket = basic_stream_socket<tcp, IoScheduler>;

        template<scheduler Scheduler>
        using resolver = basic_resolver<tcp, Scheduler>;

        // tcp socket options:
        using no_delay = detail::socket::no_delay;

    private:
        explicit tcp(int family) noexcept : family_(family) {}

    public:
        tcp() noexcept;   // IPv4 by default

        [[nodiscard]] static auto v4() noexcept -> tcp;
        [[nodiscard]] static auto v6() noexcept -> tcp;

        [[nodiscard]]
        auto family() const noexcept -> int {
            return family_;
        }

        [[nodiscard]]
        static auto type() noexcept -> int;

        [[nodiscard]]
        static auto protocol_id() noexcept -> int;

        [[nodiscard]]
        friend auto operator== (const tcp& lhs, const tcp& rhs) noexcept -> bool = default;

    private:
        int family_;
    };
}
