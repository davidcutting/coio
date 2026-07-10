#pragma once
#include <kioto/net/socket.h>
#include <kioto/net/resolver.h>

namespace kioto {
    class udp {
    public:
        template<io_scheduler IoScheduler>
        using socket = basic_datagram_socket<udp, IoScheduler>;

        template<scheduler Scheduler>
        using resolver = basic_resolver<udp, Scheduler>;

    private:
        explicit udp(int family) noexcept : family_(family) {}

    public:
        udp() noexcept;   // IPv4 by default

        [[nodiscard]] static auto v4() noexcept -> udp;
        [[nodiscard]] static auto v6() noexcept -> udp;

        [[nodiscard]]
        auto family() const noexcept -> int {
            return family_;
        }

        [[nodiscard]]
        static auto type() noexcept -> int;

        [[nodiscard]]
        static auto protocol_id() noexcept -> int;

        [[nodiscard]]
        friend auto operator== (const udp& lhs, const udp& rhs) noexcept -> bool = default;

    private:
        int family_;
    };
}
