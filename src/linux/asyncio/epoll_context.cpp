// ReSharper disable CppMemberFunctionMayBeConst
#include <coio/detail/config.h>
#if COIO_HAS_EPOLL
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <ranges>
#include <utility>
#include <coio/asyncio/epoll_context.h>
#include "../common.h"

namespace coio {
    namespace detail {
        reactor_interrupter::reactor_interrupter() {
            reader_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            if (reader_ != -1) [[likely]] { writer_ = reader_; return; }
            int pipedes[2];
            detail::throw_last_error(::pipe2(pipedes, O_CLOEXEC | O_NONBLOCK));
            reader_ = pipedes[0];
            writer_ = pipedes[1];
        }

        reactor_interrupter::~reactor_interrupter() {
            no_errno_here(::close(reader_));
            if (writer_ != reader_) [[unlikely]] no_errno_here(::close(writer_));
        }

        auto reactor_interrupter::interrupt() -> void {
            static constexpr std::uint64_t data = 1;
            static_cast<void>(::write(writer_, &data, sizeof(data)));
        }

        auto reactor_interrupter::reset() -> bool {
            if (writer_ == reader_) [[likely]] {
                while (true) {
                    std::uint64_t data;
                    const auto n = ::read(reader_, &data, sizeof(data));
                    if (n < 0) [[unlikely]] { if (errno == EINTR) continue; return false; }
                    return true;
                }
            }
            std::byte buffer[1024];
            while (true) {
                ssize_t bytes_read = ::read(reader_, buffer, sizeof(buffer));
                if (bytes_read == sizeof(buffer)) continue;
                if (bytes_read > 0) return true;
                if (bytes_read == 0) return false;
                if (errno == EINTR) continue;
                if (is_blocking_errno(errno)) return true;
                return false;
            }
        }

        auto epoll_prepare_fd(int fd) -> void {
            if (fd == -1) return;
            struct ::stat st{};
            if (::fstat(fd, &st) == -1) [[unlikely]] throw std::system_error{errno, std::system_category(), "fstat"};
            if (S_ISREG(st.st_mode) or S_ISDIR(st.st_mode)) [[unlikely]] {
                throw std::system_error{std::make_error_code(std::errc::operation_not_permitted), "the target file `fd` doesn't support epoll"};
            }
            const int flags = ::fcntl(fd, F_GETFL);
            if (flags == -1) [[unlikely]] throw std::system_error{errno, std::system_category(), "fcntl(fd, F_GETFL)"};
            if ((flags & O_NONBLOCK) == 0) {
                if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) [[unlikely]] {
                    throw std::system_error{errno, std::system_category(), "fcntl(fd, F_SETFL, ...)"};
                }
            }
        }

        auto epoll_arm_timerfd(std::chrono::steady_clock::time_point deadline) noexcept -> int {
            const int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
            if (fd == -1) return -1;
            const auto d = deadline.time_since_epoch(); // steady_clock == CLOCK_MONOTONIC on Linux
            const auto secs = std::chrono::duration_cast<std::chrono::seconds>(d);
            ::itimerspec its{};
            its.it_value.tv_sec = secs.count();
            its.it_value.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(d - secs).count();
            if (its.it_value.tv_sec == 0 and its.it_value.tv_nsec == 0) its.it_value.tv_nsec = 1; // 0,0 == disarm
            if (::timerfd_settime(fd, TFD_TIMER_ABSTIME, &its, nullptr) == -1) { ::close(fd); return -1; }
            return fd;
        }

        auto epoll_drain_timerfd(int fd) noexcept -> void {
            std::uint64_t expirations = 0;
            static_cast<void>(::read(fd, &expirations, sizeof(expirations)));
        }

        auto epoll_close_timer(int epoll_fd, int fd) noexcept -> void {
            static_cast<void>(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr));
            static_cast<void>(::close(fd));
        }
    }

    auto epoll_driver::operation::register_event(int event_type, std::uint32_t extra_flags) noexcept -> bool {
        std::scoped_lock _{data->fd_lock};
        const bool in_op_registered = data->in_op;
        const bool out_op_registered = data->out_op;
        if (event_type == EPOLLIN) {
            COIO_ASSERT(not in_op_registered && "input op initiated before a prior input op completed.");
        }
        else if (event_type == EPOLLOUT) {
            COIO_ASSERT(not out_op_registered && "output op initiated before a prior output op completed.");
        }
        else unreachable();

        std::uint32_t ev = event_type | extra_flags;
        int epoll_ctl_op = data->events == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
        if (in_op_registered) { ev |= EPOLLIN; epoll_ctl_op = EPOLL_CTL_MOD; }
        if (out_op_registered) { ev |= EPOLLOUT; epoll_ctl_op = EPOLL_CTL_MOD; }

        bool ok = ev == data->events;
        if (not ok) {
            ::epoll_event event{.events = ev, .data = {.ptr = data}};
            ok = ::epoll_ctl(driver_.epoll_fd_, epoll_ctl_op, fd, &event) == 0;
        }
        if (ok) [[likely]] {
            data->events = ev;
            registered_event_ = event_type; // remember so cancel can deregister exactly this slot
            if (event_type == EPOLLIN) data->in_op = this;
            else if (event_type == EPOLLOUT) data->out_op = this;
        }
        return ok;
    }

    epoll_driver::epoll_driver() {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        detail::throw_last_error(epoll_fd_);
        ::epoll_event event{.events = std::uint32_t(EPOLLIN | EPOLLET), .data = {.ptr = &interrupter_}};
        detail::throw_last_error(::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, interrupter_.watcher(), &event));
    }

    epoll_driver::~epoll_driver() { ::close(epoll_fd_); }

    auto epoll_driver::process_ready(detail::ready_queue& ready) -> void {
        if (event_count_ <= 0) { event_count_ = 0; return; }
        for (int i = 0; i < event_count_; ++i) {
            const auto& [event, data] = events_[i];
            COIO_ASSERT(data.ptr != nullptr);
            if (data.ptr == &interrupter_) { interrupter_.reset(); continue; }
            const auto fd_data = static_cast<per_fd_data*>(data.ptr);
            std::scoped_lock _{fd_data->fd_lock};
            std::array ops{
                std::pair{EPOLLIN, std::ref(fd_data->in_op)},
                std::pair{EPOLLOUT, std::ref(fd_data->out_op)}
            };
            for (auto [ev, op_ref] : ops) {
                auto& op = op_ref.get();
                if (event & (ev | EPOLLERR | EPOLLHUP)) {
                    if (op == nullptr or not op->perform()) continue;
                    ready.push_back(*op);
                    op = nullptr;
                }
            }
        }
        event_count_ = 0;
    }

    auto epoll_driver::poll(detail::ready_queue& ready, std::size_t /*batch*/) -> void {
        process_ready(ready); // drain events buffered by a prior poll_wait()
        event_count_ = ::epoll_wait(epoll_fd_, events_, static_cast<int>(std::ranges::size(events_)), 0);
        if (event_count_ == -1) {
            event_count_ = 0;
            if (errno != EINTR) detail::throw_last_error(-1, "epoll_wait");
        }
        process_ready(ready);
    }

    auto epoll_driver::poll_wait() -> void {
        do {
            event_count_ = ::epoll_wait(epoll_fd_, events_, static_cast<int>(std::ranges::size(events_)), -1);
        } while (event_count_ == -1 and errno == EINTR);
        if (event_count_ == -1) detail::throw_last_error(-1, "epoll_wait");
        // leave the events buffered; the next poll() drains them into the executor's run queue
    }

    // Drop op's registration for `event`. Returns whether it was still registered (i.e. hadn't already
    // completed). Posting the op back to the run queue is the CALLER's job (it holds the executor).
    auto epoll_driver::deregister(int event, operation* op) -> bool {
        COIO_ASSERT(op != nullptr and op->data != nullptr);
        std::scoped_lock _{op->data->fd_lock};
        const auto registered_op = event == EPOLLIN
            ? std::exchange(op->data->in_op, nullptr)
            : std::exchange(op->data->out_op, nullptr);
        COIO_ASSERT(registered_op == nullptr or registered_op == op);
        return registered_op != nullptr;
    }

    // io_handle teardown: deregister both slots, hand the ops back for the caller to post (as stopped).
    auto epoll_driver::cancel_all(per_fd_data* data) -> std::array<operation*, 2> {
        if (data == nullptr) return {nullptr, nullptr};
        std::scoped_lock _{data->fd_lock};
        return {std::exchange(data->in_op, nullptr), std::exchange(data->out_op, nullptr)};
    }

    auto epoll_driver::release_fd(int fd, per_fd_data* data) -> void {
        if (data == nullptr) return;
        std::scoped_lock _{data->fd_lock};
        if (data->events != 0) {
            detail::throw_last_error(::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
        }
    }

    namespace detail {
        template<> auto epoll_state_base_for<async_read_some_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            if (buffer.empty()) [[unlikely]] { result.set_value(0); return false; }
            if (not register_event(EPOLLIN, 0)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; }
            return true;
        }
        template<> auto epoll_state_base_for<async_read_some_t>::do_perform() noexcept -> bool {
            const ::ssize_t n = ::read(fd, buffer.data(), buffer.size());
            if (n == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(n);
            return true;
        }

        template<> auto epoll_state_base_for<async_write_some_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            if (buffer.empty()) [[unlikely]] { result.set_value(0); return false; }
            if (not register_event(EPOLLOUT, 0)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; }
            return true;
        }
        template<> auto epoll_state_base_for<async_write_some_t>::do_perform() noexcept -> bool {
            const ::ssize_t n = ::write(fd, buffer.data(), buffer.size());
            if (n == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(n);
            return true;
        }

        template<> auto epoll_state_base_for<async_receive_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            const ::ssize_t n = ::recv(fd, buffer.data(), buffer.size(), MSG_DONTWAIT);
            if (n == -1) {
                if (is_blocking_errno(errno)) { if (not register_event(EPOLLIN, EPOLLET)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; } return true; }
                result.set_error(std::error_code{errno, std::system_category()}); return false;
            }
            result.set_value(n); return false;
        }
        template<> auto epoll_state_base_for<async_receive_t>::do_perform() noexcept -> bool {
            const ::ssize_t n = ::recv(fd, buffer.data(), buffer.size(), MSG_DONTWAIT);
            if (n == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(n);
            return true;
        }

        // Stream and datagram send share the epoll readiness path (::send); they are separate descriptions
        // only so stoppability is deduced from the type. Identical bodies today.
        template<> auto epoll_state_base_for<async_stream_send_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            const ::ssize_t n = ::send(fd, buffer.data(), buffer.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n == -1) {
                if (is_blocking_errno(errno)) { if (not register_event(EPOLLOUT, EPOLLET)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; } return true; }
                result.set_error(std::error_code{errno, std::system_category()}); return false;
            }
            result.set_value(n); return false;
        }
        template<> auto epoll_state_base_for<async_stream_send_t>::do_perform() noexcept -> bool {
            const ::ssize_t n = ::send(fd, buffer.data(), buffer.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(n);
            return true;
        }
        template<> auto epoll_state_base_for<async_datagram_send_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            const ::ssize_t n = ::send(fd, buffer.data(), buffer.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n == -1) {
                if (is_blocking_errno(errno)) { if (not register_event(EPOLLOUT, EPOLLET)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; } return true; }
                result.set_error(std::error_code{errno, std::system_category()}); return false;
            }
            result.set_value(n); return false;
        }
        template<> auto epoll_state_base_for<async_datagram_send_t>::do_perform() noexcept -> bool {
            const ::ssize_t n = ::send(fd, buffer.data(), buffer.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(n);
            return true;
        }

        template<> auto epoll_state_base_for<async_receive_from_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            ::socklen_t len = sizeof(peer);
            const ::ssize_t n = ::recvfrom(fd, buffer.data(), buffer.size(), MSG_DONTWAIT, reinterpret_cast<::sockaddr*>(&peer), &len);
            if (n == -1) {
                if (is_blocking_errno(errno)) { if (not register_event(EPOLLIN, EPOLLET)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; } return true; }
                result.set_error(std::error_code{errno, std::system_category()}); return false;
            }
            result.set_value(sockaddr_storage_to_endpoint(peer), n); return false;
        }
        template<> auto epoll_state_base_for<async_receive_from_t>::do_perform() noexcept -> bool {
            ::socklen_t len = sizeof(peer);
            const ::ssize_t n = ::recvfrom(fd, buffer.data(), buffer.size(), MSG_DONTWAIT, reinterpret_cast<::sockaddr*>(&peer), &len);
            if (n == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(sockaddr_storage_to_endpoint(peer), n);
            return true;
        }

        template<> auto epoll_state_base_for<async_send_to_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            auto sa = endpoint_to_sockaddr_in(peer);
            auto [psa, len] = to_sockaddr(sa);
            ::ssize_t n = ::sendto(fd, buffer.data(), buffer.size(), MSG_DONTWAIT | MSG_NOSIGNAL, psa, len);
            if (n == -1) {
                if (is_blocking_errno(errno)) { if (not register_event(EPOLLOUT, EPOLLET)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; } return true; }
                result.set_error(std::error_code{errno, std::system_category()}); return false;
            }
            result.set_value(n); return false;
        }
        template<> auto epoll_state_base_for<async_send_to_t>::do_perform() noexcept -> bool {
            auto sa = endpoint_to_sockaddr_in(peer);
            auto [psa, len] = to_sockaddr(sa);
            ::ssize_t n = ::sendto(fd, buffer.data(), buffer.size(), MSG_DONTWAIT | MSG_NOSIGNAL, psa, len);
            if (n == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(n);
            return true;
        }

        template<> auto epoll_state_base_for<async_accept_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            if (not register_event(EPOLLIN, 0)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; }
            return true;
        }
        template<> auto epoll_state_base_for<async_accept_t>::do_perform() noexcept -> bool {
            auto accepted_ = ::accept4(fd, nullptr, nullptr, SOCK_NONBLOCK);
            if (accepted_ == -1) { if (is_blocking_errno(errno)) [[unlikely]] return false; result.set_error(std::error_code{errno, std::system_category()}); }
            else result.set_value(to_handle(accepted_)); // wrap the minted fd at the mint site (same boundary as uring)
            return true;
        }

        template<> auto epoll_state_base_for<async_connect_t>::do_start() noexcept -> bool {
            if (fd == -1) [[unlikely]] { result.set_error(std::make_error_code(std::errc::bad_file_descriptor)); return false; }
            const auto flags = ::fcntl(fd, F_GETFL);
            if (flags == -1) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; }
            if ((flags & O_NONBLOCK) == 0) {
                if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; }
            }
            auto sa = endpoint_to_sockaddr_in(peer);
            auto [psa, len] = to_sockaddr(sa);
            const int ec = ::connect(fd, psa, len) == -1 ? errno : 0;
            if ((flags & O_NONBLOCK) == 0) {
                if (::fcntl(fd, F_SETFL, flags) == -1) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; }
            }
            if (ec != 0) {
                if (ec == EINPROGRESS or ec == EAGAIN) { if (not register_event(EPOLLOUT, 0)) [[unlikely]] { result.set_error(std::error_code{errno, std::system_category()}); return false; } return true; }
                result.set_error(std::error_code{ec, std::system_category()}); return false;
            }
            result.set_value(); return false;
        }
        template<> auto epoll_state_base_for<async_connect_t>::do_perform() noexcept -> bool {
            int ec = 0;
            ::socklen_t len = sizeof(ec);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &ec, &len) == -1) result.set_error(std::error_code{errno, std::system_category()});
            else if (ec != 0) result.set_error(std::error_code{ec, std::system_category()});
            else result.set_value();
            return true;
        }
    }
}
#endif
