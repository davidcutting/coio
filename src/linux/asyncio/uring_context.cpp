#include <coio/detail/config.h>
#if COIO_HAS_IO_URING
#include <chrono>
#include <limits>
#include <thread>
#include <coio/asyncio/uring_context.h>
#include <coio/utils/scope_exit.h>
#include <coio/detail/suppress_push.h> // IWYU pragma: keep
#include "../common.h"

namespace coio {
    namespace {
        constexpr std::size_t default_uring_entries = 4096;
        constexpr std::size_t submit_batch_size = 32;
        constexpr std::uint64_t wake_user_data = 0x636f'696f'7761'6b65ULL; // "coiowake"

        auto init_uring(::io_uring& uring, std::size_t entries) -> void {
            if (entries > std::numeric_limits<unsigned>::max()) {
                throw std::system_error{std::make_error_code(std::errc::value_too_large)};
            }
            // DISABLED so the single-issuer owner binds when the ring is enabled on the run() thread.
            // SUBMIT_ALL: a bad SQE doesn't abort the rest of the batch (we submit in batches). COOP_TASKRUN
            // is only added to the non-DEFER fallback tier -- DEFER_TASKRUN already implies cooperative task
            // running (it runs task work only on wait), so pairing COOP with it would be redundant.
            for (const unsigned flags : {
                     unsigned{IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SUBMIT_ALL | IORING_SETUP_R_DISABLED},
                     unsigned{IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SUBMIT_ALL | IORING_SETUP_R_DISABLED},
                 }) {
                const auto ec = ::io_uring_queue_init(entries, &uring, flags);
                if (ec == 0) return;
                if (ec != -EINVAL) throw std::system_error{-ec, std::system_category()};
            }
            throw std::system_error{EINVAL, std::system_category()};
        }

        [[noreturn]] auto sqe_exhausted() -> void { std::terminate(); }
    }

    uring_driver::uring_driver(std::size_t entries) { init_uring(uring_, entries); }
    uring_driver::uring_driver() : uring_driver(default_uring_entries) {}
    uring_driver::~uring_driver() { ::io_uring_queue_exit(&uring_); }

    auto uring_driver::operation::submit_cancel() -> void { // owner thread only (touches the ring)
        auto sqe = driver_.allocate_sqe();
        if (sqe == nullptr) [[unlikely]] sqe_exhausted();
        ::io_uring_prep_cancel(sqe, this, 0);
        ::io_uring_sqe_set_data(sqe, nullptr);
        driver_.submit_sqes();
    }

    auto uring_driver::operation::do_cancel() -> void {
        if (driver_.is_owner()) {
            submit_cancel();
            return;
        }
        // Cross-thread stop: never touch the ring off-owner. Hand the op to the owner, which submits the
        // cancel. cancel_state_ keeps the node alive until its CQE is processed. See classify/drain_cancels.
        std::uint8_t expected = active;
        if (cancel_state_.compare_exchange_strong(expected, cancel_queued, std::memory_order_acq_rel)) {
            driver_.request_cancel(*this);
        }
    }

    auto uring_driver::request_cancel(operation& op) -> void {
        cancel_stack_.push(op);
        wake_up();
    }

    auto uring_driver::drain_cancels(detail::ready_queue& ready) -> void { // owner thread only
        if (cancel_stack_.empty()) return;
        auto* n = cancel_stack_.pop_all();
        while (n != nullptr) {
            auto* next = n->cancel_link_;
            if (n->completion_ready_) {
                n->completion_ready_ = false;
                n->cancel_state_.store(operation::completed, std::memory_order_release);
                ready.push_back(*n);
            }
            else {
                n->cancel_state_.store(operation::drained, std::memory_order_release);
                n->submit_cancel();
            }
            n = next;
        }
    }

    auto uring_driver::cancel_fd(int fd) -> void {
        // Teardown submits into the ring, so it MUST run on the owner (the single issuer).
        COIO_ASSERT(is_owner());
        auto sqe = allocate_sqe();
        if (sqe == nullptr) [[unlikely]] sqe_exhausted();
        ::io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
        ::io_uring_sqe_set_data(sqe, nullptr);
        submit_sqes();
    }

    auto uring_driver::classify(detail::ready_queue& ready, void* user_data, int res, unsigned flags) -> void {
        if (not user_data or user_data == reinterpret_cast<void*>(wake_user_data)) return;
        auto op = static_cast<operation*>(user_data);
        COIO_TSAN_ACQUIRE(op);
        // Multishot ops consume many CQEs from one SQE: on_completion delivers this datagram and returns
        // false while still armed (IORING_CQE_F_MORE), so we neither finish nor free until it returns true.
        if (not op->on_completion(res, flags)) return;
        std::uint8_t expected = operation::active;
        if (op->cancel_state_.compare_exchange_strong(expected, operation::completed, std::memory_order_acq_rel)) {
            ready.push_back(*op);
        }
        else if (expected == operation::cancel_queued) {
            // Cancel still queued in cancel_stack_; drain_cancels() finishes it once it is off the stack.
            op->completion_ready_ = true;
        }
        else {
            op->cancel_state_.store(operation::completed, std::memory_order_release);
            ready.push_back(*op);
        }
    }

    auto uring_driver::poll(detail::ready_queue& ready, std::size_t batch) -> void {
        if (not enabled_) [[unlikely]] {
            if (const auto ec = ::io_uring_enable_rings(&uring_); ec < 0) {
                throw std::system_error{-ec, std::system_category()};
            }
            owner_.store(std::this_thread::get_id(), std::memory_order_relaxed);
            enabled_ = true;
        }
        submit_sqes();
        drain_cancels(ready);

        std::size_t reaped = 0;
        while (reaped < batch) {
            ::io_uring_cqe* cqes[8]{};
            const auto want = std::min<std::size_t>(std::ranges::size(cqes), batch - reaped);
            const auto n = ::io_uring_peek_batch_cqe(&uring_, cqes, static_cast<unsigned>(want));
            if (n == 0) break;
            scope_exit advance{[this, n]() noexcept { ::io_uring_cq_advance(&uring_, n); }};
            for (auto* cqe : std::span(cqes, n)) {
                classify(ready, ::io_uring_cqe_get_data(cqe), cqe->res, cqe->flags);
            }
            reaped += n;
        }
    }

    auto uring_driver::poll_wait() -> void {
        // One enter flushes any pending SQEs *and* blocks for at least one CQE (native TIMEOUT SQEs wake
        // this; no deadline needed). Replaces the old submit()+wait_cqe() two-syscall path. The return is
        // the number of SQEs submitted (like io_uring_submit), so reconcile pending_sqes_ the same way.
        int ret = 0;
        do {
            ret = ::io_uring_submit_and_wait(&uring_, 1);
        } while (ret == -EINTR);
        if (ret < 0 and ret != -ETIME) throw std::system_error{-ret, std::system_category()};
        if (ret > 0) {
            COIO_ASSERT(pending_sqes_ >= std::size_t(ret)); // NOLINT(*-use-integer-sign-comparison)
            pending_sqes_ -= ret;
        }
        // Leave the CQE for the next poll() to reap.
    }

    auto uring_driver::submit(operation& op) -> bool {
        auto sqe = allocate_sqe();
        if (sqe == nullptr) return false;
        op.prepare(sqe);
        ::io_uring_sqe_set_data(sqe, static_cast<operation*>(&op));
        COIO_TSAN_RELEASE(static_cast<operation*>(&op));
        post_submit_sqes();
        return true;
    }

    auto uring_driver::allocate_sqe() noexcept -> ::io_uring_sqe* {
        ::io_uring_sqe* sqe = ::io_uring_get_sqe(&uring_);
        for (std::size_t retry = 3u; sqe == nullptr and retry-- > 0;) {
            submit_sqes();
            sqe = ::io_uring_get_sqe(&uring_);
        }
        if (sqe) ++pending_sqes_;
        return sqe;
    }

    auto uring_driver::submit_sqes() -> void { // owner thread only
        if (pending_sqes_ == 0) return;
        const int n = ::io_uring_submit(&uring_);
        if (n < 0) [[unlikely]] std::terminate();
        COIO_ASSERT(pending_sqes_ >= std::size_t(n)); // NOLINT(*-use-integer-sign-comparison)
        pending_sqes_ -= n;
    }

    auto uring_driver::post_submit_sqes() -> void { // owner thread only
        if (pending_sqes_ >= submit_batch_size) submit_sqes();
    }

    auto uring_driver::wake_up() noexcept -> void {
        // msg_ring via the register syscall needs no source ring, so ANY thread may call it, and it never
        // violates IORING_SETUP_SINGLE_ISSUER.
        ::io_uring_sqe sqe{};
        ::io_uring_prep_msg_ring(&sqe, uring_.ring_fd, 0, wake_user_data, 0);
        int res = 0;
        do {
            res = ::io_uring_register_sync_msg(&sqe);
        } while (res == -EINTR);
    }

    namespace detail {
        uring_sexpr_wrapper<async_send_to_t>::type::type(async_send_to_t s) noexcept {
            peer = endpoint_to_sockaddr_in(s.peer);
            auto [psa, len] = to_sockaddr(peer);
            buffer = {.iov_base = const_cast<std::byte*>(s.buffer.data()), .iov_len = s.buffer.size()};
            msg = {.msg_name = psa, .msg_namelen = len, .msg_iov = &buffer, .msg_iovlen = 1};
        }

        uring_sexpr_wrapper<async_receive_from_t>::type::type(async_receive_from_t s) noexcept {
            buffer = {.iov_base = s.buffer.data(), .iov_len = s.buffer.size()};
            msg = {.msg_name = &peer, .msg_namelen = sizeof(peer), .msg_iov = &buffer, .msg_iovlen = 1};
        }

        uring_sexpr_wrapper<async_connect_t>::type::type(async_connect_t s) noexcept : peer(endpoint_to_sockaddr_in(s.peer)) {}

        uring_sexpr_wrapper<async_sleep_t>::type::type(async_sleep_t s) noexcept {
            const auto d = s.deadline.time_since_epoch(); // steady_clock == CLOCK_MONOTONIC on Linux
            const auto secs = std::chrono::duration_cast<std::chrono::seconds>(d);
            ts.tv_sec = secs.count();
            ts.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(d - secs).count();
        }

        template<> auto uring_state_base_for<async_read_some_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_read(sqe, fd, buffer.data(), buffer.size(), -1);
        }
        template<> auto uring_state_base_for<async_write_some_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_write(sqe, fd, buffer.data(), buffer.size(), -1);
        }
        template<> auto uring_state_base_for<async_read_some_at_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_read(sqe, fd, buffer.data(), buffer.size(), offset);
        }
        template<> auto uring_state_base_for<async_write_some_at_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_write(sqe, fd, buffer.data(), buffer.size(), offset);
        }
        template<> auto uring_state_base_for<async_receive_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
        }
        template<> auto uring_state_base_for<async_send_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_send(sqe, fd, buffer.data(), buffer.size(), MSG_NOSIGNAL);
        }
        template<> auto uring_state_base_for<async_receive_from_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_recvmsg(sqe, fd, &msg, 0);
        }
        template<> auto uring_state_base_for<async_receive_from_t>::on_completion(int cqe_res, unsigned) -> bool {
            if (cqe_res < 0) {
                const std::error_code ec{-cqe_res, std::system_category()};
                if (ec == std::errc::operation_canceled) result.set_stopped();
                else result.set_error(ec);
            }
            else result.set_value(sockaddr_storage_to_endpoint(peer), cqe_res);
            return true;
        }
        template<> auto uring_state_base_for<async_send_to_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_sendmsg(sqe, fd, &msg, MSG_NOSIGNAL);
        }
        template<> auto uring_state_base_for<async_accept_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_accept(sqe, fd, nullptr, nullptr, 0);
        }
        template<> auto uring_state_base_for<async_accept_t>::on_completion(int cqe_res, unsigned) -> bool {
            if (cqe_res < 0) {
                const std::error_code ec{-cqe_res, std::system_category()};
                if (ec == std::errc::operation_canceled) result.set_stopped();
                else result.set_error(ec);
            }
            // Wrap the freshly-minted fd into an opaque handle right at the mint site (the backend boundary),
            // so the accept sender advertises native_handle and no raw fd escapes upward.
            else result.set_value(to_handle(cqe_res));
            return true;
        }
        template<> auto uring_state_base_for<async_connect_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            auto [psa, len] = to_sockaddr(peer);
            ::io_uring_prep_connect(sqe, fd, psa, len);
        }

        // A1: the timer. TIMEOUT SQE with an absolute monotonic deadline; -ETIME means it fired.
        template<> auto uring_state_base_for<async_sleep_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_timeout(sqe, &ts, 0, IORING_TIMEOUT_ABS);
        }
        template<> auto uring_state_base_for<async_sleep_t>::on_completion(int cqe_res, unsigned) -> bool {
            if (cqe_res == -ETIME or cqe_res == 0) result.set_value();
            else if (cqe_res == -ECANCELED) result.set_stopped();
            else result.set_error(std::error_code{-cqe_res, std::system_category()});
            return true;
        }
    }
}

#include <coio/detail/suppress_pop.h> // IWYU pragma: keep

#endif
