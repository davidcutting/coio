#include <coio/detail/config.h>
#if COIO_HAS_IO_URING
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
            // Created DISABLED so the single-issuer owner binds when the ring is enabled on the run()
            // thread, not the constructing thread (lets a runtime build workers then pin each to a core).
            for (const unsigned flags : {
                     unsigned{IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_R_DISABLED},
                     unsigned{IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_R_DISABLED},
                 }) {
                const auto ec = ::io_uring_queue_init(entries, &uring, flags);
                if (ec == 0) return;
                if (ec != -EINVAL) throw std::system_error{-ec, std::system_category()};
            }
            throw std::system_error{EINVAL, std::system_category()};
        }

        [[noreturn]]
        auto sqe_exhuasted() -> void {
            // TODO: more proper handle
            std::terminate();
        }
    }

    auto uring_context::uring_node::submit_cancel() -> void { // owner thread only (touches the ring)
        auto sqe = context_.allocate_sqe();
        if (sqe == nullptr) [[unlikely]] {
            sqe_exhuasted();
        }
        ::io_uring_prep_cancel(sqe, this, 0);
        ::io_uring_sqe_set_data(sqe, nullptr);
        context_.submit_sqes();
    }

    auto uring_context::uring_node::do_cancel() -> void {
        if (context_.is_owner()) {
            submit_cancel();
            return;
        }
        // Cross-thread stop: NEVER touch the ring off-owner — that races the single issuer's submitter
        // and corrupts the SQ ring. Hand the op to the owner (which submits the cancel). The op stays
        // alive until its CQE is processed; cancel_state_ coordinates that so we never free a node that
        // is still linked in cancel_stack_. See uring_context::do_one.
        std::uint8_t expected = active;
        if (cancel_state_.compare_exchange_strong(expected, cancel_queued, std::memory_order_acq_rel)) {
            context_.request_cancel(*this);
        }
    }

    auto uring_context::request_cancel(uring_node& node) -> void {
        cancel_stack_.push(node);
        interrupt();
    }

    auto uring_context::drain_cancels() -> void { // owner thread only
        // Cheap acquire load avoids the atomic exchange (pop_all) every turn in the common no-cancel
        // case; a concurrent request_cancel always interrupt()s, so a missed turn is re-driven.
        if (cancel_stack_.empty()) return;
        auto* n = cancel_stack_.pop_all();
        while (n != nullptr) {
            auto* next = n->cancel_link_;
            if (n->completion_ready_) {
                // Its CQE already arrived while it sat in the stack; finish it now (it is off the stack
                // and its completion cannot race us — both happen on this thread).
                n->completion_ready_ = false;
                n->cancel_state_.store(uring_node::completed, std::memory_order_release);
                local_queue_.push_back(*n);
            }
            else {
                // Submit the cancel from the owner; the op's -ECANCELED CQE finishes it via classify.
                n->cancel_state_.store(uring_node::drained, std::memory_order_release);
                n->submit_cancel();
            }
            n = next;
        }
    }

    auto uring_context::scheduler::io_object::release() -> int {
        cancel();
        return std::exchange(fd_, -1);
    }

    auto uring_context::scheduler::io_object::cancel() -> void {
        if (fd_ == -1) return;
        // Teardown submits into the ring, so it MUST run on the owner (the single issuer). Tearing an
        // io_object down from another thread violates the thread-per-core contract — pin sticky I/O to
        // its worker (spawn_on(current_scheduler())) so its destructor runs on-owner. (do_cancel(),
        // by contrast, tolerates cross-thread stop by routing the cancel to the owner.)
        COIO_ASSERT(ctx_->is_owner());
        auto sqe = ctx_->allocate_sqe();
        if (sqe == nullptr) [[unlikely]] {
            sqe_exhuasted();
        }
        ::io_uring_prep_cancel_fd(sqe, fd_, IORING_ASYNC_CANCEL_ALL);
        ::io_uring_sqe_set_data(sqe, nullptr);
        ctx_->submit_sqes();
    }

    uring_context::uring_context(std::size_t entries, std::pmr::memory_resource& memory_resource) : loop_base(memory_resource) {
        init_uring(uring_, entries);
        lockfree_inject_ = true;
        park_aware_ = true;
    }

    uring_context::uring_context() : uring_context(default_uring_entries) {}

    uring_context::~uring_context() {
        ::io_uring_queue_exit(&uring_);
    }

    auto uring_context::do_one(bool infinite) -> bool {
        if (not enabled_) [[unlikely]] {
            if (const auto ec = ::io_uring_enable_rings(&uring_); ec < 0) {
                throw std::system_error{-ec, std::system_category()};
            }
            owner_.store(std::this_thread::get_id(), std::memory_order_relaxed);
            enabled_ = true;
        }

        const auto classify = [&](void* user_data, int res) {
            if (not user_data or user_data == this or user_data == reinterpret_cast<void*>(wake_user_data)) return;
            auto op = static_cast<uring_node*>(user_data);
            COIO_TSAN_ACQUIRE(op);
            op->complete(res);
            std::uint8_t expected = uring_node::active;
            if (op->cancel_state_.compare_exchange_strong(expected, uring_node::completed, std::memory_order_acq_rel)) {
                local_queue_.push_back(*op);
            }
            else if (expected == uring_node::cancel_queued) {
                // Cancel still queued for this op in cancel_stack_; finishing (freeing) it here would
                // dangle that link. Let drain_cancels() finish it once it is off the stack.
                op->completion_ready_ = true;
            }
            else {
                op->cancel_state_.store(uring_node::completed, std::memory_order_release);
                local_queue_.push_back(*op);
            }
        };

        for (;;) {
            drain_cancels();
            drain_inject();
            timer_queue_.take_ready_timers(local_queue_);

            if (auto* op = local_queue_.pop_front()) {
                op->finish();
                return true;
            }
            if (work_count_ == 0) return false;

            submit_sqes();

            if (infinite) {
                // Park protocol (see loop_base::notify): publish that we are about to block, then
                // re-check the sources a producer might have filled after the checks above. If we find
                // work, unpublish and handle it without blocking; otherwise block with parked_ set so
                // a producer that enqueues concurrently will see it and post a wakeup CQE.
                parked_.store(true, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_seq_cst);
                drain_inject();
                timer_queue_.take_ready_timers(local_queue_);
                if (auto* op = local_queue_.pop_front()) {
                    parked_.store(false, std::memory_order_relaxed);
                    op->finish();
                    return true;
                }
            }

            ::io_uring_cqe* cqe = nullptr;
            std::optional cqe_guard{scope_exit{[&] {
                ::io_uring_cqe_seen(&uring_, cqe);
            }}};
            int ec = 0;
            if (infinite) {
                using microseconds = std::chrono::duration<std::int64_t, std::micro>;
                if (const auto earliest = timer_queue_.earliest()) {
                    const auto now = std::chrono::steady_clock::now();
                    const auto usec = std::max(std::chrono::duration_cast<microseconds>(*earliest - now).count(), {});
                    ::__kernel_timespec timeout{
                        .tv_sec = usec / 1000'000,
                        .tv_nsec = (usec % 1000'000) * 1'000
                    };
                    ec = -::io_uring_wait_cqe_timeout(&uring_, &cqe, &timeout);
                }
                else {
                    ec = -::io_uring_wait_cqe_timeout(&uring_, &cqe, nullptr);
                }
            }
            else {
                ::__kernel_timespec immediate{};
                ec = -::io_uring_wait_cqe_timeout(&uring_, &cqe, &immediate);
            }
            parked_.store(false, std::memory_order_relaxed);

            if (ec == EINTR or ec == ETIME) {
                cqe_guard.reset();
                cqe = nullptr;
                ec = 0;
            }
            if (ec > 0) throw std::system_error{ec, std::system_category()};

            if (cqe) classify(::io_uring_cqe_get_data(cqe), cqe->res);
            cqe_guard.reset();

            while (true) {
                ::io_uring_cqe* peeked_cqes[8]{};
                const auto n = ::io_uring_peek_batch_cqe(&uring_, peeked_cqes, std::ranges::size(peeked_cqes));
                if (n == 0) break;
                scope_exit _{[this, n]() noexcept {
                    ::io_uring_cq_advance(&uring_, n);
                }};
                for (auto peeked_cqe : std::span(peeked_cqes, n)) {
                    classify(::io_uring_cqe_get_data(peeked_cqe), peeked_cqe->res);
                }
            }

            if (not infinite) {
                drain_cancels();
                drain_inject();
                timer_queue_.take_ready_timers(local_queue_);
                if (auto* op = local_queue_.pop_front()) {
                    op->finish();
                    return true;
                }
                return false;
            }
        }
    }

    auto uring_context::allocate_sqe() noexcept -> io_uring_sqe* {
        ::io_uring_sqe* sqe = ::io_uring_get_sqe(&uring_);
        for (std::size_t retry = 3u; sqe == nullptr and retry-- > 0;) {
            submit_sqes();
            sqe = ::io_uring_get_sqe(&uring_);
        }
        if (sqe) ++pending_sqes_;
        return sqe;
    }

    auto uring_context::submit_sqes() -> void { // called only on the owner thread
        if (pending_sqes_ == 0) return;
        const int n = ::io_uring_submit(&uring_);
        if (n < 0) [[unlikely]] std::terminate();
        COIO_ASSERT(pending_sqes_ >= std::size_t(n)); // NOLINT(*-use-integer-sign-comparison)
        pending_sqes_ -= n;
    }

    auto uring_context::post_submit_sqes() -> void { // called only on the owner thread
        if (pending_sqes_ >= submit_batch_size) submit_sqes();
    }

    auto uring_context::interrupt() -> void {
        // msg_ring via the register syscall needs no source ring, so ANY thread may call it, and it is
        // not a normal SQ submission — it never violates IORING_SETUP_SINGLE_ISSUER.
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
            buffer = {
                .iov_base = const_cast<std::byte*>(s.buffer.data()),
                .iov_len = s.buffer.size()
            };
            msg = {
                .msg_name = psa,
                .msg_namelen = len,
                .msg_iov = &buffer,
                .msg_iovlen = 1
            };
        }

        uring_sexpr_wrapper<async_receive_from_t>::type::type(async_receive_from_t s) noexcept {
            buffer = {
                .iov_base = s.buffer.data(),
                .iov_len = s.buffer.size()
            };
            msg = {
                .msg_name = &peer,
                .msg_namelen = sizeof(peer),
                .msg_iov = &buffer,
                .msg_iovlen = 1
            };
        }

        uring_sexpr_wrapper<async_connect_t>::type::type(async_connect_t s) noexcept : peer(endpoint_to_sockaddr_in(s.peer)) {}


        template<>
        auto uring_state_base_for<async_read_some_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_read(sqe, fd, buffer.data(), buffer.size(), -1);
        }

        template<>
        auto uring_state_base_for<async_write_some_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_write(sqe, fd, buffer.data(), buffer.size(), -1);
        }

        template<>
        auto uring_state_base_for<async_read_some_at_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_read(sqe, fd, buffer.data(), buffer.size(), offset);
        }

        template<>
        auto uring_state_base_for<async_write_some_at_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_write(sqe, fd, buffer.data(), buffer.size(), offset);
        }

        template<>
        auto uring_state_base_for<async_receive_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
        }


        template<>
        auto uring_state_base_for<async_send_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_send(sqe, fd, buffer.data(), buffer.size(), MSG_NOSIGNAL);
        }

        template<>
        auto uring_state_base_for<async_receive_from_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_recvmsg(sqe, fd, &msg, 0);
        }

        template<>
        auto uring_state_base_for<async_receive_from_t>::complete(int cqe_res) -> void {
            if (cqe_res < 0) {
                const std::error_code ec{-cqe_res, std::system_category()};
                if (ec == std::errc::operation_canceled) {
                    result.set_stopped();
                }
                else {
                    result.set_error(ec);
                }
            }
            else {
                result.set_value(sockaddr_storage_to_endpoint(peer), cqe_res);
            }
        }


        template<>
        auto uring_state_base_for<async_send_to_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_sendmsg(sqe, fd, &msg, MSG_NOSIGNAL);
        }


        template<>
        auto uring_state_base_for<async_accept_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            ::io_uring_prep_accept(sqe, fd, nullptr, nullptr, 0);
        }


        template<>
        auto uring_state_base_for<async_connect_t>::prepare(::io_uring_sqe* sqe) noexcept -> void {
            auto [psa, len] = to_sockaddr(peer);
            ::io_uring_prep_connect(sqe, fd, psa, len);
        }
    }
}

#include <coio/detail/suppress_pop.h> // IWYU pragma: keep

#endif
