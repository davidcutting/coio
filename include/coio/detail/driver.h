#pragma once
#include <concepts>
#include <cstddef>
#include <coio/detail/operation_base.h>
#include <coio/detail/intrusive_list.h>
#include <coio/utils/type_traits.h>

// Capability tags name SERVICES a driver can offer, not drivers themselves: several drivers may
// provide the same capability (the timer heap, epoll's timerfd and io_uring's timeout op all
// provide `timer`). A driver advertises its set via `using capabilities = type_list<...>`;
// executor::get_driver<Cap> resolves to the FIRST driver (in declaration order) providing Cap.
namespace coio::capability {
    struct timer {}; // timed scheduling: schedule_at / schedule_after
    struct io {};    // sockets/pipes: the pollable io-op family (read/write/recv/send/accept/...)
    struct file {};  // REGULAR files: not pollable, so epoll can't serve them — io_uring/IOCP only
    // A ROLE tag, not an op-family: "a dedicated reactor-less worker" (park/wake + run posted work). Only
    // the lightweight worker driver claims it, so .capability<cpu>() resolves to that — NOT to a heavy
    // io_uring/epoll reactor (which can run CPU work via plain placement, but shouldn't be *chosen* for it).
    struct cpu {};
}

namespace coio::detail {
    // The executor's owner-only run queue. Drivers drain their ready completions into it; the executor
    // then calls operation_base::finish() on each.
    using ready_queue = intrusive_list<operation_base>;

    // What EVERY driver provides to the executor: a non-blocking poll() that flushes its own pending
    // submissions and drains up to `batch` completions into `ready`. Submission is op-facing and typed
    // per driver (driver.submit(Driver::operation&)), so it is deliberately NOT part of this contract.
    template<typename D>
    concept driver = requires(D& d, ready_queue& ready, std::size_t batch) {
        typename D::capabilities;
        d.poll(ready, batch);
    };

    // The executor's FIRST driver is the wait-owner: it alone can block the thread (until a completion
    // is ready) and be woken. wake_up() is any-thread, re-entrant, and durable — a wake racing the park
    // window still makes poll_wait() return.
    template<typename D>
    concept wait_driver = driver<D> and requires(D& d) {
        d.poll_wait();
        { d.wake_up() } noexcept;
    };
}
