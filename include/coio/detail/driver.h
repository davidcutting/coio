#pragma once
#include <concepts>
#include <cstddef>
#include <coio/detail/operation_base.h>
#include <coio/detail/intrusive_list.h>

namespace coio::detail {
    // The executor's owner-only run queue. Drivers drain their ready completions into it; the executor
    // then calls operation_base::finish() on each.
    using ready_queue = intrusive_list<operation_base>;

    // What EVERY driver provides to the executor: a non-blocking poll() that flushes its own pending
    // submissions and drains up to `batch` completions into `ready`. Submission is op-facing and typed
    // per driver (driver.submit(Driver::operation&)), so it is deliberately NOT part of this contract.
    template<typename D>
    concept driver = requires(D& d, ready_queue& ready, std::size_t batch) {
        typename D::capability;
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
