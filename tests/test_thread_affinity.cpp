#include <doctest/doctest.h>
#include <coio/detail/config.h>
// pthread affinity is linux-only; elsewhere this TU compiles to a single skip marker (doctest.h must
// stay outside the guard — it supplies main()).
#if defined(__linux__)
#include <atomic>
#include <coio/core.h>
#include <coio/uring_runtime.h>
#include <coio/thread_affinity.h>

TEST_CASE("pin_to_core launcher drives a uring_runtime's workers to completion") {
    coio::uring_runtime rt{3, 4096, coio::pin_to_core{}};

    std::atomic<int> done{0};
    constexpr int n = 200;
    for (int i = 0; i < n; ++i) {
        rt.spawn(coio::just() | coio::then([&done] { done.fetch_add(1, std::memory_order_relaxed); }));
    }
    coio::this_thread::sync_wait(rt.join());

    CHECK_EQ(done.load(), n);
}
#else
TEST_CASE("pin_to_core launcher (skipped: linux-only)") { CHECK(true); }
#endif
