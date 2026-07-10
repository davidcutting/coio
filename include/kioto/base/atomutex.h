#pragma once
#include <atomic>
#include <kioto/base/config.h>

namespace kioto {
    class atomutex {
    public:
        atomutex() = default;

        atomutex(const atomutex&) = delete;

        ~atomutex() = default;

        auto operator= (const atomutex&) -> atomutex& = delete;

        KIOTO_ALWAYS_INLINE auto lock() noexcept -> void {
            while (flag_.test_and_set(std::memory_order_acquire)) {
                flag_.wait(true, std::memory_order_relaxed);
            }
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE auto try_lock() noexcept -> bool {
            return not flag_.test_and_set(std::memory_order_acquire);
        }

        KIOTO_ALWAYS_INLINE auto unlock() noexcept -> void {
            flag_.clear(std::memory_order_release);
            flag_.notify_one();
        }

    private:
        std::atomic_flag flag_{};
    };
}
