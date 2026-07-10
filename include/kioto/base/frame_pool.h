#pragma once
#include <cstddef>
#include <new>

namespace kioto::detail {
    // Per-thread size-classed free-list recycling coroutine frames. NOT thread-safe by design: each worker
    // touches only its own (via tl_frame_pool). A frame freed on a different thread joins the FREEING
    // thread's pool — blocks migrate, safe because every block is ::operator new-backed (any pool can
    // ::operator delete any block at shutdown). A PINNED task allocs+frees on one thread: plain pop/push.
    class frame_pool {
    public:
        static constexpr std::size_t granularity = alignof(std::max_align_t);
        static constexpr std::size_t max_pooled = 8192;                      // larger frames go to malloc
        static constexpr std::size_t bucket_count = max_pooled / granularity;

        frame_pool() = default;
        frame_pool(const frame_pool&) = delete;
        auto operator= (const frame_pool&) -> frame_pool& = delete;

        ~frame_pool() {
            for (auto* head : free_lists_) {
                while (head != nullptr) {
                    auto* next = head->next;
                    ::operator delete(head);
                    head = next;
                }
            }
        }

        [[nodiscard]] auto allocate(std::size_t n) -> void* {
            const auto b = bucket_of(n);
            if (b >= bucket_count) return ::operator new(n);
            if (auto* head = free_lists_[b]) {
                free_lists_[b] = head->next;
                return head;
            }
            return ::operator new((b + 1) * granularity);
        }

        auto deallocate(void* ptr, std::size_t n) noexcept -> void {
            const auto b = bucket_of(n);
            if (b >= bucket_count) {
                ::operator delete(ptr);
                return;
            }
            auto* node = static_cast<free_node*>(ptr);
            node->next = free_lists_[b];
            free_lists_[b] = node;
        }

        [[nodiscard]] static constexpr auto bucket_of(std::size_t n) noexcept -> std::size_t {
            return n == 0 ? 0 : (n - 1) / granularity;
        }

    private:
        struct free_node { free_node* next; };
        free_node* free_lists_[bucket_count]{};
    };

    // The current thread's frame pool, or null on non-worker threads (then frames use malloc directly).
    inline thread_local frame_pool* tl_frame_pool = nullptr;

    [[nodiscard]] inline auto frame_allocate(std::size_t n) -> void* {
        if (auto* p = tl_frame_pool) return p->allocate(n);
        // No pool here, but round small frames to the bucket size anyway so a later free on a worker
        // (which pools by bucket) reuses a correctly-sized block.
        if (const auto b = frame_pool::bucket_of(n); b < frame_pool::bucket_count) {
            return ::operator new((b + 1) * frame_pool::granularity);
        }
        return ::operator new(n);
    }

    inline auto frame_deallocate(void* ptr, std::size_t n) noexcept -> void {
        if (auto* p = tl_frame_pool) {
            p->deallocate(ptr, n);
            return;
        }
        static_cast<void>(n);
        ::operator delete(ptr);
    }

    // A stateless std-style allocator that routes through the current thread's frame pool. Used as the
    // default coroutine-frame allocator so pinned tasks recycle their frames with no code changes.
    template<typename T = std::byte>
    struct frame_pool_allocator {
        using value_type = T;

        frame_pool_allocator() = default;

        template<typename U>
        constexpr frame_pool_allocator(const frame_pool_allocator<U>&) noexcept {}

        [[nodiscard]] auto allocate(std::size_t count) -> T* {
            return static_cast<T*>(frame_allocate(count * sizeof(T)));
        }

        auto deallocate(T* ptr, std::size_t count) noexcept -> void {
            frame_deallocate(ptr, count * sizeof(T));
        }

        template<typename U>
        auto operator== (const frame_pool_allocator<U>&) const noexcept -> bool { return true; }
    };
}
