#pragma once
#include <memory>
#include <utility>
#include <kioto/base/concepts.h>

namespace kioto {
    template<unqualified_object T, simple_allocator Allocator, typename... Args>
    [[nodiscard]]
    KIOTO_ALWAYS_INLINE auto new_object(const Allocator& allocator, Args&&... args) -> T* {
        using alloc_t = typename std::allocator_traits<Allocator>::template rebind_alloc<T>;
        using alloc_traits = std::allocator_traits<alloc_t>;
        alloc_t alloc(allocator);
        auto ptr = alloc_traits::allocate(alloc, 1);
        try {
            alloc_traits::construct(alloc, ptr, std::forward<Args>(args)...);
            return ptr;
        }
        catch (...) {
            alloc_traits::deallocate(alloc, ptr, 1);
            throw;
        }
    }

    template<simple_allocator Allocator, typename T>
    KIOTO_ALWAYS_INLINE auto delete_object(const Allocator& allocator, T* ptr) noexcept -> void {
        using alloc_t = typename std::allocator_traits<Allocator>::template rebind_alloc<T>;
        using alloc_traits = std::allocator_traits<alloc_t>;
        alloc_t alloc(allocator);
        alloc_traits::destroy(alloc, ptr);
        alloc_traits::deallocate(alloc, ptr, 1);
    }
}
