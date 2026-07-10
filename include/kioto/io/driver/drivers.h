#pragma once
#include <kioto/base/config.h>
#include <kioto/runtime/init.h>
#include <kioto/io/time_loop.h>                  // timer_driver — always available
#if KIOTO_HAS_IO_URING
#include <kioto/io/driver/uring_context.h>      // uring_driver
#endif
#if KIOTO_HAS_EPOLL
#include <kioto/io/driver/epoll_context.h>      // epoll_driver
#endif
#if KIOTO_HAS_IOCP
#include <kioto/io/driver/iocp_context.h>       // iocp_driver (Windows)
#endif

// The default driver registry for this build. FAST-PATH FIRST: the native completion driver leads
// (io_uring on Linux, IOCP on Windows — both serve io+file), then epoll (Linux fallback, no file support
// -> capability<file> on an epoll-only build is a compile error), then the timer heap last. capability<>
// resolution picks the first provider. Include this for .capability<>(); .driver<>() needs only init.h.
namespace kioto {
    using default_drivers = type_list<
#if KIOTO_HAS_IO_URING
        uring_driver,
#endif
#if KIOTO_HAS_EPOLL
        epoll_driver,
#endif
#if KIOTO_HAS_IOCP
        iocp_driver,
#endif
        timer_driver
    >;

    // Builder entry point with this build's registry baked in for .capability<>().
    //     kioto::runtime::builder().pool(4).capability<capability::io>()   // fastest io driver on this build
    //     kioto::runtime::builder<my_registry>() ...                       // override the registry
    namespace runtime {
        template<typename Registry = default_drivers>
        [[nodiscard]] auto builder() noexcept -> runtime_builder<false, std::tuple<>, std::tuple<>, Registry> {
            return {};
        }
    }
}
