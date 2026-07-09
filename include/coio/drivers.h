#pragma once
#include <coio/detail/config.h>
#include <coio/init.h>
#include <coio/time_loop.h>                  // timer_driver — always available
#if COIO_HAS_IO_URING
#include <coio/asyncio/uring_context.h>      // uring_driver
#endif
#if COIO_HAS_EPOLL
#include <coio/asyncio/epoll_context.h>      // epoll_driver
#endif

// The default driver registry for this build, and the customization-point wiring that lets
// coio::runtime::builder().pool(n).capability<...>() resolve capabilities to drivers automatically.
// FAST-PATH FIRST: io_uring leads on a build that has it, so "just give me networking and files" gets the
// fast path; epoll is the fallback (and can't serve files, so capability<file> on an epoll-only build is a
// clear compile error); the userspace timer heap is always last. Include this header wherever you use
// .capability<>(); .driver<>() (explicit backends / bespoke topologies) works from <coio/init.h> alone.
namespace coio {
    using default_drivers = type_list<
#if COIO_HAS_IO_URING
        uring_driver,
#endif
#if COIO_HAS_EPOLL
        epoll_driver,
#endif
        timer_driver
    >;

    // The builder entry point, with this build's registry baked in for .capability<>(). No customization
    // point / include-order games: default_drivers is a concrete type, defined once, here.
    //     coio::runtime::builder().pool(4).capability<capability::io>()   // -> fastest io driver on this build
    //     coio::runtime::builder<my_registry>() ...                       // override the registry if you like
    namespace runtime {
        template<typename Registry = default_drivers>
        [[nodiscard]] auto builder() noexcept -> runtime_builder<false, std::tuple<>, std::tuple<>, Registry> {
            return {};
        }
    }
}
