#pragma once
#include <cassert>
#include <version>
#ifdef __linux__
#include <linux/version.h>
#endif

#ifdef __SANITIZE_THREAD__
#define KIOTO_BUILD_WITH_TSAN 1
#elif defined(__clang__)
#if __has_feature(thread_sanitizer)
#define KIOTO_BUILD_WITH_TSAN 1
#else
#define KIOTO_BUILD_WITH_TSAN 0
#endif
#else
#define KIOTO_BUILD_WITH_TSAN 0
#endif

#if KIOTO_BUILD_WITH_TSAN and __has_include(<sanitizer/tsan_interface.h>)
#include <sanitizer/tsan_interface.h>
#define KIOTO_TSAN_ACQUIRE(addr) __tsan_acquire(addr)
#define KIOTO_TSAN_RELEASE(addr) __tsan_release(addr)
#else
#define KIOTO_TSAN_ACQUIRE(addr) static_cast<void>(addr)
#define KIOTO_TSAN_RELEASE(addr) static_cast<void>(addr)
#endif

#define KIOTO_CXX_STD98 199711L
#define KIOTO_CXX_STD11 201103L
#define KIOTO_CXX_STD14 201402L
#define KIOTO_CXX_STD17 201703L
#define KIOTO_CXX_STD20 202002L
#define KIOTO_CXX_STD23 202302L
#define KIOTO_CXX_STD26 202603L

#ifdef _MSVC_LANG
#define KIOTO_CXX_STANDARD _MSVC_LANG
#else
#define KIOTO_CXX_STANDARD __cplusplus
#endif

#if KIOTO_CXX_STANDARD < KIOTO_CXX_STD20 or not defined(__cpp_impl_coroutine) or not defined(__cpp_lib_coroutine)
#error "kioto requires a C++20 compiler that supports coroutine."
#endif

#ifdef __clang__
#define KIOTO_CXX_COMPILER_CLANG 1
#define KIOTO_CXX_COMPILER_GCC 0
#define KIOTO_CXX_COMPILER_MSVC 0
#elif defined(__GNUC__)
#define KIOTO_CXX_COMPILER_CLANG 0
#define KIOTO_CXX_COMPILER_GCC 1
#define KIOTO_CXX_COMPILER_MSVC 0
#elif defined(_MSC_VER)
#define KIOTO_CXX_COMPILER_CLANG 0
#define KIOTO_CXX_COMPILER_GCC 0
#define KIOTO_CXX_COMPILER_MSVC 1
#else
#error "kioto requires the C++ compiler is clang, gcc or msvc."
#endif

#if KIOTO_CXX_COMPILER_CLANG
#define KIOTO_ALWAYS_INLINE [[clang::always_inline]] inline
#elif KIOTO_CXX_COMPILER_GCC
#define KIOTO_ALWAYS_INLINE [[gnu::always_inline]] inline
#elif KIOTO_CXX_COMPILER_MSVC
#define KIOTO_ALWAYS_INLINE [[msvc::forceinline]] inline
#endif

#if KIOTO_CXX_STANDARD >= KIOTO_CXX_STD23 and defined(__cpp_static_call_operator)
#define KIOTO_STATIC_CALL_OP static
#define KIOTO_STATIC_CALL_OP_CONST
#else
#define KIOTO_STATIC_CALL_OP
#define KIOTO_STATIC_CALL_OP_CONST const
#endif

#define KIOTO_ASSERT(...) assert(__VA_ARGS__)

#if __has_cpp_attribute(assume)
#define KIOTO_ASSUME(expr) [[assume(expr)]]
#elif KIOTO_CXX_COMPILER_CLANG
#define KIOTO_ASSUME(expr) __builtin_assume(expr)
#elif KIOTO_CXX_COMPILER_GCC
#define KIOTO_ASSUME(expr) __attribute__((assume(expr)))
#elif KIOTO_CXX_COMPILER_MSVC
#define KIOTO_ASSUME(expr) __assume(expr)
#endif

#ifdef __cpp_deleted_function
#define KIOTO_DELETE_WITH_REASON(reason) delete(reason)
#else
#define KIOTO_DELETE_WITH_REASON(reason) delete
#endif

#if KIOTO_CXX_STANDARD >= KIOTO_CXX_STD23 and defined(__cpp_lib_start_lifetime_as)
#define KIOTO_START_LIFETIME_AS(type, address) void(std::start_lifetime_as<type>(address))
#define KIOTO_START_LIFETIME_AS_ARRAY(type, address, size) void(std::start_lifetime_as_array<type>(address, size))
#else
#define KIOTO_START_LIFETIME_AS(type, address) void(0)
#define KIOTO_START_LIFETIME_AS_ARRAY(type, address, size) void(0)
#endif

#if __has_cpp_attribute(no_unique_address)
#define KIOTO_NO_UNIQUE_ADDRESS [[no_unique_address]]
#elif KIOTO_CXX_COMPILER_MSVC and __has_cpp_attribute(msvc::no_unique_address)
#define KIOTO_NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#else
#define KIOTO_NO_UNIQUE_ADDRESS
#endif

#if defined(_WIN32) or defined(_WIN64)
#define KIOTO_OS_WINDOWS 1
#define KIOTO_OS_LINUX 0
#elif defined(__linux__)
#define KIOTO_OS_WINDOWS 0
#define KIOTO_OS_LINUX 1
#else
#error "unsupported operation system."
#endif

#if KIOTO_OS_WINDOWS
#define KIOTO_HAS_EPOLL 0
#define KIOTO_HAS_IO_URING 0
#if __has_include(<ioapiset.h>)
#define KIOTO_HAS_IOCP 1
#else
#define KIOTO_HAS_IOCP 0
#endif
#elif KIOTO_OS_LINUX
#define KIOTO_HAS_IOCP 0
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 5, 45) and __has_include(<sys/epoll.h>)
#define KIOTO_HAS_EPOLL 1
#else
#define KIOTO_HAS_EPOLL 0
#endif
#if __has_include(<liburing.h>)
#define KIOTO_HAS_IO_URING 1
#else
#define KIOTO_HAS_IO_URING 0
#endif
#endif

#define KIOTO_STRINGIZE_IMPL(...) #__VA_ARGS__
#define KIOTO_STRINGIZE(...) KIOTO_STRINGIZE_IMPL(__VA_ARGS__)

#if KIOTO_CXX_COMPILER_MSVC
#define KIOTO_PRAGMA(ARG) __pragma(ARG)
#else
#define KIOTO_PRAGMA(ARG) _Pragma(KIOTO_STRINGIZE(ARG))
#endif

#if KIOTO_CXX_COMPILER_CLANG
#define KIOTO_CLANG_SUPPRESS_PUSH() KIOTO_PRAGMA(clang diagnostic push)
#define KIOTO_CLANG_IGNORE(...) KIOTO_PRAGMA(clang diagnostic ignored __VA_ARGS__)
#define KIOTO_CLANG_SUPPRESS_POP() KIOTO_PRAGMA(clang diagnostic pop)
#else
#define KIOTO_CLANG_SUPPRESS_PUSH()
#define KIOTO_CLANG_IGNORE(...)
#define KIOTO_CLANG_SUPPRESS_POP()
#endif

#if KIOTO_CXX_COMPILER_GCC
#define KIOTO_GCC_SUPPRESS_PUSH() KIOTO_PRAGMA(gcc diagnostic push)
#define KIOTO_GCC_IGNORE(...) KIOTO_PRAGMA(gcc diagnostic ignored __VA_ARGS__)
#define KIOTO_GCC_SUPPRESS_POP() KIOTO_PRAGMA(gcc diagnostic pop)
#else
#define KIOTO_GCC_SUPPRESS_PUSH()
#define KIOTO_GCC_IGNORE(...)
#define KIOTO_GCC_SUPPRESS_POP()
#endif

#if KIOTO_CXX_COMPILER_MSVC
#define KIOTO_MSVC_SUPPRESS_PUSH() KIOTO_PRAGMA(warning(push))
#define KIOTO_MSVC_IGNORE(...) KIOTO_PRAGMA(warning(disable : __VA_ARGS__))
#define KIOTO_MSVC_SUPPRESS_POP() KIOTO_PRAGMA(warning(pop))
#else
#define KIOTO_MSVC_SUPPRESS_PUSH()
#define KIOTO_MSVC_IGNORE(...)
#define KIOTO_MSVC_SUPPRESS_POP()
#endif
