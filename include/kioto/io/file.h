#pragma once
#include <bit>
#include <filesystem>
#include <utility>
#include <kioto/core.h>
#include <kioto/exec/async_result.h>
#include <kioto/base/error.h>
#include <kioto/io/io_descriptions.h>
#include <kioto/io/io_sender.h>
#include <kioto/base/zstring_view.h>

namespace kioto {
    namespace detail {
#if KIOTO_OS_LINUX
        using file_native_handle_type = int;

        inline constexpr file_native_handle_type invalid_file_handle = -1;
#elif KIOTO_OS_WINDOWS
        using file_native_handle_type = void*;

        inline const file_native_handle_type invalid_file_handle = reinterpret_cast<void*>(std::uintptr_t(-1)); // NOLINT(*-misplaced-const)
#endif

        // Bridges the file layer's raw handle (void* HANDLE on Windows, int fd elsewhere) to the opaque
        // codec's native_fd (base/basic.h): POSIX coincides; Windows rides the HANDLE bits unchanged.
        // Separate #if from the types above — to_native_file must exist on every platform.
#if KIOTO_OS_WINDOWS
        [[nodiscard]] inline auto to_handle(file_native_handle_type handle) noexcept -> native_handle {
            return to_handle(std::bit_cast<native_fd>(handle));
        }
        [[nodiscard]] inline auto to_native_file(native_handle handle) noexcept -> file_native_handle_type {
            return std::bit_cast<file_native_handle_type>(to_native(handle));
        }
#else
        [[nodiscard]] constexpr auto to_native_file(native_handle handle) noexcept -> file_native_handle_type {
            return to_native(handle);
        }
#endif
        // OR-combinable open flags.
        enum class open_mode {
            read_only = 1,
            write_only = 2,
            read_write = 4,
            append = 8,
            create = 16,
            exclusive = 32,          // fail if the file already exists
            truncate = 64,
            sync_all_on_write = 128
        };

        KIOTO_ALWAYS_INLINE constexpr auto operator| (open_mode lhs, open_mode rhs) noexcept -> open_mode {
            return static_cast<open_mode>(int(lhs) | int(rhs));
        }

        KIOTO_ALWAYS_INLINE constexpr auto operator& (open_mode lhs, open_mode rhs) noexcept -> open_mode {
            return static_cast<open_mode>(int(lhs) & int(rhs));
        }

        enum class seek_whence { seek_set = 0, seek_cur = 1, seek_end = 2 };

        // Blocking file syscalls (throw std::system_error on failure).
        auto open_file(zstring_view path, open_mode mode, bool random_access) -> file_native_handle_type;
        auto file_read(file_native_handle_type handle, std::span<std::byte> buffer) -> std::size_t;
        auto file_write(file_native_handle_type handle, std::span<const std::byte> buffer) -> std::size_t;
        auto file_read_at(file_native_handle_type handle, std::size_t offset, std::span<std::byte> buffer) -> std::size_t;
        auto file_write_at(file_native_handle_type handle, std::size_t offset, std::span<const std::byte> buffer) -> std::size_t;
        auto close_file(file_native_handle_type handle) -> void;
        auto file_seek(file_native_handle_type handle, std::size_t offset, seek_whence whence) -> std::size_t;
        auto file_size(file_native_handle_type handle) -> std::size_t;
        auto file_resize(file_native_handle_type handle, std::size_t new_size) -> void;
        auto file_sync_all(file_native_handle_type handle) -> void;
        auto file_sync_data(file_native_handle_type handle) -> void;

        template<io_scheduler IoScheduler>
        class file_base {
        private:
            using implementation_type = decltype(std::declval<IoScheduler&>().make_io_handle(std::declval<typename IoScheduler::native_handle_type>()));
            static_assert(detail::io_driver_handle<implementation_type>,
                          "IoScheduler::make_io_handle must yield a conforming io handle (see detail::io_driver_handle)");

        public:
            // Opaque driver handle; raw fd only via the detail::to_native() hatch (for blocking file syscalls).
            using native_handle_type = typename IoScheduler::native_handle_type;
            using scheduler_type = IoScheduler;

        public:
            explicit file_base(scheduler_type scheduler) noexcept : file_base(std::move(scheduler), native_handle_type{}) {}

            file_base(scheduler_type scheduler, native_handle_type handle) : impl_(scheduler.make_io_handle(handle)) {}

            file_base(const file_base&) = delete;

            file_base(file_base&& other) = default;

            ~file_base() noexcept {
                close();
            }

            auto operator= (file_base other) noexcept -> file_base& {
                std::ranges::swap(impl_, other.impl_);
                return *this;
            }

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto get_io_scheduler() const noexcept -> scheduler_type {
                return impl_.get_io_scheduler();
            }

            // Cancels in-flight ops immediately. Throws std::system_error on failure.
            KIOTO_ALWAYS_INLINE auto close() -> void {
                close_file(detail::to_native_file(release()));
            }

            // Detaches the native handle to the caller; outstanding ops finish ASAP.
            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto release() -> native_handle_type {
                return impl_.release();
            }

            // In-flight reads/writes complete with operation_aborted.
            KIOTO_ALWAYS_INLINE auto cancel() -> void {
                impl_.cancel();
            }

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto native_handle() const noexcept -> native_handle_type {
                return impl_.native_handle();
            }

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto is_open() const noexcept -> bool {
                return native_handle() != native_handle_type{};
            }

            KIOTO_ALWAYS_INLINE explicit operator bool() const noexcept {
                return is_open();
            }

        protected:
            implementation_type impl_;
        };

        template<io_scheduler IoScheduler>
        class stream_file_base : public detail::file_base<IoScheduler> {
        private:
            using base = detail::file_base<IoScheduler>;

        public:
            using base::base;

            KIOTO_ALWAYS_INLINE auto read_some(std::span<std::byte> buffer) -> std::size_t {
                if constexpr (requires { this->impl_.file_read(buffer); }) {
                    return this->impl_.file_read(buffer);
                }
                else {
                    return detail::file_read(detail::to_native_file(this->native_handle()), buffer);
                }
            }

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto async_read_some(std::span<std::byte> buffer) {
                // EOF-on-zero folded into the driver completion (async_read_some_t::eof_on_zero) — no
                // wrapping let_value, one fewer sender/op-state per read.
                return this->get_io_scheduler().schedule_io(this->impl_, detail::async_read_some_t{buffer});
            }

            KIOTO_ALWAYS_INLINE auto write_some(std::span<const std::byte> buffer) -> std::size_t {
                if constexpr (requires { this->impl_.file_write(buffer); }) {
                    return this->impl_.file_write(buffer);
                }
                else {
                    return detail::file_write(detail::to_native_file(this->native_handle()), buffer);
                }
            }

            [[nodiscard]]
            KIOTO_ALWAYS_INLINE auto async_write_some(std::span<const std::byte> buffer) {
                return this->get_io_scheduler().schedule_io(this->impl_, detail::async_write_some_t{buffer});
            }
        };

        template<io_scheduler IoScheduler>
        class random_access_file_base : public detail::file_base<IoScheduler> {
        private:
            using base = detail::file_base<IoScheduler>;

        public:
            using base::base;

            // Positional read; does not move the file position.
            KIOTO_ALWAYS_INLINE auto read_some_at(std::size_t offset, std::span<std::byte> buffer) -> std::size_t {
                return detail::file_read_at(detail::to_native_file(this->native_handle()), offset, buffer);
            }

            KIOTO_ALWAYS_INLINE auto async_read_some_at(
                std::size_t offset,
                std::span<std::byte> buffer
            ) {
                // EOF-on-zero folded into the driver completion (async_read_some_at_t::eof_on_zero).
                return this->get_io_scheduler().schedule_io(this->impl_, detail::async_read_some_at_t{offset, buffer});
            }

            // Positional write; does not move the file position.
            KIOTO_ALWAYS_INLINE auto write_some_at(std::size_t offset, std::span<const std::byte> buffer) -> std::size_t {
                return detail::file_write_at(detail::to_native_file(this->native_handle()), offset, buffer);
            }

            KIOTO_ALWAYS_INLINE auto async_write_some_at(std::size_t offset, std::span<const std::byte> buffer) {
                return this->get_io_scheduler().schedule_io(
                    this->impl_,
                    detail::async_write_some_at_t{offset, buffer}
                );
            }
        };
    }

    // Sequential file: maintains an internal position that advances with each read/write.
    template<io_scheduler IoScheduler>
    class stream_file : public detail::stream_file_base<IoScheduler> {
    private:
        using base = detail::stream_file_base<IoScheduler>;
        // Regular files aren't pollable, so only a file-capable driver (io_uring/IOCP) serves them; epoll
        // can't. Gated on the concrete FILE class, not stream_file_base (pipes derive from that too).
        static_assert(IoScheduler::executor_type::template has_capability<capability::file>,
            "regular file I/O needs a driver providing capability::file (io_uring/IOCP); "
            "epoll can't do regular-file readiness");

    public:
        using enum detail::open_mode;
        using enum detail::seek_whence;

    public:
        using base::base;

        stream_file(IoScheduler scheduler, zstring_view path, detail::open_mode mode) : base(std::move(scheduler)) {
            open(path, mode);
        }

        // Throws error::already_open if already open.
        KIOTO_ALWAYS_INLINE auto open(zstring_view path, detail::open_mode mode) -> void {
            if (this->is_open()) throw std::system_error{error::already_open, "open"};
            this->impl_ = this->get_io_scheduler().make_io_handle(detail::to_handle(detail::open_file(path, mode, false)));
        }

        KIOTO_ALWAYS_INLINE auto resize(std::size_t new_size) -> void {
            if constexpr (requires { this->impl_.file_resize(new_size); }) {
                return this->impl_.file_resize(new_size);
            }
            else {
                return detail::file_resize(detail::to_native_file(this->native_handle()), new_size);
            }
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE  auto size() const -> std::size_t {
            return detail::file_size(detail::to_native_file(this->native_handle()));
        }

        KIOTO_ALWAYS_INLINE auto seek(std::size_t offset, detail::seek_whence whence) -> std::size_t {
            if constexpr (requires { this->impl_.file_seek(offset, whence); }) {
                return this->impl_.file_seek(offset, whence);
            }
            else {
                return detail::file_seek(detail::to_native_file(this->native_handle()), offset, whence);
            }
        }

        KIOTO_ALWAYS_INLINE auto sync_all() -> void {   // blocks until data + metadata are durable
            detail::file_sync_all(detail::to_native_file(this->native_handle()));
        }

        KIOTO_ALWAYS_INLINE auto sync_data() -> void {  // blocks until data is durable (metadata may lag)
            detail::file_sync_data(detail::to_native_file(this->native_handle()));
        }
    };

    // Random-access file: no internal position; every read/write takes an explicit offset.
    template<io_scheduler IoScheduler>
    class random_access_file : public detail::random_access_file_base<IoScheduler> {
    private:
        using base = detail::random_access_file_base<IoScheduler>;
        static_assert(IoScheduler::executor_type::template has_capability<capability::file>,
            "regular file I/O needs a driver providing capability::file (io_uring/IOCP); "
            "epoll can't do regular-file readiness");

    public:
        using enum detail::open_mode;
        using enum detail::seek_whence;

    public:
        using base::base;

        random_access_file(IoScheduler scheduler, zstring_view path, detail::open_mode mode) : base(std::move(scheduler)) {
            open(path, mode);
        }

        // Throws error::already_open if already open.
        KIOTO_ALWAYS_INLINE auto open(zstring_view path, detail::open_mode mode) -> void {
            if (this->is_open()) throw std::system_error{error::already_open, "open"};
            this->impl_ = this->get_io_scheduler().make_io_handle(detail::to_handle(detail::open_file(path, mode, true)));
        }

        KIOTO_ALWAYS_INLINE auto resize(std::size_t new_size) -> void {
            if constexpr (requires { this->impl_.file_resize(new_size); }) {
                return this->impl_.file_resize(new_size);
            }
            else {
                return detail::file_resize(detail::to_native_file(this->native_handle()), new_size);
            }
        }

        [[nodiscard]]
        KIOTO_ALWAYS_INLINE  auto size() const -> std::size_t {
            return detail::file_size(detail::to_native_file(this->native_handle()));
        }

        KIOTO_ALWAYS_INLINE auto sync_all() -> void {   // blocks until data + metadata are durable
            detail::file_sync_all(detail::to_native_file(this->native_handle()));
        }

        KIOTO_ALWAYS_INLINE auto sync_data() -> void {  // blocks until data is durable (metadata may lag)
            detail::file_sync_data(detail::to_native_file(this->native_handle()));
        }
    };
}
