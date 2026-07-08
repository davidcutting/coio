// ReSharper disable CppRedundantTypenameKeyword
#pragma once
#include <atomic>
#include <concepts>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <coio/execution_context.h>
#include <coio/detail/operation_base.h>
#include <coio/detail/frame_pool.h>
#include <coio/utils/async_scope.h>

namespace coio {
    template<typename W>
    concept runtime_worker = execution_context<W> and requires(W& w, detail::operation_base& op) {
        w.run();
        w.request_stop();
        w.wake_up();
        w.submit(op); // the runtime round-robins balanced work onto each worker's own inbox
    };

    template<typename L>
    concept thread_launcher = requires(L& l, std::size_t i) {
        { l(i, [] {}) } -> std::same_as<std::jthread>;
    };

    struct default_thread_launcher {
        template<std::invocable Entry>
        auto operator() (std::size_t /*index*/, Entry entry) const -> std::jthread {
            return std::jthread{std::move(entry)};
        }
    };

    template<runtime_worker Worker>
    class basic_runtime {
    public:
        using worker_type = Worker;
        using worker_scheduler = std::decay_t<decltype(std::declval<Worker&>().get_scheduler())>;

        class scheduler {
            friend basic_runtime;
            explicit scheduler(basic_runtime* runtime) noexcept : runtime_(runtime) {}

        public:
            using scheduler_concept = execution::scheduler_tag;

            struct env {
                basic_runtime* runtime;

                auto query(execution::get_completion_scheduler_t<execution::set_value_t>) const noexcept -> scheduler {
                    return scheduler{runtime};
                }
            };

            class schedule_sender {
                friend scheduler;
                explicit schedule_sender(basic_runtime* runtime) noexcept : runtime_(runtime) {}

            public:
                using sender_concept = execution::sender_tag;
                using completion_signatures = execution::completion_signatures<execution::set_value_t()>;

                template<execution::receiver Rcvr>
                struct operation : detail::operation_base {
                    using operation_state_concept = execution::operation_state_tag;

                    operation(basic_runtime* runtime, Rcvr rcvr) noexcept
                        : runtime_(runtime), rcvr_(std::move(rcvr)) {}

                    auto start() & noexcept -> void {
                        runtime_->post(*this);
                    }

                    auto finish() -> void override {
                        execution::set_value(std::move(rcvr_));
                    }

                    basic_runtime* runtime_;
                    Rcvr rcvr_;
                };

                template<execution::receiver Rcvr>
                COIO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept -> operation<Rcvr> {
                    return operation<Rcvr>{runtime_, std::move(rcvr)};
                }

                template<similar_to<schedule_sender>, typename...>
                static consteval auto get_completion_signatures() noexcept -> completion_signatures {
                    return {};
                }

                COIO_ALWAYS_INLINE auto get_env() const noexcept -> env {
                    return env{runtime_};
                }

            private:
                basic_runtime* runtime_;
            };

            [[nodiscard]]
            COIO_ALWAYS_INLINE auto schedule() const noexcept -> schedule_sender {
                return schedule_sender{runtime_};
            }

            [[nodiscard]]
            COIO_ALWAYS_INLINE static constexpr auto query(execution::get_forward_progress_guarantee_t) noexcept {
                return execution::forward_progress_guarantee::parallel;
            }

            [[nodiscard]]
            COIO_ALWAYS_INLINE auto query(get_allocator_t) const noexcept -> std::pmr::polymorphic_allocator<> {
                return {};
            }

            friend auto operator== (const scheduler&, const scheduler&) noexcept -> bool = default;

        private:
            basic_runtime* runtime_;
        };

    public:
        template<std::invocable<std::size_t> Factory, thread_launcher Launcher = default_thread_launcher>
            requires std::convertible_to<std::invoke_result_t<Factory&, std::size_t>, std::unique_ptr<Worker>>
        basic_runtime(std::size_t count, Factory make_worker, Launcher launch = {}) {
            if (count == 0) count = 1;
            workers_.reserve(count);
            guards_.reserve(count);
            threads_.reserve(count);

            for (std::size_t i = 0; i < count; ++i) {
                workers_.push_back(make_worker(i));
            }
            for (auto& w : workers_) guards_.emplace_back(*w);
            for (std::size_t i = 0; i < count; ++i) {
                threads_.push_back(launch(i, [ctx = workers_[i].get()] { run_worker(*ctx); }));
            }
        }

        basic_runtime(const basic_runtime&) = delete;

        auto operator= (const basic_runtime&) -> basic_runtime& = delete;

        ~basic_runtime() {
            // counting_scope must be joined exactly once before destruction. If join() was not already
            // driven to completion, cancel + drain the scope WHILE the workers still run (suspended I/O
            // needs them) so it is empty before it is destroyed, then stop.
            if (not joined_.load(std::memory_order_acquire)) {
                scope_.close();
                scope_.request_stop();
                this_thread::sync_wait(join());
            }
            stop();
        }

        [[nodiscard]]
        static auto default_worker_count() noexcept -> std::size_t {
            const auto n = std::thread::hardware_concurrency();
            return n == 0 ? 1 : n;
        }

        [[nodiscard]]
        auto size() const noexcept -> std::size_t {
            return workers_.size();
        }

        [[nodiscard]]
        auto get_scheduler() noexcept -> scheduler {
            return scheduler{this};
        }

        [[nodiscard]]
        static auto current_scheduler() noexcept -> std::optional<worker_scheduler> {
            if (auto* w = current_worker_) return w->get_scheduler();
            return std::nullopt;
        }

        auto spawn(execution::sender auto sndr) -> void {
            scope_.spawn_on(get_scheduler(), std::move(sndr));
        }

        template<execution::scheduler Sched>
        auto spawn_on(Sched sched, execution::sender auto sndr) -> void {
            scope_.spawn_on(std::move(sched), std::move(sndr));
        }

        [[nodiscard]]
        auto join() noexcept {
            return execution::then(scope_.join(), [this]() noexcept {
                joined_.store(true, std::memory_order_release);
            });
        }

        auto stop() -> void {
            if (stopped_.exchange(true)) return;
            for (auto& w : workers_) w->request_stop();
            guards_.clear();
            for (auto& t : threads_) {
                if (t.joinable()) t.join();
            }
        }

    private:
        // Balanced tier: round-robin the op onto a worker's own inbox (its lock-free inject stack).
        // No shared injector, no pull — the worker drains it in do_one like any cross-thread post.
        auto post(detail::operation_base& op) -> void {
            const auto i = wake_cursor_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
            workers_[i]->submit(op);
        }

        static auto run_worker(Worker& worker) -> void {
            current_worker_ = &worker;
            // Frames of coroutines created on this worker recycle through this per-thread pool. It is a
            // stack local, destroyed only after run() drains (so no live frame outlives it).
            detail::frame_pool pool;
            detail::tl_frame_pool = &pool;
            worker.run();
            detail::tl_frame_pool = nullptr;
            current_worker_ = nullptr;
        }

    private:
        static inline thread_local Worker* current_worker_ = nullptr;

        std::vector<std::unique_ptr<Worker>> workers_;
        std::vector<work_guard<Worker>> guards_;
        std::vector<std::jthread> threads_;
        std::atomic<std::size_t> wake_cursor_{0};
        async_scope scope_;
        std::atomic<bool> stopped_{false};
        std::atomic<bool> joined_{false};
    };
}
