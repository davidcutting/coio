#include <kioto/core.h>
#include <kioto/io/execution_context.h>
#include <kioto/base/timer.h>
#include "common.h"

auto job(kioto::time_loop::scheduler sched, std::string_view name, int value, std::chrono::seconds timeout) -> kioto::task<int> {
    kioto::timer timer{sched};
    co_await timer.async_wait(timeout);
    ::println("{} completed", name);
    co_return value;
}

auto main() -> int {
    using namespace std::chrono_literals;
    kioto::time_loop context;
    const auto tick = std::chrono::steady_clock::now();
    auto value = kioto::this_thread::sync_wait_with_variant(kioto::when_any(
        kioto::starts_on(context.get_scheduler(), job(context.get_scheduler(), "foo", 114, 2s)),
        kioto::starts_on(context.get_scheduler(), job(context.get_scheduler(), "bar", 514, 1s)),
        kioto::starts_on(context.get_scheduler(), job(context.get_scheduler(), "qux", 1919, 3s)),
        [&context]() -> kioto::task<> {
            context.run();
            co_return;
        }()
    )).value();
    auto [i] = std::get<std::tuple<int>>(value);
    const auto tock = std::chrono::steady_clock::now();
    ::println("result: i = {}", i);
    ::println("take: {}ms", std::chrono::duration_cast<std::chrono::milliseconds>(tock - tick).count()); // take: 1000ms
}
