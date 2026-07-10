// Adapted from https://en.cppreference.com/w/cpp/thread/latch.html#Example
#include <kioto/core.h>
#include <kioto/exec/sync_primitives.h>
#include "common.h"

struct Job {
    std::string name;
    std::string product;
};

auto work(Job& job, kioto::async_latch<>& work_done, kioto::async_latch<>& start_clean_up) -> kioto::task<> {
    job.product = job.name + " worked";
    work_done.count_down();
    co_await start_clean_up.wait();
    job.product = job.name + " cleaned";
}

auto main() -> int {
    Job jobs[]{{"Annika"}, {"Buru"}, {"Chuck"}};
    kioto::async_latch<> work_done{std::ranges::size(jobs)};
    kioto::async_latch<> start_clean_up{1};

    kioto::async_scope scope;
    ::print("Work is starting... ");
    for (auto& job : jobs) {
        scope.spawn(work(job, work_done, start_clean_up));
    }

    kioto::this_thread::sync_wait(work_done.wait());
    ::println("done:");
    for (const auto& job : jobs) {
        ::println(" {}", job.product);
    }

    ::print("Workers are cleaning up... ");
    start_clean_up.count_down();

    kioto::this_thread::sync_wait(scope.join());
    ::println("done:");
    for (const auto& job : jobs) {
        ::println(" {}", job.product);
    }
}
