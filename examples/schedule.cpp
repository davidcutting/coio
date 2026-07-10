#include <thread>
#include <kioto/core.h>
#include <kioto/io/execution_context.h>
#include "common.h"

namespace {
    class worker {
    public:
        worker(std::string_view name) {
            thrd = std::jthread{[name, this] {
                ::debug("worker-{} run...", name);
                loop.run();
            }};
        }

        worker(const worker&) = delete;

        auto operator=(const worker&) -> worker& = delete;

        auto scheduler() {
            return loop.get_scheduler();
        }

    private:
        kioto::time_loop loop;
        std::jthread thrd;
        kioto::work_guard<kioto::time_loop> _{loop};
    };
}

auto main() -> int {
    using namespace std::string_view_literals;
    worker workers[]{"alice"sv, "bob"sv};
    auto _ = kioto::this_thread::sync_wait(kioto::just() | kioto::then([] {
        ::debug("in main thread");
    }) | kioto::continues_on(workers[0].scheduler()) | kioto::then([] {
        ::debug("in worker-alice thread");
    }) | kioto::continues_on(workers[1].scheduler()) | kioto::then([] {
        ::debug("in worker-bob thread");
    })).value();
}
