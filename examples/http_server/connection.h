#pragma once
#include "define.h"
#include "router.h"

namespace http {
    auto connection(
        tcp_socket socket,
        kioto::endpoint remote_endpoint,
        router& router
    ) -> io_executor::task<>;
}
