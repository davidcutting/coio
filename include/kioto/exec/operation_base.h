#pragma once

namespace kioto::detail {
    struct operation_base {
        operation_base* next_ = nullptr;
        virtual auto finish() -> void = 0;

    protected:
        ~operation_base() = default;
    };
}
