#include <kioto/io/io.h>
#include <kioto/base/flat_buffer.h>
#include "json_rpc.h"

auto generate_id() noexcept -> int {
    static int id = 0;
    return id++;
}

auto call(json_rpc::tcp_socket& socket, const json_rpc::value& request) -> json_rpc::io_context::task<json_rpc::value> {
    kioto::flat_buffer buffer;
    const std::string line = json_rpc::dump(request) + '\n';
    co_await (kioto::async_write(socket, kioto::as_bytes(line)) | as_throwing);

    const auto n = co_await (kioto::async_read_until(socket, buffer, '\n') | as_throwing);
    const auto data = buffer.data();
    auto value = json_rpc::parse(std::string_view{reinterpret_cast<const char*>(data.data()), n});
    buffer.consume(n);
    co_return value;
}

auto run_client() -> json_rpc::io_context::task<> try {
    json_rpc::io_context::scheduler sched = co_await kioto::read_scheduler();
    static auto json_rpc_version = "2.0";
    json_rpc::tcp_socket socket{sched};
    co_await socket.async_connect({kioto::ipv4_address::loopback(), 9090});
    ::println("connected to {}", socket.remote_endpoint());

    ::println("{}", json_rpc::dump(co_await call(socket, json_rpc::object{
        {"jsonrpc", json_rpc_version},
        {"method", "add"},
        {"params", json_rpc::array{100, 50}},
        {"id", generate_id()},
    })));

    ::println("{}", json_rpc::dump(co_await call(socket, json_rpc::object{
        {"jsonrpc", json_rpc_version},
        {"method", "subtract"},
        {"params", json_rpc::array{100, 50}},
        {"id", generate_id()},
    })));

    ::println("{}", json_rpc::dump(co_await call(socket, json_rpc::array{
        json_rpc::object{
            {"jsonrpc", json_rpc_version},
            {"method", "add"},
            {"params", json_rpc::array{114, 514}},
            {"id", generate_id()},
        },
        json_rpc::object{
            {"jsonrpc", json_rpc_version},
            {"method", "subtract"},
            {"params", json_rpc::array{1919, 810}},
            {"id", generate_id()},
        }
    })));
}
catch (const std::exception& e) {
    ::println("client error: {}", e.what());
}

auto main() -> int {
    json_rpc::io_context context;
    kioto::async_scope scope;
    scope.spawn_on(context.get_scheduler(), run_client());
    context.run();
    kioto::this_thread::sync_wait(scope.join());
}
