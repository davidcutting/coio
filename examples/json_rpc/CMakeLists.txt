add_executable(
    example-json_rpc_client
    json_rpc.h
    client.cpp
)

target_link_libraries(
    example-json_rpc_client
    PRIVATE
    kioto
)

add_executable(
    example-json_rpc_server
    json_rpc.h
    server.cpp
)

target_link_libraries(
    example-json_rpc_server
    PRIVATE
    kioto
)