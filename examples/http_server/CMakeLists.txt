add_executable(
    example-http_server
    connection.h
    connection.cpp
    define.h
    response.h
    response.cpp
    request.h
    router.h
    router.cpp
    main.cpp
)

target_link_libraries(
    example-http_server
    PRIVATE
    coio
)

target_compile_definitions(
    example-http_server
    PRIVATE
    HTTP_SERVER_STATIC_DIR="${CMAKE_CURRENT_SOURCE_DIR}/static"
)