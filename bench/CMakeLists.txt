project(coio-benchmarks)

CPMFindPackage(
    NAME benchmark
    GITHUB_REPOSITORY google/benchmark
    GIT_TAG v1.9.4
    EXCLUDE_FROM_ALL YES
    SYSTEM YES
    OPTIONS
    "BENCHMARK_ENABLE_TESTING OFF"
    "BENCHMARK_ENABLE_INSTALL OFF"
)

file(GLOB COIO_BENCH_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/bench_*.cpp")

foreach(COIO_BENCH_SOURCE IN LISTS COIO_BENCH_SOURCES)
    get_filename_component(COIO_BENCH_NAME "${COIO_BENCH_SOURCE}" NAME_WE)

    add_executable(${COIO_BENCH_NAME} ${COIO_BENCH_SOURCE})

    target_link_libraries(
        ${COIO_BENCH_NAME}
        PRIVATE
        coio
        benchmark::benchmark
        benchmark::benchmark_main
    )
endforeach()
