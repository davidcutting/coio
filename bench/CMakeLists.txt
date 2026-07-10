project(kioto-benchmarks)

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

file(GLOB KIOTO_BENCH_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/bench_*.cpp")

foreach(KIOTO_BENCH_SOURCE IN LISTS KIOTO_BENCH_SOURCES)
    get_filename_component(KIOTO_BENCH_NAME "${KIOTO_BENCH_SOURCE}" NAME_WE)

    add_executable(${KIOTO_BENCH_NAME} ${KIOTO_BENCH_SOURCE})

    target_link_libraries(
        ${KIOTO_BENCH_NAME}
        PRIVATE
        kioto
        benchmark::benchmark
        benchmark::benchmark_main
    )
endforeach()
