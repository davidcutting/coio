cmake_minimum_required(VERSION 3.26)

project(kioto)

option(KIOTO_BUILD_EXAMPLES "whether to build examples" OFF)
option(KIOTO_BUILD_TESTS "whether to build tests" OFF)
option(KIOTO_BUILD_BENCHMARKS "whether to build benchmarks" OFF)
option(KIOTO_BUILD_WITH_ASAN "whether to enable AddressSanitizer" OFF)
option(KIOTO_BUILD_WITH_TSAN "whether to enable ThreadSanitizer" OFF)
option(KIOTO_BUILD_WITH_UBSAN "whether to enable UndefinedBehaviorSanitizer" OFF)
set(KIOTO_SENDERS_BACKEND "NVIDIA" CACHE STRING "the backend to use for std::execution support.
    available options are: NVIDIA, BEMAN, CXX26.
    NVIDIA - use NVIDIA/stdexec implementation,
    BEMAN - use bemanproject/execution implementation,
    CXX26 - use the standard library implementation"
)

include("${PROJECT_SOURCE_DIR}/cmake/CPM.cmake")

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

file(GLOB_RECURSE KIOTO_PUBLIC_HEADERS CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/include/*.h")
add_library(
    ${PROJECT_NAME}
    ${KIOTO_PUBLIC_HEADERS}
)

if (MSVC)
    set(KIOTO_COMPILER_WARNING_FLAG "/W4")
else()
    set(
        KIOTO_COMPILER_WARNING_FLAG
        "-Wall"
        "-Wextra"
        "-Wpedantic"
    )
endif()

target_compile_options(
    ${PROJECT_NAME}
    PUBLIC
    "$<$<CXX_COMPILER_ID:MSVC>:/utf-8>"
    "$<$<CXX_COMPILER_ID:MSVC>:/permissive->"
    "$<$<CXX_COMPILER_ID:MSVC>:/Zc:preprocessor>"
    PRIVATE
    "${KIOTO_COMPILER_WARNING_FLAG}"
)

add_library(
    ${PROJECT_NAME}::${PROJECT_NAME}
    ALIAS
    ${PROJECT_NAME}
)

target_compile_definitions(
    ${PROJECT_NAME}
    PUBLIC
    -DKIOTO_EXECUTION_USE_${KIOTO_SENDERS_BACKEND}
)

target_sources(
    ${PROJECT_NAME}
    INTERFACE
    FILE_SET kioto_public_headers
    TYPE HEADERS
    BASE_DIRS ${PROJECT_SOURCE_DIR}/include
    FILES
    ${KIOTO_PUBLIC_HEADERS}
)

# Module-first sources: each module's platform impls are co-located and selected by filename
# suffix (foo.linux.cpp / foo.win.cpp); a plain foo.cpp is portable and always compiled.
file(GLOB_RECURSE KIOTO_ALL_SOURCES CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/src/kioto/*.cpp")
set(KIOTO_PORTABLE_SOURCES ${KIOTO_ALL_SOURCES})
list(FILTER KIOTO_PORTABLE_SOURCES EXCLUDE REGEX "\\.(linux|win)\\.cpp$")
target_sources(
    ${PROJECT_NAME}
    PRIVATE
    ${KIOTO_PORTABLE_SOURCES}
)

if (LINUX)
    file(GLOB_RECURSE KIOTO_LINUX_SOURCES CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/src/kioto/*.linux.cpp")
    target_sources(
        ${PROJECT_NAME}
        PRIVATE
        ${KIOTO_LINUX_SOURCES}
    )
elseif (WIN32)
    file(GLOB_RECURSE KIOTO_WIN_SOURCES CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/src/kioto/*.win.cpp")
    target_sources(
        ${PROJECT_NAME}
        PRIVATE
        ${KIOTO_WIN_SOURCES}
    )

    target_link_libraries(
        ${PROJECT_NAME}
        PUBLIC
        ws2_32
        mswsock
    )

    target_compile_definitions(
        ${PROJECT_NAME}
        PRIVATE
        -DWIN32_LEAN_AND_MEAN
        -DNOMINMAX
    )
endif()

target_include_directories(
    ${PROJECT_NAME}
    PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${PROJECT_NAME}/include>
    PRIVATE
    # private base for internal impl headers (kioto/common.linux.h, kioto/common.win.h)
    ${PROJECT_SOURCE_DIR}/src
)

find_package(Threads REQUIRED)

target_link_libraries(
    ${PROJECT_NAME}
    PUBLIC
    Threads::Threads
)

set(KIOTO_SANITIZERS)
if (KIOTO_BUILD_WITH_ASAN)
    list(APPEND KIOTO_SANITIZERS address)
endif()
if (KIOTO_BUILD_WITH_TSAN)
    list(APPEND KIOTO_SANITIZERS thread)
endif()
if (KIOTO_BUILD_WITH_UBSAN)
    list(APPEND KIOTO_SANITIZERS undefined)
endif()

if (KIOTO_SANITIZERS)
    list(JOIN KIOTO_SANITIZERS "," KIOTO_SANITIZER_FLAG)

    if (MSVC)
        target_compile_options(
            ${PROJECT_NAME}
            PUBLIC
            "/fsanitize=${KIOTO_SANITIZER_FLAG}"
        )
    else()
        target_compile_options(
            ${PROJECT_NAME}
            PUBLIC
            "-fsanitize=${KIOTO_SANITIZER_FLAG}"
            "-fno-omit-frame-pointer"
        )

        target_link_options(
            ${PROJECT_NAME}
            PUBLIC
            "-fsanitize=${KIOTO_SANITIZER_FLAG}"
        )
    endif()
endif()

# mimalloc as the default allocator (measured 1.5-3.3x on spawn-heavy throughput vs glibc; neutral on
# the allocation-free datagram path). Linked PUBLIC so it propagates into every executable that links
# kioto. --no-as-needed is required: kioto references no mimalloc symbol directly (it calls malloc, which
# resolves at runtime), so the linker would otherwise drop the DT_NEEDED and the override wouldn't
# happen. Skipped under sanitizers, which install their own malloc interceptors.
option(KIOTO_USE_MIMALLOC "Link mimalloc as the default allocator" ON)
if (KIOTO_USE_MIMALLOC AND NOT KIOTO_SANITIZERS)
    find_library(MIMALLOC_LIBRARY NAMES mimalloc)
    if (MIMALLOC_LIBRARY)
        message(STATUS "Using mimalloc as the default allocator: ${MIMALLOC_LIBRARY}")
        target_link_libraries(
            ${PROJECT_NAME}
            PUBLIC
            "-Wl,--no-as-needed" ${MIMALLOC_LIBRARY} "-Wl,--as-needed"
        )
    else()
        message(WARNING "KIOTO_USE_MIMALLOC=ON but libmimalloc was not found; using the system allocator")
    endif()
endif()

if (KIOTO_SENDERS_BACKEND STREQUAL "NVIDIA")
    target_compile_features(
        ${PROJECT_NAME}
        PUBLIC
        cxx_std_20
    )
    CPMFindPackage(
        NAME stdexec
        GITHUB_REPOSITORY NVIDIA/stdexec
        GIT_TAG main
        EXCLUDE_FROM_ALL YES
        SYSTEM YES
        OPTIONS
        "STDEXEC_BUILD_EXAMPLES OFF"
        "STDEXEC_BUILD_TESTS OFF"
    )
    target_link_libraries(${PROJECT_NAME} PUBLIC $<BUILD_LOCAL_INTERFACE:STDEXEC::stdexec>)
elseif (KIOTO_SENDERS_BACKEND STREQUAL "BEMAN")
    target_compile_features(
        ${PROJECT_NAME}
        PUBLIC
        cxx_std_23
    )
    CPMFindPackage(
        NAME beman.execution
        GITHUB_REPOSITORY bemanproject/execution
        GIT_TAG main
        EXCLUDE_FROM_ALL YES
        SYSTEM YES
        OPTIONS
        "BEMAN_EXECUTION_ENABLE_TESTING OFF"
        "BEMAN_EXECUTION_BUILD_EXAMPLES OFF"
        "BEMAN_USE_MODULES OFF"
    )
    target_link_libraries(${PROJECT_NAME} PUBLIC $<BUILD_LOCAL_INTERFACE:beman::execution_headers>)
endif()

if (LINUX)
    find_package(Liburing QUIET)
    if (Liburing_FOUND)
        message(STATUS "Found liburing: ${Liburing_LIBRARY}")
        target_link_libraries(${PROJECT_NAME} PUBLIC Liburing::uring)
        install(
            FILES ${PROJECT_SOURCE_DIR}/cmake/FindLiburing.cmake
            DESTINATION ${PROJECT_NAME}/lib/cmake/${PROJECT_NAME}
        )
    else()
        message(STATUS "liburing not found, io_uring support will be unavailable")
    endif()
endif()

install(
    TARGETS ${PROJECT_NAME}
    EXPORT ${PROJECT_NAME}-targets
    DESTINATION ${PROJECT_NAME}/lib/${CMAKE_BUILD_TYPE}
    FILE_SET kioto_public_headers DESTINATION ${PROJECT_NAME}/include
)

install(
    EXPORT ${PROJECT_NAME}-targets
    FILE ${PROJECT_NAME}-targets.cmake
    NAMESPACE ${PROJECT_NAME}::
    DESTINATION ${PROJECT_NAME}/lib/cmake/${PROJECT_NAME}
)

export(
    EXPORT ${PROJECT_NAME}-targets
    NAMESPACE ${PROJECT_NAME}::
    FILE ${PROJECT_BINARY_DIR}/${PROJECT_NAME}-targets.cmake
)

include(CMakePackageConfigHelpers)

configure_package_config_file(
    ${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}-config.cmake.in
    ${PROJECT_NAME}-config.cmake
    INSTALL_DESTINATION ${PROJECT_NAME}/lib/cmake/${PROJECT_NAME}
)

install(
    FILES ${PROJECT_BINARY_DIR}/${PROJECT_NAME}-config.cmake
    DESTINATION ${PROJECT_NAME}/lib/cmake/${PROJECT_NAME}
)

if (KIOTO_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if (KIOTO_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if (KIOTO_BUILD_BENCHMARKS)
    add_subdirectory(bench)
endif()
