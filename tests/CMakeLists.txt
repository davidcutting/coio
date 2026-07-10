project(kioto-tests)

file(GLOB KIOTO_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/test_*.cpp")

CPMFindPackage(
    NAME doctest
    GITHUB_REPOSITORY doctest/doctest
    GIT_TAG v2.5.2
    EXCLUDE_FROM_ALL YES
    SYSTEM YES
)

foreach(KIOTO_TEST_SOURCE IN LISTS KIOTO_TEST_SOURCES)
    get_filename_component(KIOTO_TEST_NAME "${KIOTO_TEST_SOURCE}" NAME_WE)

    add_executable(
        ${KIOTO_TEST_NAME}
    )

    target_sources(
        ${KIOTO_TEST_NAME}
        PRIVATE
        ${KIOTO_TEST_SOURCE}
    )

    target_link_libraries(
        ${KIOTO_TEST_NAME}
        PRIVATE
        doctest::doctest
        kioto
    )
    target_compile_definitions(
        ${KIOTO_TEST_NAME}
        PRIVATE
        -DDOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
    )

    add_test(
        NAME ${KIOTO_TEST_NAME}
        COMMAND $<TARGET_FILE:${KIOTO_TEST_NAME}>
    )
endforeach()
