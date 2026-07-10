// kioto::zstring_view — a null-terminated string_view (the type file/socket paths take for paths/hosts).
// The invariant that matters: c_str() is always null-terminated for C APIs.
#include <string>
#include <string_view>
#include <doctest/doctest.h>
#include <kioto/base/zstring_view.h>

TEST_CASE("zstring_view from a literal is a null-terminated view") {
    kioto::zstring_view z = "hello";
    CHECK(z.size() == 5);
    CHECK(not z.empty());
    CHECK(z[0] == 'h');
    CHECK(std::string_view(z.view()) == "hello");
    CHECK(z.c_str()[z.size()] == '\0');   // the whole point: null-terminated
}

TEST_CASE("zstring_view from a std::string preserves the terminator and value") {
    std::string s = "kioto-path";
    kioto::zstring_view z = s;
    CHECK(z.view() == "kioto-path");
    CHECK(z.c_str()[z.size()] == '\0');
}

TEST_CASE("zstring_view compares by value") {
    kioto::zstring_view a = "abc";
    kioto::zstring_view b = "abc";
    kioto::zstring_view c = "abd";
    CHECK(a == b);
    CHECK(a != c);
    CHECK(a < c);
}

TEST_CASE("an empty zstring_view is empty and null-terminated") {
    kioto::zstring_view z = "";
    CHECK(z.empty());
    CHECK(z.c_str()[0] == '\0');
}
