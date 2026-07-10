// kioto::basic_fixed_string — the compile-time string vocabulary type. Constexpr-usable, so it's checked
// both at compile time (static_assert) and at runtime (for line coverage of its accessors).
#include <string_view>
#include <doctest/doctest.h>
#include <kioto/base/fixed_string.h>

namespace {
    static_assert(kioto::basic_fixed_string{"hello"}.size() == 5);
    static_assert(not kioto::basic_fixed_string{"hello"}.empty());
    static_assert(kioto::basic_fixed_string{""}.empty());
    static_assert(kioto::basic_fixed_string{"abc"}[0] == 'a');
    static_assert(kioto::basic_fixed_string{"abc"}[2] == 'c');
}

TEST_CASE("basic_fixed_string exposes its contents at runtime") {
    auto s = kioto::basic_fixed_string{"kioto"};
    CHECK(s.size() == 5);
    CHECK(s.length() == 5);
    CHECK(not s.empty());
    CHECK(s[0] == 'k');
    CHECK(s[4] == 'o');
    CHECK(std::string_view(s.c_str(), s.size()) == "kioto");
    CHECK(std::string_view(s.data(), s.size()) == "kioto");
}

TEST_CASE("an empty fixed string is empty") {
    auto s = kioto::basic_fixed_string{""};
    CHECK(s.empty());
    CHECK(s.size() == 0);
}
