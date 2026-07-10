#include <array>
#include <vector>
#include <doctest/doctest.h>
#include <kioto/exec/generator.h>

namespace {
auto iota(int n) -> kioto::generator<int> {
    for (int i = 0; i < n; ++i) {
        co_yield i;
    }
}

auto flattened_values() -> kioto::generator<const int&> {
    std::array<int, 1> first{1};
    for (const int& value : first) {
        co_yield value;
    }

    std::array<int, 2> middle{2, 3};
    co_yield kioto::elements_of{middle};

    co_yield kioto::elements_of{[]() -> kioto::generator<const int&> {
        std::array<int, 2> tail{4, 5};
        for (const int& value : tail) {
            co_yield value;
        }
    }()};
}

auto mutable_refs(std::vector<int>& values) -> kioto::generator<int&> {
    for (auto& value : values) {
        co_yield value;
    }
}
}

TEST_CASE("generator yields a simple sequence") {
    std::vector<int> actual;
    for (int value : iota(5)) {
        actual.push_back(value);
    }

    CHECK_EQ(actual, std::vector<int>{0, 1, 2, 3, 4});
}

TEST_CASE("generator flattens nested ranges") {
    std::vector<int> actual;
    for (int value : flattened_values()) {
        actual.push_back(value);
    }

    CHECK_EQ(actual, std::vector<int>{1, 2, 3, 4, 5});
}

TEST_CASE("generator can expose mutable references") {
    std::vector<int> values{1, 2, 3};

    for (int& value : mutable_refs(values)) {
        value *= 10;
    }

    CHECK_EQ(values, std::vector<int>{10, 20, 30});
}
