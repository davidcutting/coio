// kioto::scope_exit — the RAII cleanup guard used across teardown paths.
#include <doctest/doctest.h>
#include <kioto/base/scope_exit.h>

TEST_CASE("scope_exit runs its action at end of scope") {
    int ran = 0;
    {
        kioto::scope_exit guard{[&ran] { ++ran; }};
        CHECK(ran == 0);   // not yet
    }
    CHECK(ran == 1);       // ran exactly once on scope exit
}

TEST_CASE("release() cancels the action") {
    int ran = 0;
    {
        kioto::scope_exit guard{[&ran] { ++ran; }};
        guard.release();
    }
    CHECK(ran == 0);
}

TEST_CASE("moving a scope_exit transfers the action exactly once") {
    int ran = 0;
    {
        kioto::scope_exit outer{[&ran] { ++ran; }};
        {
            kioto::scope_exit inner{std::move(outer)};   // outer is released, inner owns the action
            CHECK(ran == 0);
        }
        CHECK(ran == 1);   // inner ran it
    }
    CHECK(ran == 1);       // outer, released by the move, does NOT run it again
}
