// Compile-time contract for kioto::type_list — the metaprogramming spine the driver/capability/completion
// machinery is built on. All checks are static_assert (zero runtime), so a regression is a build failure.
#include <type_traits>
#include <doctest/doctest.h>
#include <kioto/base/type_traits.h>

namespace {
    using kioto::type_list;
    struct A; struct B; struct C; struct D;

    static_assert(type_list<>::size == 0);
    static_assert(type_list<A, B, C>::size == 3);

    static_assert(type_list<A, B, C>::contains<B>);
    static_assert(not type_list<A, B, C>::contains<D>);
    static_assert(not type_list<>::contains<A>);

    static_assert(type_list<A, B, C>::find<C> == 2);
    static_assert(type_list<A, B, C>::find<D> == type_list<>::npos);

    static_assert(std::is_same_v<type_list<A, B, C>::at<0>, A>);
    static_assert(std::is_same_v<type_list<A, B, C>::at<2>, C>);

    static_assert(std::is_same_v<type_list<A, B>::concat<type_list<C, D>>, type_list<A, B, C, D>>);
    static_assert(std::is_same_v<type_list<>::concat<type_list<A>>, type_list<A>>);

    static_assert(std::is_same_v<type_list<B, C>::push_front<A>, type_list<A, B, C>>);
    static_assert(std::is_same_v<type_list<A, B>::push_back<C>, type_list<A, B, C>>);
    static_assert(std::is_same_v<type_list<A>::prepend<B, C>, type_list<B, C, A>>);
    static_assert(std::is_same_v<type_list<A>::append<B, C>, type_list<A, B, C>>);

    static_assert(std::is_same_v<type_list<A, B, C>::pop_front, type_list<B, C>>);
    static_assert(std::is_same_v<type_list<A, B, C>::pop_back, type_list<A, B>>);

    static_assert(std::is_same_v<type_list<A, B, C>::reverse, type_list<C, B, A>>);
    static_assert(std::is_same_v<type_list<>::reverse, type_list<>>);

    static_assert(std::is_same_v<type_list<A, B, C, B>::remove<B>, type_list<A, C>>);
    static_assert(std::is_same_v<type_list<A, B, A, C, B>::unique, type_list<A, B, C>>);
    static_assert(std::is_same_v<type_list<A, B, A>::replace<A, D>, type_list<D, B, D>>);
    // transform<F> / filter<Pred> take template-template args; they're exercised internally by
    // replace/unique above and the driver capability machinery, so not re-checked here.
}

TEST_CASE("type_list metaprogramming holds at compile time") {
    CHECK(true);   // the static_asserts above are the test; this keeps doctest happy
}
