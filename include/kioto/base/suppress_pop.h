// ReSharper disable once CppMissingIncludeGuard

#if defined(_MSC_VER)
    #pragma warning(pop)
#elif defined(__clang__)
    #pragma clang diagnostic pop
#elif defined(__GNUC__)
    #pragma GCC diagnostic pop
#endif

#ifdef KIOTO_DETAIL_SUPPRESS_PUSH_INCLUDED
#undef KIOTO_DETAIL_SUPPRESS_PUSH_INCLUDED
#else
#error "<kioto/base/suppress_pop.h> shall be included before <kioto/base/suppress_push.h> is included again"
#endif
