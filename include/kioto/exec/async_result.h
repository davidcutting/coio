#pragma once
#include <tuple>
#include <type_traits>
#include <variant>
#include <kioto/exec/execution.h>
#include <kioto/base/utility.h>
#include <kioto/base/suppress_push.h> // IWYU pragma: keep

namespace kioto {
    template<typename, typename>
    class async_result;

    template<typename... Values, typename Error>
    class async_result<execution::set_value_t(Values...), execution::set_error_t(Error)> {
    public:
        using sender_concept = execution::sender_tag;
        using receiver_concept = execution::receiver_tag;
        using completion_signatures = execution::completion_signatures<
            execution::set_value_t(Values...),
            execution::set_error_t(Error),
            execution::set_stopped_t()
        >;

    public:
        async_result() = default;

        KIOTO_ALWAYS_INLINE auto set_value(Values... values) noexcept {
            KIOTO_ASSERT(result_.index() == 0);
            result_.template emplace<1>(std::forward<Values>(values)...);
        }

        KIOTO_ALWAYS_INLINE auto set_error(Error e) noexcept -> void {
            KIOTO_ASSERT(result_.index() == 0);
            result_.template emplace<2>(std::forward<Error>(e));
        }

        KIOTO_ALWAYS_INLINE auto set_stopped() noexcept -> void {
            KIOTO_ASSERT(result_.index() == 0);
            result_.template emplace<0>();
        }

        template<similar_to<async_result>, typename...>
        static consteval auto get_completion_signatures() noexcept -> completion_signatures {
            return {};
        }

        template<execution::receiver Rcvr>
        KIOTO_ALWAYS_INLINE auto forward_to(Rcvr&& rcvr) noexcept -> void {
            switch (result_.index()) {
            case 0: {
                execution::set_stopped(std::forward<Rcvr>(rcvr));
                break;
            }
            case 1: {
                std::apply(std::bind_front(execution::set_value, std::forward<Rcvr>(rcvr)), std::move(std::get<1>(result_)));
                break;
            }
            case 2: {
                execution::set_error(std::forward<Rcvr>(rcvr), std::move(std::get<2>(result_)));
                break;
            }
            default: unreachable();
            }
        }

        template<execution::receiver Rcvr>
        KIOTO_ALWAYS_INLINE auto connect(Rcvr rcvr) && noexcept {
            struct state {
                using operation_state_concept = execution::operation_state_tag;

                KIOTO_ALWAYS_INLINE auto start() & noexcept -> void {
                    self_.forward_to(std::move(rcvr_));
                }

                async_result self_;
                Rcvr rcvr_;
            };
            return state{std::move(*this), std::move(rcvr)};
        }

        KIOTO_ALWAYS_INLINE auto affine() && noexcept -> async_result {
            return std::move(*this);
        }

    private:
        std::variant<std::monostate, std::tuple<Values...>, Error> result_;
    };
}

#include <kioto/base/suppress_pop.h> // IWYU pragma: keep
