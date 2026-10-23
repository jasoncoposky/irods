

#ifndef IRODS_LEXICAL_CAST_HPP
#define IRODS_LEXICAL_CAST_HPP

#include "irods/rodsErrorTable.h"
#include "irods/irods_error.hpp"
#include "irods/irods_exception.hpp"

#include <fmt/format.h>

#include <charconv>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>

namespace irods {
    template <typename T, typename S>
    error lexical_cast(const S& _s, T& _t) {
        if constexpr (std::is_same_v<T, std::string> &&
                      (std::is_arithmetic_v<std::decay_t<S>> || std::is_enum_v<std::decay_t<S>>)) {
            _t = fmt::to_string(_s);
            return SUCCESS();
        }
        else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool> &&
                           std::is_convertible_v<S, std::string_view>) {
            const std::string_view sv{_s};
            if (!sv.empty()) {
                T val{};
                const auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
                if (ec == std::errc{} && ptr == sv.data() + sv.size()) {
                    _t = val;
                    return SUCCESS();
                }
            }
            return ERROR(INVALID_LEXICAL_CAST, fmt::format("failed to cast {}", sv));
        }
        else {
            std::stringstream ss;
            if (ss << _s && ss >> _t && ss.eof()) {
                return SUCCESS();
            }
            return ERROR(INVALID_LEXICAL_CAST, fmt::format("failed to cast {}", _s));
        }
    } // lexical_cast

    template <typename T>
    error lexical_cast(T& _t, uint64_t _s) {
        return lexical_cast<T, uint64_t>(_s, _t);
    } // lexical_cast

    template <typename T>
    error lexical_cast(T& _t, const std::string& _s) {
        return lexical_cast<T, std::string>(_s, _t);
    } // lexical_cast

    template <typename T, typename S>
    T lexical_cast(const S& _s) {
        T val{};
        if (const auto err = lexical_cast<T, S>(_s, val); !err.ok()) {
            THROW(err.code(), err.result());
        }
        return val;
    } // lexical_cast

}; // namespace irods

#endif // IRODS_LEXICAL_CAST_HPP

