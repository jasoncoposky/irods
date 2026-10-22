#ifndef IRODS_FILESYSTEM_PATH_UTILITIES_HPP
#define IRODS_FILESYSTEM_PATH_UTILITIES_HPP

#include "irods/filesystem/path.hpp"
#include "irods/filesystem/filesystem_error.hpp"

#include "irods/rodsDef.h"
#include "irods/rodsErrorTable.h"
#include "irods/system_error.hpp"

#include <cstddef>
#include <cstring>
#include <string_view>
#include <system_error>

namespace irods::filesystem
{
    /// Returns whether the path element \p _c represents the preferred path separator.
    ///
    /// \since 4.2.12
    inline constexpr auto is_separator(path::value_type _c) noexcept -> bool
    {
        return path::preferred_separator == _c;
    }

    inline auto validate_path_not_empty(const path& _p, std::error_code& _ec) noexcept -> bool
    {
        if (_p.empty()) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return false;
        }
        _ec.clear();
        return true;
    }

    inline auto validate_path_length(std::string_view _p, std::error_code& _ec) noexcept -> bool
    {
        if (_p.size() >= MAX_NAME_LEN) {
            _ec = make_error_code(USER_PATH_EXCEEDS_MAX);
            return false;
        }
        if (_p.find('\0') != std::string_view::npos) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return false;
        }
        _ec.clear();
        return true;
    }

    inline auto validate_path(const path& _p, std::error_code& _ec) noexcept -> bool
    {
        return validate_path_not_empty(_p, _ec) && validate_path_length(_p.string_view(), _ec);
    }

    template <std::size_t N>
    inline auto copy_to_c_array(char (&_dst)[N], std::string_view _src, std::error_code& _ec) noexcept -> bool
    {
        if (_src.size() >= N) {
            _ec = make_error_code(USER_PATH_EXCEEDS_MAX);
            return false;
        }
        if (_src.find('\0') != std::string_view::npos) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return false;
        }
        if (!_src.empty()) {
            std::memcpy(_dst, _src.data(), _src.size());
        }
        _dst[_src.size()] = '\0';
        _ec.clear();
        return true;
    }

    /// Throws a filesystem_error if the length of path \p _p exceeds the maximum length
    /// allowed for a logical path.
    ///
    /// \since 4.2.12
    inline auto throw_if_path_length_exceeds_limit(const path& _p) -> void
    {
        std::error_code ec;
        if (!validate_path_length(_p.string_view(), ec)) {
            throw filesystem_error{"path length cannot exceed max path size", _p, ec};
        }
    }

    /// Throws a filesystem_error if the path, \p _p, is empty.
    ///
    /// \since 4.2.12
    inline auto throw_if_path_is_empty(const path& _p) -> void
    {
        std::error_code ec;
        if (!validate_path_not_empty(_p, ec)) {
            throw filesystem_error{"empty path", _p, ec};
        }
    }
} // namespace irods::filesystem

#endif // IRODS_FILESYSTEM_PATH_UTILITIES_HPP
