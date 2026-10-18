#ifndef IRODS_FILESYSTEM_OBJECT_STATUS_HPP
#define IRODS_FILESYSTEM_OBJECT_STATUS_HPP

#include "irods/filesystem/permissions.hpp"

#include <cstdint>
#include <type_traits>
#include <utility>
#include <vector>

namespace irods::experimental::filesystem
{
    enum class object_type
    {
        none,
        not_found,
        data_object,
        collection,
        special_collection,
        unknown
    };

    enum class status_options : std::uint8_t
    {
        // clang-format off
        basic               = 0U,
        include_permissions = 1U << 0U,
        include_inheritance = 1U << 1U,
        full                = include_permissions | include_inheritance
        // clang-format on
    };

    constexpr auto operator|=(status_options& _lhs, status_options _rhs) noexcept -> status_options&
    {
        using T = std::underlying_type_t<status_options>;
        return _lhs = static_cast<status_options>(static_cast<T>(_lhs) | static_cast<T>(_rhs));
    }

    constexpr auto operator|(status_options _lhs, status_options _rhs) noexcept -> status_options
    {
        return _lhs |= _rhs;
    }

    constexpr auto operator&=(status_options& _lhs, status_options _rhs) noexcept -> status_options&
    {
        using T = std::underlying_type_t<status_options>;
        return _lhs = static_cast<status_options>(static_cast<T>(_lhs) & static_cast<T>(_rhs));
    }

    constexpr auto operator&(status_options _lhs, status_options _rhs) noexcept -> status_options
    {
        return _lhs &= _rhs;
    }

    constexpr auto operator^=(status_options& _lhs, status_options _rhs) noexcept -> status_options&
    {
        using T = std::underlying_type_t<status_options>;
        return _lhs = static_cast<status_options>(static_cast<T>(_lhs) ^ static_cast<T>(_rhs));
    }

    constexpr auto operator^(status_options _lhs, status_options _rhs) noexcept -> status_options
    {
        return _lhs ^= _rhs;
    }

    constexpr auto operator~(status_options _value) noexcept -> status_options
    {
        using T = std::underlying_type_t<status_options>;
        return static_cast<status_options>(~static_cast<T>(_value));
    }

    class object_status
    {
    public:
        // Constructors and destructor

        object_status() noexcept
            : object_status{object_type::none}
        {
        }

        object_status(const object_status& _other) = default;
        object_status(object_status&& _other) noexcept = default;

        explicit object_status(object_type _type) noexcept
            : type_{_type}
            , perms_{}
            , inheritance_{}
        {
        }

        explicit object_status(object_type _type, const std::vector<entity_permission>& _perms)
            : type_{_type}
            , perms_{_perms}
            , inheritance_{}
        {
        }

        explicit object_status(object_type _type, std::vector<entity_permission>&& _perms) noexcept
            : type_{_type}
            , perms_{std::move(_perms)}
            , inheritance_{}
        {
        }

        ~object_status() noexcept = default;

        // Assignment operators

        auto operator=(const object_status& _other) -> object_status& = default;
        auto operator=(object_status&& _other) noexcept -> object_status& = default;

        // clang-format off

        // Observers

        auto type() const noexcept -> object_type { return type_; }
        auto permissions() const noexcept -> const std::vector<entity_permission>& { return perms_; }
        auto is_inheritance_enabled() const noexcept -> bool { return inheritance_; }

        // Modifiers

        auto type(object_type _ot) noexcept -> void { type_ = _ot; }
        auto permissions(const std::vector<entity_permission>& _perms) -> void { perms_ = _perms; }
        auto permissions(std::vector<entity_permission>&& _perms) noexcept -> void { perms_ = std::move(_perms); }
        auto inheritance(bool _value) noexcept -> void { inheritance_ = _value; }

        // clang-format on

    private:
        object_type type_;
        std::vector<entity_permission> perms_;
        bool inheritance_;
    };
} // namespace irods::experimental::filesystem

#endif // IRODS_FILESYSTEM_OBJECT_STATUS_HPP
