#ifndef IRODS_FILESYSTEM_PATH_TRAITS_HPP
#define IRODS_FILESYSTEM_PATH_TRAITS_HPP

#include <string>
#include <string_view>
#include <vector>
#include <list>

namespace irods::filesystem
{
    class collection_entry;
        
    namespace path_traits
    {
        template <typename>
        inline static constexpr bool is_pathable = false;

        // clang-format off
        template <> inline constexpr bool is_pathable<char*>             = true;
        template <> inline constexpr bool is_pathable<const char*>       = true;
        template <> inline constexpr bool is_pathable<std::string>       = true;
        template <> inline constexpr bool is_pathable<std::string_view>  = true;
        template <> inline constexpr bool is_pathable<std::vector<char>> = true;
        template <> inline constexpr bool is_pathable<std::list<char>>   = true;
        template <> inline constexpr bool is_pathable<collection_entry>  = true;
        // clang-format on
    } // namespace path_traits
} // namespace irods::filesystem

#endif // IRODS_FILESYSTEM_PATH_TRAITS_HPP
