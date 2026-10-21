#ifndef IRODS_COLLECTION_TYPES_HPP
#define IRODS_COLLECTION_TYPES_HPP

#include "irods/filesystem/path.hpp"
#include "irods/objInfo.h"
#include "irods/rodsType.h"

#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>

namespace irods::experimental::collections
{
    enum class archive_format
    {
        tar,
        gzip_tar,
        bzip2_tar,
        zip
    };

    struct catalog_collection_info
    {
        rodsLong_t coll_id{};
    };

    struct linked_collection_info
    {
        rodsLong_t coll_id{};
        irods::experimental::filesystem::path target_collection_path;
    };

    struct mounted_collection_info
    {
        rodsLong_t coll_id{};
        std::filesystem::path physical_directory_path;
        std::string resource_hierarchy;
        std::string root_resource;
    };

    struct archive_collection_info
    {
        rodsLong_t coll_id{};
        irods::experimental::filesystem::path archive_logical_path;
        std::filesystem::path archive_physical_path;
        std::filesystem::path cache_directory_path;
        std::string resource_hierarchy;
        std::string root_resource;
        int replica_number{};
        bool cache_dirty{false};
        archive_format format{archive_format::tar};
    };

    using collection_info_variant = std::variant<
        catalog_collection_info,
        linked_collection_info,
        mounted_collection_info,
        archive_collection_info>;

    /// Parses the archive format from a data_type string (e.g., "tar file", "gzipTar", "bzip2Tar", "zipFile").
    auto parse_archive_format(std::string_view _data_type) noexcept -> archive_format;

    /// Returns the canonical iRODS data_type string for an archive_format.
    auto to_data_type_string(archive_format _fmt) noexcept -> std::string_view;

    /// Parses R_COLL_MAIN's coll_info2 string ("<cacheDir>;;;<rescHier>;;;<cacheDirty>") into archive_collection_info.
    auto parse_cached_archive_info2(std::string_view _coll_info2, archive_collection_info& _out) -> int;

    /// Formats archive_collection_info into R_COLL_MAIN's coll_info2 string ("<cacheDir>;;;<rescHier>;;;<cacheDirty>").
    auto format_cached_archive_info2(const archive_collection_info& _info) -> std::string;

    /// Parses R_COLL_MAIN (coll_type, coll_info1, coll_info2) into a strongly-typed collection_info_variant.
    auto parse_special_collection_info(std::string_view _coll_path,
                                       std::string_view _coll_type,
                                       std::string_view _coll_info1,
                                       std::string_view _coll_info2,
                                       collection_info_variant& _out) -> int;

    /// Populates a legacy specColl_t structure from a governing collection path and collection_info_variant.
    auto populate_legacy_spec_coll(std::string_view _governing_coll,
                                   const collection_info_variant& _info,
                                   specColl_t& _out) -> void;

    /// Converts a legacy specColl_t structure into a strongly-typed collection_info_variant.
    auto from_legacy_spec_coll(const specColl_t& _in) -> collection_info_variant;

    /// Composes a physical path inside _physical_root for _target_logical_path under _logical_root,
    /// rejecting prefix mismatches, absolute subpaths, and ".." traversal with SYS_STRUCT_FILE_PATH_ERR.
    auto compose_contained_physical_path(const std::filesystem::path& _physical_root,
                                         std::string_view _logical_root,
                                         std::string_view _target_logical_path,
                                         std::filesystem::path& _out_physical_path) -> int;

    /// Verifies that neither _cache_dir nor any existing prefix component of _relative_subpath
    /// is a symbolic link or a hard-linked regular file (hard_link_count > 1).
    auto validate_cache_path_no_links(const std::filesystem::path& _cache_dir,
                                      const std::filesystem::path& _relative_subpath) -> int;

    /// Projects a canonical logical path back onto the original link collection prefix.
    auto project_linked_logical_path(std::string_view _original_link_prefix,
                                     std::string_view _canonical_target_prefix,
                                     std::string_view _canonical_entry_path) -> std::string;

    enum class link_resolution_mode
    {
        follow_links,
        do_not_follow_link
    };

    struct resolved_path
    {
        irods::experimental::filesystem::path original_path;
        irods::experimental::filesystem::path canonical_path;
        irods::experimental::filesystem::path governing_coll;
        std::filesystem::path relative_subpath;
        bool was_link_translated{false};
        collection_info_variant info{catalog_collection_info{}};
    };

    class collection_resolution_cache
    {
      public:
        auto lookup_positive(std::string_view _logical_path,
                             std::string& _out_governing_coll,
                             collection_info_variant& _out_info) const -> bool;

        auto is_known_negative(std::string_view _logical_path) const -> bool;

        auto store_positive(std::string_view _governing_coll, collection_info_variant _info) -> void;

        auto store_negative(std::string_view _logical_path) -> void;

        auto invalidate(std::string_view _logical_path) -> void;

        auto clear() noexcept -> void;

      private:
        std::unordered_map<std::string, collection_info_variant> positive_entries_;
        std::unordered_set<std::string> negative_paths_;
    };

    using catalog_special_lookup_fn = std::function<int(
        std::string_view _query_path,
        std::string& _out_governing_coll,
        collection_info_variant& _out_info)>;

    /// Resolves a logical path through any LINKED_COLL chain and identifies its governing collection
    /// without mutating caller buffers.
    auto resolve_collection_path_with_lookup(std::string_view _input_path,
                                             link_resolution_mode _mode,
                                             collection_resolution_cache& _cache,
                                             const catalog_special_lookup_fn& _lookup_fn,
                                             resolved_path& _out_resolved) -> int;
} // namespace irods::experimental::collections

#endif // IRODS_COLLECTION_TYPES_HPP
