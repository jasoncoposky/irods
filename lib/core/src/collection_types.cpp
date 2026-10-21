#include "irods/collection_types.hpp"

#include "irods/irods_hierarchy_parser.hpp"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/stringOpr.h"

#include <charconv>
#include <cstring>
#include <system_error>

namespace irods::experimental::collections
{
    namespace
    {
        auto extract_root_resource(std::string_view _resc_hier) -> std::string
        {
            if (_resc_hier.empty()) {
                return {};
            }

            irods::hierarchy_parser parser;
            parser.set_string(std::string{_resc_hier});

            std::string first_resc;
            parser.first_resc(first_resc);
            return first_resc;
        }
    } // anonymous namespace

    auto parse_archive_format(std::string_view _data_type) noexcept -> archive_format
    {
        if (_data_type.find(GZIP_TAR_DT_STR) != std::string_view::npos || _data_type == "g" || _data_type == "gzip") {
            return archive_format::gzip_tar;
        }
        if (_data_type.find(BZIP2_TAR_DT_STR) != std::string_view::npos || _data_type == "b" || _data_type == "bzip2") {
            return archive_format::bzip2_tar;
        }
        if (_data_type.find(ZIP_DT_STR) != std::string_view::npos || _data_type == "z" || _data_type == "zip") {
            return archive_format::zip;
        }
        return archive_format::tar;
    }

    auto to_data_type_string(archive_format _fmt) noexcept -> std::string_view
    {
        switch (_fmt) {
            case archive_format::gzip_tar:
                return GZIP_TAR_DT_STR;
            case archive_format::bzip2_tar:
                return BZIP2_TAR_DT_STR;
            case archive_format::zip:
                return ZIP_DT_STR;
            case archive_format::tar:
            default:
                return TAR_DT_STR;
        }
    }

    auto parse_cached_archive_info2(std::string_view _coll_info2, archive_collection_info& _out) -> int
    {
        if (_coll_info2.empty()) {
            _out.cache_directory_path.clear();
            _out.resource_hierarchy.clear();
            _out.root_resource.clear();
            _out.cache_dirty = false;
            return 0;
        }

        constexpr std::string_view delim = ";;;";
        const auto pos1 = _coll_info2.find(delim);
        if (pos1 == std::string_view::npos) {
            rodsLog(LOG_NOTICE, "parse_cached_archive_info2: collInfo2 %.*s format error 1",
                    static_cast<int>(_coll_info2.size()), _coll_info2.data());
            return SYS_COLLINFO_2_FORMAT_ERR;
        }

        const auto rem = _coll_info2.substr(pos1 + delim.size());
        const auto pos2 = rem.find(delim);
        if (pos2 == std::string_view::npos) {
            rodsLog(LOG_NOTICE, "parse_cached_archive_info2: collInfo2 %.*s format error 2",
                    static_cast<int>(_coll_info2.size()), _coll_info2.data());
            return SYS_COLLINFO_2_FORMAT_ERR;
        }

        const auto cache_dir_sv = _coll_info2.substr(0, pos1);
        const auto resc_hier_sv = rem.substr(0, pos2);
        const auto dirty_sv = rem.substr(pos2 + delim.size());

        _out.cache_directory_path = std::filesystem::path{cache_dir_sv};
        _out.resource_hierarchy = std::string{resc_hier_sv};
        _out.root_resource = extract_root_resource(resc_hier_sv);

        int dirty_val = 0;
        std::from_chars(dirty_sv.data(), dirty_sv.data() + dirty_sv.size(), dirty_val);
        _out.cache_dirty = (dirty_val != 0);

        return 0;
    }

    auto format_cached_archive_info2(const archive_collection_info& _info) -> std::string
    {
        if (_info.cache_directory_path.empty() || (_info.resource_hierarchy.empty() && _info.root_resource.empty())) {
            return {};
        }

        const auto& hier = !_info.resource_hierarchy.empty() ? _info.resource_hierarchy : _info.root_resource;
        std::string result;
        const auto cache_str = _info.cache_directory_path.string();
        result.reserve(cache_str.size() + hier.size() + 8);
        result.append(cache_str);
        result.append(";;;");
        result.append(hier);
        result.append(";;;");
        result.push_back(_info.cache_dirty ? '1' : '0');
        return result;
    }

    auto parse_special_collection_info(std::string_view _coll_path,
                                       std::string_view _coll_type,
                                       std::string_view _coll_info1,
                                       std::string_view _coll_info2,
                                       collection_info_variant& _out) -> int
    {
        static_cast<void>(_coll_path);

        if (_coll_type.empty() || _coll_type == "NULL_SPECIAL_VALUE") {
            _out = catalog_collection_info{};
            return 0;
        }

        if (_coll_type == MOUNT_POINT_STR) {
            mounted_collection_info mnt{};
            mnt.physical_directory_path = std::filesystem::path{_coll_info1};
            mnt.resource_hierarchy = std::string{_coll_info2};
            mnt.root_resource = extract_root_resource(_coll_info2);
            _out = std::move(mnt);
            return 0;
        }

        if (_coll_type == LINK_POINT_STR) {
            linked_collection_info link{};
            link.target_collection_path = irods::experimental::filesystem::path{std::string{_coll_info1}};
            _out = std::move(link);
            return 0;
        }

        if (_coll_type == TAR_STRUCT_FILE_STR) {
            archive_collection_info arch{};
            arch.archive_logical_path = irods::experimental::filesystem::path{std::string{_coll_info1}};
            arch.format = archive_format::tar;
            if (const int ec = parse_cached_archive_info2(_coll_info2, arch); ec < 0) {
                return ec;
            }
            _out = std::move(arch);
            return 0;
        }

        _out = catalog_collection_info{};
        rodsLog(LOG_ERROR, "parse_special_collection_info: unmatched specColl type %.*s",
                static_cast<int>(_coll_type.size()), _coll_type.data());
        return SYS_UNMATCHED_SPEC_COLL_TYPE;
    }

    auto populate_legacy_spec_coll(std::string_view _governing_coll,
                                   const collection_info_variant& _info,
                                   specColl_t& _out) -> void
    {
        std::memset(&_out, 0, sizeof(specColl_t));
        rstrcpy(_out.collection, std::string{_governing_coll}.c_str(), MAX_NAME_LEN);

        std::visit(
            [&_out](const auto& _item) {
                using T = std::decay_t<decltype(_item)>;
                if constexpr (std::is_same_v<T, catalog_collection_info>) {
                    _out.collClass = NO_SPEC_COLL;
                }
                else if constexpr (std::is_same_v<T, linked_collection_info>) {
                    _out.collClass = LINKED_COLL;
                    rstrcpy(_out.phyPath, _item.target_collection_path.c_str(), MAX_NAME_LEN);
                }
                else if constexpr (std::is_same_v<T, mounted_collection_info>) {
                    _out.collClass = MOUNTED_COLL;
                    rstrcpy(_out.phyPath, _item.physical_directory_path.c_str(), MAX_NAME_LEN);
                    rstrcpy(_out.rescHier, _item.resource_hierarchy.c_str(), MAX_NAME_LEN);
                    rstrcpy(_out.resource, _item.root_resource.c_str(), NAME_LEN);
                }
                else if constexpr (std::is_same_v<T, archive_collection_info>) {
                    _out.collClass = STRUCT_FILE_COLL;
                    _out.type = TAR_STRUCT_FILE_T;
                    rstrcpy(_out.objPath, _item.archive_logical_path.c_str(), MAX_NAME_LEN);
                    rstrcpy(_out.phyPath, _item.archive_physical_path.c_str(), MAX_NAME_LEN);
                    rstrcpy(_out.cacheDir, _item.cache_directory_path.c_str(), MAX_NAME_LEN);
                    rstrcpy(_out.rescHier, _item.resource_hierarchy.c_str(), MAX_NAME_LEN);
                    rstrcpy(_out.resource, _item.root_resource.c_str(), NAME_LEN);
                    _out.cacheDirty = _item.cache_dirty ? 1 : 0;
                    _out.replNum = _item.replica_number;
                }
            },
            _info);
    }

    auto from_legacy_spec_coll(const specColl_t& _in) -> collection_info_variant
    {
        switch (_in.collClass) {
            case LINKED_COLL: {
                linked_collection_info link{};
                link.target_collection_path = irods::experimental::filesystem::path{_in.phyPath};
                return link;
            }
            case MOUNTED_COLL: {
                mounted_collection_info mnt{};
                mnt.physical_directory_path = std::filesystem::path{_in.phyPath};
                mnt.resource_hierarchy = _in.rescHier;
                mnt.root_resource = (_in.resource[0] != '\0') ? std::string{_in.resource}
                                                              : extract_root_resource(_in.rescHier);
                return mnt;
            }
            case STRUCT_FILE_COLL: {
                archive_collection_info arch{};
                arch.archive_logical_path = irods::experimental::filesystem::path{_in.objPath};
                arch.archive_physical_path = std::filesystem::path{_in.phyPath};
                arch.cache_directory_path = std::filesystem::path{_in.cacheDir};
                arch.resource_hierarchy = _in.rescHier;
                arch.root_resource = (_in.resource[0] != '\0') ? std::string{_in.resource}
                                                               : extract_root_resource(_in.rescHier);
                arch.replica_number = _in.replNum;
                arch.cache_dirty = (_in.cacheDirty != 0);
                arch.format = archive_format::tar;
                return arch;
            }
            case NO_SPEC_COLL:
            default:
                return catalog_collection_info{};
        }
    }

    auto compose_contained_physical_path(const std::filesystem::path& _physical_root,
                                         std::string_view _logical_root,
                                         std::string_view _target_logical_path,
                                         std::filesystem::path& _out_physical_path) -> int
    {
        while (_logical_root.size() > 1 && _logical_root.back() == '/') {
            _logical_root.remove_suffix(1);
        }

        if (_target_logical_path.size() < _logical_root.size() ||
            _target_logical_path.substr(0, _logical_root.size()) != _logical_root)
        {
            return SYS_STRUCT_FILE_PATH_ERR;
        }

        auto suffix = _target_logical_path.substr(_logical_root.size());
        if (!suffix.empty() && suffix.front() != '/') {
            return SYS_STRUCT_FILE_PATH_ERR;
        }

        while (!suffix.empty() && suffix.front() == '/') {
            suffix.remove_prefix(1);
        }

        if (suffix.empty()) {
            _out_physical_path = _physical_root;
            return 0;
        }

        std::filesystem::path rel_path{suffix};
        rel_path = rel_path.lexically_normal();

        if (rel_path.is_absolute() || rel_path.has_root_name()) {
            return SYS_STRUCT_FILE_PATH_ERR;
        }

        if (!rel_path.empty() && *rel_path.begin() == "..") {
            return SYS_STRUCT_FILE_PATH_ERR;
        }

        if (rel_path == ".") {
            _out_physical_path = _physical_root;
            return 0;
        }

        _out_physical_path = _physical_root / rel_path;
        return 0;
    }

    auto validate_cache_path_no_links(const std::filesystem::path& _cache_dir,
                                      const std::filesystem::path& _relative_subpath) -> int
    {
        std::error_code ec;
        const auto cache_status = std::filesystem::symlink_status(_cache_dir, ec);
        if (ec) {
            return UNIX_FILE_STAT_ERR - ec.value();
        }

        if (std::filesystem::is_symlink(cache_status)) {
            return SYS_STRUCT_FILE_PATH_ERR;
        }

        const auto norm_rel = _relative_subpath.lexically_normal();
        if (norm_rel.is_absolute() || (!norm_rel.empty() && *norm_rel.begin() == "..")) {
            return SYS_STRUCT_FILE_PATH_ERR;
        }

        auto current_path = _cache_dir;
        for (auto itr = norm_rel.begin(), end = norm_rel.end(); itr != end; ++itr) {
            if (*itr == "." || itr->empty()) {
                continue;
            }
            current_path /= *itr;

            ec.clear();
            const auto st = std::filesystem::symlink_status(current_path, ec);
            if (st.type() == std::filesystem::file_type::not_found) {
                break;
            }
            if (ec) {
                return UNIX_FILE_STAT_ERR - ec.value();
            }
            if (std::filesystem::is_symlink(st)) {
                return SYS_STRUCT_FILE_PATH_ERR;
            }

            auto next_itr = itr;
            ++next_itr;
            if (next_itr == end && std::filesystem::is_regular_file(st)) {
                ec.clear();
                const auto link_cnt = std::filesystem::hard_link_count(current_path, ec);
                if (ec) {
                    return UNIX_FILE_STAT_ERR - ec.value();
                }
                if (link_cnt > 1) {
                    return SYS_STRUCT_FILE_PATH_ERR;
                }
            }
        }

        return 0;
    }

    auto project_linked_logical_path(std::string_view _original_link_prefix,
                                     std::string_view _canonical_target_prefix,
                                     std::string_view _canonical_entry_path) -> std::string
    {
        while (_original_link_prefix.size() > 1 && _original_link_prefix.back() == '/') {
            _original_link_prefix.remove_suffix(1);
        }
        while (_canonical_target_prefix.size() > 1 && _canonical_target_prefix.back() == '/') {
            _canonical_target_prefix.remove_suffix(1);
        }

        if (_canonical_entry_path.size() < _canonical_target_prefix.size() ||
            _canonical_entry_path.substr(0, _canonical_target_prefix.size()) != _canonical_target_prefix)
        {
            return std::string{_canonical_entry_path};
        }

        const auto suffix = _canonical_entry_path.substr(_canonical_target_prefix.size());
        if (!suffix.empty() && suffix.front() != '/') {
            return std::string{_canonical_entry_path};
        }

        std::string projected;
        projected.reserve(_original_link_prefix.size() + suffix.size());
        projected.append(_original_link_prefix);
        projected.append(suffix);
        return projected;
    }

    namespace
    {
        auto is_path_or_descendant(std::string_view _candidate, std::string_view _prefix) noexcept -> bool
        {
            while (_prefix.size() > 1 && _prefix.back() == '/') {
                _prefix.remove_suffix(1);
            }
            if (_candidate.size() < _prefix.size() || _candidate.substr(0, _prefix.size()) != _prefix) {
                return false;
            }
            return _candidate.size() == _prefix.size() || _candidate[_prefix.size()] == '/';
        }

        auto normalize_logical_sv(std::string_view _p) -> std::string
        {
            while (_p.size() > 1 && _p.back() == '/') {
                _p.remove_suffix(1);
            }
            return std::string{_p};
        }
    } // anonymous namespace

    auto collection_resolution_cache::lookup_positive(std::string_view _logical_path,
                                                      std::string& _out_governing_coll,
                                                      collection_info_variant& _out_info) const -> bool
    {
        const auto norm = normalize_logical_sv(_logical_path);
        std::string current = norm;
        while (!current.empty() && current != "/") {
            if (auto it = positive_entries_.find(current); it != positive_entries_.end()) {
                _out_governing_coll = it->first;
                _out_info = it->second;
                return true;
            }
            const auto slash = current.rfind('/');
            if (slash == std::string::npos || slash == 0) {
                break;
            }
            current.resize(slash);
        }
        return false;
    }

    auto collection_resolution_cache::is_known_negative(std::string_view _logical_path) const -> bool
    {
        return negative_paths_.find(normalize_logical_sv(_logical_path)) != negative_paths_.end();
    }

    auto collection_resolution_cache::store_positive(std::string_view _governing_coll,
                                                     collection_info_variant _info) -> void
    {
        auto key = normalize_logical_sv(_governing_coll);
        negative_paths_.erase(key);
        positive_entries_[std::move(key)] = std::move(_info);
    }

    auto collection_resolution_cache::store_negative(std::string_view _logical_path) -> void
    {
        if (negative_paths_.size() >= 256) {
            negative_paths_.clear();
        }
        negative_paths_.insert(normalize_logical_sv(_logical_path));
    }

    auto collection_resolution_cache::invalidate(std::string_view _logical_path) -> void
    {
        const auto norm = normalize_logical_sv(_logical_path);
        for (auto it = positive_entries_.begin(); it != positive_entries_.end();) {
            if (is_path_or_descendant(it->first, norm) || is_path_or_descendant(norm, it->first)) {
                it = positive_entries_.erase(it);
            }
            else {
                ++it;
            }
        }
        for (auto it = negative_paths_.begin(); it != negative_paths_.end();) {
            if (is_path_or_descendant(*it, norm) || is_path_or_descendant(norm, *it)) {
                it = negative_paths_.erase(it);
            }
            else {
                ++it;
            }
        }
    }

    auto collection_resolution_cache::clear() noexcept -> void
    {
        positive_entries_.clear();
        negative_paths_.clear();
    }

    auto resolve_collection_path_with_lookup(std::string_view _input_path,
                                             link_resolution_mode _mode,
                                             collection_resolution_cache& _cache,
                                             const catalog_special_lookup_fn& _lookup_fn,
                                             resolved_path& _out_resolved) -> int
    {
        if (_input_path.empty()) {
            return USER__NULL_INPUT_ERR;
        }

        const auto orig_norm = normalize_logical_sv(_input_path);
        _out_resolved.original_path = irods::experimental::filesystem::path{orig_norm};

        std::string current_path = orig_norm;
        bool link_translated = false;
        int link_count = 0;

        while (true) {
            std::string governing_coll;
            collection_info_variant info{catalog_collection_info{}};

            bool found_special = _cache.lookup_positive(current_path, governing_coll, info);
            if (!found_special && !_cache.is_known_negative(current_path)) {
                const int status = _lookup_fn(current_path, governing_coll, info);
                if (status >= 0 && !std::holds_alternative<catalog_collection_info>(info)) {
                    _cache.store_positive(governing_coll, info);
                    found_special = true;
                }
                else if (status == CAT_NO_ROWS_FOUND || std::holds_alternative<catalog_collection_info>(info)) {
                    _cache.store_negative(current_path);
                }
                else if (status < 0) {
                    return status;
                }
            }

            if (!found_special) {
                _out_resolved.canonical_path = irods::experimental::filesystem::path{current_path};
                _out_resolved.governing_coll = _out_resolved.canonical_path;
                _out_resolved.relative_subpath.clear();
                _out_resolved.was_link_translated = link_translated;
                _out_resolved.info = catalog_collection_info{};
                return 0;
            }

            if (const auto* link_info = std::get_if<linked_collection_info>(&info)) {
                if (_mode == link_resolution_mode::do_not_follow_link && current_path == governing_coll) {
                    _out_resolved.canonical_path = irods::experimental::filesystem::path{current_path};
                    _out_resolved.governing_coll = irods::experimental::filesystem::path{governing_coll};
                    _out_resolved.relative_subpath.clear();
                    _out_resolved.was_link_translated = link_translated;
                    _out_resolved.info = info;
                    return 0;
                }

                if (++link_count > MAX_LINK_CNT) {
                    return SYS_COLL_LINK_PATH_ERR;
                }

                const auto target_str = normalize_logical_sv(link_info->target_collection_path.string());
                const auto suffix = std::string_view{current_path}.substr(governing_coll.size());
                if (target_str.size() + suffix.size() >= MAX_NAME_LEN) {
                    return USER_STRLEN_TOOLONG;
                }

                std::string next_path;
                next_path.reserve(target_str.size() + suffix.size());
                next_path.append(target_str);
                next_path.append(suffix);
                current_path = std::move(next_path);
                link_translated = true;
                continue;
            }

            // Governing collection is MOUNTED_COLL or ARCHIVE_COLL
            std::filesystem::path rel_subpath;
            if (current_path.size() > governing_coll.size()) {
                auto suffix = std::string_view{current_path}.substr(governing_coll.size());
                while (!suffix.empty() && suffix.front() == '/') {
                    suffix.remove_prefix(1);
                }
                rel_subpath = std::filesystem::path{suffix}.lexically_normal();
                if (rel_subpath.is_absolute() || (!rel_subpath.empty() && *rel_subpath.begin() == "..")) {
                    return SYS_STRUCT_FILE_PATH_ERR;
                }
                if (rel_subpath == ".") {
                    rel_subpath.clear();
                }
            }

            _out_resolved.canonical_path = irods::experimental::filesystem::path{current_path};
            _out_resolved.governing_coll = irods::experimental::filesystem::path{governing_coll};
            _out_resolved.relative_subpath = std::move(rel_subpath);
            _out_resolved.was_link_translated = link_translated;
            _out_resolved.info = std::move(info);
            return 0;
        }
    }
} // namespace irods::experimental::collections
