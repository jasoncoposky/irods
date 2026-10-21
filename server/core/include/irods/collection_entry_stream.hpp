#ifndef IRODS_COLLECTION_ENTRY_STREAM_HPP
#define IRODS_COLLECTION_ENTRY_STREAM_HPP

/// \file

#include "irods/collection_types.hpp"
#include "irods/miscUtil.h"
#include "irods/objInfo.h"
#include "irods/rodsConnect.h"
#include "irods/rodsGenQuery.h"
#include "irods/rodsType.h"

#include <string>
#include <string_view>

namespace irods::experimental::collections
{
    /// Uniform representation of a collection or data object entry yielded during collection iteration.
    struct collection_entry
    {
        objType_t obj_type{UNKNOWN_OBJ_T};
        std::string coll_name;
        std::string data_name;
        std::string data_id;
        std::string create_time;
        std::string modify_time;
        std::string chksum;
        std::string resource;
        std::string resc_hier;
        std::string phy_path;
        std::string owner_name;
        std::string owner_zone;
        std::string data_type;
        rodsLong_t data_size{0};
        int repl_num{0};
        int repl_status{0};
        uint data_mode{0};
        specColl_t spec_coll{};
        bool has_spec_coll{false};
    };

    /// Re-projects a `collection_entry`'s logical `coll_name` from a canonical link target prefix
    /// back onto the user-visible link collection prefix.
    auto project_entry_for_link(
        const collection_entry& _entry,
        std::string_view _original_link_prefix,
        std::string_view _canonical_target_prefix) -> collection_entry;

    /// Converts a legacy `collEnt_t` into a value-typed `collection_entry`.
    auto from_legacy_coll_ent(const collEnt_t& _in) -> collection_entry;

    /// Populates a legacy `collEnt_t` from a value-typed `collection_entry`.
    auto to_legacy_coll_ent(const collection_entry& _in, collEnt_t& _out) -> void;

    /// Allocates and initializes a 5-column `genQueryOut_t` buffer (`COL_COLL_NAME`, `COL_DATA_NAME`,
    /// `COL_D_CREATE_TIME`, `COL_D_MODIFY_TIME`, `COL_DATA_SIZE`) used by `rsQuerySpecColl`.
    auto init_spec_coll_query_out(genQueryOut_t** _out) -> int;

    /// Appends a single `collection_entry` (file or subdirectory) into a `genQueryOut_t` buffer
    /// formatted for `rsQuerySpecColl`.
    auto append_to_spec_coll_query_out(const collection_entry& _entry, genQueryOut_t& _out) -> int;

} // namespace irods::experimental::collections

#endif // IRODS_COLLECTION_ENTRY_STREAM_HPP
