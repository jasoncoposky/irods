#include "irods/collection_entry_stream.hpp"

#include "irods/querySpecColl.h"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsGenQuery.h"
#include "irods/stringOpr.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace irods::experimental::collections
{
    auto project_entry_for_link(
        const collection_entry& _entry,
        std::string_view _original_link_prefix,
        std::string_view _canonical_target_prefix) -> collection_entry
    {
        collection_entry projected = _entry;
        if (!_original_link_prefix.empty() && !_canonical_target_prefix.empty()) {
            projected.coll_name = project_linked_logical_path(
                _original_link_prefix, _canonical_target_prefix, _entry.coll_name);
        }
        return projected;
    }

    auto from_legacy_coll_ent(const collEnt_t& _in) -> collection_entry
    {
        collection_entry out{};
        out.obj_type = _in.objType;
        out.repl_num = _in.replNum;
        out.repl_status = _in.replStatus;
        out.data_mode = _in.dataMode;
        out.data_size = _in.dataSize;

        if (_in.collName) {
            out.coll_name = _in.collName;
        }
        if (_in.dataName) {
            out.data_name = _in.dataName;
        }
        if (_in.dataId) {
            out.data_id = _in.dataId;
        }
        if (_in.createTime) {
            out.create_time = _in.createTime;
        }
        if (_in.modifyTime) {
            out.modify_time = _in.modifyTime;
        }
        if (_in.chksum) {
            out.chksum = _in.chksum;
        }
        if (_in.resource) {
            out.resource = _in.resource;
        }
        if (_in.resc_hier) {
            out.resc_hier = _in.resc_hier;
        }
        if (_in.phyPath) {
            out.phy_path = _in.phyPath;
        }
        if (_in.ownerName) {
            out.owner_name = _in.ownerName;
        }
        if (_in.dataType) {
            out.data_type = _in.dataType;
        }
        if (_in.specColl.collClass != NO_SPEC_COLL) {
            out.spec_coll = _in.specColl;
            out.has_spec_coll = true;
        }

        return out;
    }

    auto to_legacy_coll_ent(const collection_entry& _in, collEnt_t& _out) -> void
    {
        std::memset(&_out, 0, sizeof(collEnt_t));
        _out.objType = _in.obj_type;
        _out.replNum = _in.repl_num;
        _out.replStatus = _in.repl_status;
        _out.dataMode = _in.data_mode;
        _out.dataSize = _in.data_size;

        if (!_in.coll_name.empty()) {
            _out.collName = strdup(_in.coll_name.c_str());
        }
        if (!_in.data_name.empty()) {
            _out.dataName = strdup(_in.data_name.c_str());
        }
        if (!_in.data_id.empty()) {
            _out.dataId = strdup(_in.data_id.c_str());
        }
        if (!_in.create_time.empty()) {
            _out.createTime = strdup(_in.create_time.c_str());
        }
        if (!_in.modify_time.empty()) {
            _out.modifyTime = strdup(_in.modify_time.c_str());
        }
        if (!_in.chksum.empty()) {
            _out.chksum = strdup(_in.chksum.c_str());
        }
        if (!_in.resource.empty()) {
            _out.resource = strdup(_in.resource.c_str());
        }
        if (!_in.resc_hier.empty()) {
            _out.resc_hier = strdup(_in.resc_hier.c_str());
        }
        if (!_in.phy_path.empty()) {
            _out.phyPath = strdup(_in.phy_path.c_str());
        }
        if (!_in.owner_name.empty()) {
            _out.ownerName = strdup(_in.owner_name.c_str());
        }
        if (!_in.data_type.empty()) {
            _out.dataType = strdup(_in.data_type.c_str());
        }
        if (_in.has_spec_coll) {
            _out.specColl = _in.spec_coll;
        }
    }

    auto init_spec_coll_query_out(genQueryOut_t** _out) -> int
    {
        if (!_out) {
            return USER__NULL_INPUT_ERR;
        }

        auto* q_out = static_cast<genQueryOut_t*>(std::malloc(sizeof(genQueryOut_t)));
        if (!q_out) {
            return SYS_MALLOC_ERR;
        }
        std::memset(q_out, 0, sizeof(genQueryOut_t));

        q_out->attriCnt = 5;

        const auto init_col = [](sqlResult_t& _col, int _attr_inx, int _len) {
            _col.attriInx = _attr_inx;
            _col.len = _len;
            const std::size_t total = static_cast<std::size_t>(_len) * MAX_SPEC_COLL_ROW;
            _col.value = static_cast<char*>(std::malloc(total));
            std::memset(_col.value, 0, total);
        };

        init_col(q_out->sqlResult[0], COL_COLL_NAME, MAX_NAME_LEN);
        init_col(q_out->sqlResult[1], COL_DATA_NAME, MAX_NAME_LEN);
        init_col(q_out->sqlResult[2], COL_D_CREATE_TIME, NAME_LEN);
        init_col(q_out->sqlResult[3], COL_D_MODIFY_TIME, NAME_LEN);
        init_col(q_out->sqlResult[4], COL_DATA_SIZE, NAME_LEN);

        q_out->continueInx = -1;
        *_out = q_out;
        return 0;
    }

    auto append_to_spec_coll_query_out(const collection_entry& _entry, genQueryOut_t& _out) -> int
    {
        if (_out.rowCnt >= MAX_SPEC_COLL_ROW) {
            return SYS_OUT_OF_FILE_DESC;
        }

        const int row = _out.rowCnt;
        rstrcpy(&_out.sqlResult[0].value[MAX_NAME_LEN * row], _entry.coll_name.c_str(), MAX_NAME_LEN);
        if (_entry.obj_type == DATA_OBJ_T) {
            rstrcpy(&_out.sqlResult[1].value[MAX_NAME_LEN * row], _entry.data_name.c_str(), MAX_NAME_LEN);
        }
        rstrcpy(&_out.sqlResult[2].value[NAME_LEN * row], _entry.create_time.c_str(), NAME_LEN);
        rstrcpy(&_out.sqlResult[3].value[NAME_LEN * row], _entry.modify_time.c_str(), NAME_LEN);
        std::snprintf(&_out.sqlResult[4].value[NAME_LEN * row], NAME_LEN, "%lld", _entry.data_size);

        _out.rowCnt++;
        return 0;
    }

} // namespace irods::experimental::collections
