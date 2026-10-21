#include "irods/collection_driver.hpp"

#include "irods/collection.hpp"
#include "irods/fileClosedir.h"
#include "irods/fileMkdir.h"
#include "irods/fileOpendir.h"
#include "irods/fileReaddir.h"
#include "irods/fileRmdir.h"
#include "irods/fileStat.h"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/irods_resource_backport.hpp"
#include "irods/objMetaOpr.hpp"
#include "irods/physPath.hpp"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/rsCollCreate.hpp"
#include "irods/rsFileClosedir.hpp"
#include "irods/rsFileMkdir.hpp"
#include "irods/rsFileOpendir.hpp"
#include "irods/rsFileReaddir.hpp"
#include "irods/rsFileRmdir.hpp"
#include "irods/rsFileStat.hpp"
#include "irods/rsObjStat.hpp"
#include "irods/rsRegColl.hpp"
#include "irods/rsRmColl.hpp"
#include "irods/rsStructFileSync.hpp"
#include "irods/rsSubStructFileClosedir.hpp"
#include "irods/rsSubStructFileMkdir.hpp"
#include "irods/rsSubStructFileOpendir.hpp"
#include "irods/rsSubStructFileReaddir.hpp"
#include "irods/rsSubStructFileRmdir.hpp"
#include "irods/rsSubStructFileStat.hpp"
#include "irods/rsSyncMountedColl.hpp"
#include "irods/specColl.hpp"
#include "irods/stringOpr.h"
#include "irods/subStructFileClosedir.h"
#include "irods/subStructFileMkdir.h"
#include "irods/subStructFileOpendir.h"
#include "irods/subStructFileReaddir.h"
#include "irods/subStructFileRmdir.h"
#include "irods/subStructFileStat.h"

#include <cstring>
#include <string>

namespace irods::experimental::collections
{
    namespace
    {
        const CatalogCollectionDriver g_catalog_driver{};
        const MountedCollectionDriver g_mounted_driver{};
        const ArchiveCollectionDriver g_archive_driver{};
    } // anonymous namespace

    // -------------------------------------------------------------------------
    // CatalogCollectionDriver
    // -------------------------------------------------------------------------

    auto CatalogCollectionDriver::stat_path(
        RsComm& _comm,
        const resolved_path& _resolved,
        rodsObjStat_t** _out) const -> int
    {
        dataObjInp_t inp{};
        rstrcpy(inp.objPath, _resolved.canonical_path.c_str(), MAX_NAME_LEN);

        int status = dataObjStat(&_comm, &inp, _out);
        if (status >= 0) {
            return status;
        }

        status = collStat(&_comm, &inp, _out);
        if (status >= 0) {
            if (*_out && (*_out)->specColl == nullptr) {
                specCollCache_t* spec_cache = nullptr;
                if (getSpecCollCache(&_comm, inp.objPath, 0, &spec_cache) >= 0) {
                    replSpecColl(&spec_cache->specColl, &(*_out)->specColl);
                }
            }
            return status;
        }

        return USER_FILE_DOES_NOT_EXIST;
    }

    auto CatalogCollectionDriver::create_directory(
        RsComm& _comm,
        const resolved_path& _resolved,
        collInp_t& _inp) const -> int
    {
        rstrcpy(_inp.collName, _resolved.canonical_path.c_str(), MAX_NAME_LEN);
        if (isColl(&_comm, _inp.collName, nullptr) >= 0) {
            return CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME;
        }
        const int rc = _rsRegColl(&_comm, &_inp);
        if (rc >= 0) {
            invalidate_collection_cache(_resolved.canonical_path.string_view());
        }
        return rc;
    }

    auto CatalogCollectionDriver::remove_directory(
        RsComm& _comm,
        const resolved_path& _resolved,
        collInp_t& _inp) const -> int
    {
        rstrcpy(_inp.collName, _resolved.canonical_path.c_str(), MAX_NAME_LEN);
        const int rc = svrUnregColl(&_comm, &_inp);
        if (rc >= 0) {
            invalidate_collection_cache(_resolved.canonical_path.string_view());
        }
        return rc;
    }

    auto CatalogCollectionDriver::sync(
        RsComm&,
        const resolved_path&,
        int) const -> int
    {
        return SYS_COLL_NOT_MOUNTED_ERR;
    }

    auto CatalogCollectionDriver::open_dir(
        RsComm&,
        dataObjInfo_t&) const -> int
    {
        return SYS_UNKNOWN_SPEC_COLL_CLASS;
    }

    auto CatalogCollectionDriver::read_dir(
        RsComm&,
        const dataObjInfo_t&,
        int,
        rodsDirent_t**) const -> int
    {
        return SYS_UNKNOWN_SPEC_COLL_CLASS;
    }

    auto CatalogCollectionDriver::close_dir(
        RsComm&,
        const dataObjInfo_t&,
        int) const -> int
    {
        return SYS_UNKNOWN_SPEC_COLL_CLASS;
    }

    auto CatalogCollectionDriver::stat_entry(
        RsComm& _comm,
        dataObjInfo_t& _info,
        rodsStat_t** _out) const -> int
    {
        fileStatInp_t file_stat_inp{};
        rstrcpy(file_stat_inp.fileName, _info.filePath, MAX_NAME_LEN);
        rstrcpy(file_stat_inp.rescHier, _info.rescHier, MAX_NAME_LEN);
        rstrcpy(file_stat_inp.objPath, _info.objPath, MAX_NAME_LEN);
        return rsFileStat(&_comm, &file_stat_inp, _out);
    }

    // -------------------------------------------------------------------------
    // MountedCollectionDriver
    // -------------------------------------------------------------------------

    auto MountedCollectionDriver::stat_path(
        RsComm& _comm,
        const resolved_path& _resolved,
        rodsObjStat_t** _out) const -> int
    {
        dataObjInp_t inp{};
        rstrcpy(inp.objPath, _resolved.canonical_path.c_str(), MAX_NAME_LEN);

        if (const int status = collStat(&_comm, &inp, _out); status >= 0) {
            if (*_out && (*_out)->specColl == nullptr) {
                specCollCache_t* spec_cache = nullptr;
                if (getSpecCollCache(&_comm, inp.objPath, 0, &spec_cache) >= 0) {
                    replSpecColl(&spec_cache->specColl, &(*_out)->specColl);
                }
            }
            return status;
        }

        const int status = statPathInSpecColl(&_comm, inp.objPath, 0, _out);
        return status < 0 ? USER_FILE_DOES_NOT_EXIST : status;
    }

    auto MountedCollectionDriver::create_directory(
        RsComm& _comm,
        const resolved_path& _resolved,
        collInp_t&) const -> int
    {
        dataObjInfo_t* data_obj_info = nullptr;
        const irods::at_scope_exit free_info{[&data_obj_info] { freeDataObjInfo(data_obj_info); }};

        char path_buf[MAX_NAME_LEN]{};
        rstrcpy(path_buf, _resolved.canonical_path.c_str(), MAX_NAME_LEN);

        const int status = resolvePathInSpecColl(&_comm, path_buf, WRITE_COLL_PERM, 0, &data_obj_info);
        if (status >= 0) {
            return status == COLL_OBJ_T ? 0 : USER_INPUT_PATH_ERR;
        }
        if (status == SYS_SPEC_COLL_OBJ_NOT_EXIST) {
            if (data_obj_info && data_obj_info->specColl &&
                data_obj_info->specColl->collClass == LINKED_COLL)
            {
                return SYS_COLL_LINK_PATH_ERR;
            }
            return l3Mkdir(&_comm, data_obj_info);
        }
        return status;
    }

    auto MountedCollectionDriver::remove_directory(
        RsComm& _comm,
        const resolved_path& _resolved,
        collInp_t& _inp) const -> int
    {
        dataObjInfo_t* data_obj_info = nullptr;
        const irods::at_scope_exit free_info{[&data_obj_info] { freeDataObjInfo(data_obj_info); }};

        char path_buf[MAX_NAME_LEN]{};
        rstrcpy(path_buf, _resolved.canonical_path.c_str(), MAX_NAME_LEN);

        const int status = resolvePathInSpecColl(&_comm, path_buf, WRITE_COLL_PERM, 0, &data_obj_info);
        if (status < 0 && status != CAT_NO_ROWS_FOUND) {
            return status;
        }
        if (status == COLL_OBJ_T && data_obj_info && data_obj_info->specColl) {
            if (data_obj_info->specColl->collClass == LINKED_COLL) {
                rstrcpy(_inp.collName, data_obj_info->objPath, MAX_NAME_LEN);
                return svrUnregColl(&_comm, &_inp);
            }
            return l3Rmdir(&_comm, data_obj_info);
        }
        return svrUnregColl(&_comm, &_inp);
    }

    auto MountedCollectionDriver::sync(
        RsComm&,
        const resolved_path&,
        int) const -> int
    {
        return SYS_COLL_NOT_MOUNTED_ERR;
    }

    auto MountedCollectionDriver::open_dir(
        RsComm& _comm,
        dataObjInfo_t& _info) const -> int
    {
        std::string location;
        if (const auto ret = irods::get_loc_for_hier_string(_info.rescHier, location); !ret.ok()) {
            irods::log(PASSMSG("MountedCollectionDriver::open_dir - failed in get_loc_for_hier_string", ret));
            return -1;
        }

        fileOpendirInp_t inp{};
        rstrcpy(inp.dirName, _info.filePath, MAX_NAME_LEN);
        rstrcpy(inp.resc_name_, _info.rescName, MAX_NAME_LEN);
        rstrcpy(inp.resc_hier_, _info.rescHier, MAX_NAME_LEN);
        rstrcpy(inp.addr.hostAddr, location.c_str(), NAME_LEN);

        const int status = rsFileOpendir(&_comm, &inp);
        if (status < 0) {
            rodsLog(LOG_ERROR, "l3Opendir: rsFileOpendir for %s error, status = %d", _info.filePath, status);
        }
        return status;
    }

    auto MountedCollectionDriver::read_dir(
        RsComm& _comm,
        const dataObjInfo_t&,
        int _l3_desc_inx,
        rodsDirent_t** _out) const -> int
    {
        fileReaddirInp_t inp{};
        inp.fileInx = _l3_desc_inx;
        return rsFileReaddir(&_comm, &inp, _out);
    }

    auto MountedCollectionDriver::close_dir(
        RsComm& _comm,
        const dataObjInfo_t&,
        int _l3_desc_inx) const -> int
    {
        fileClosedirInp_t inp{};
        inp.fileInx = _l3_desc_inx;
        return rsFileClosedir(&_comm, &inp);
    }

    auto MountedCollectionDriver::stat_entry(
        RsComm& _comm,
        dataObjInfo_t& _info,
        rodsStat_t** _out) const -> int
    {
        fileStatInp_t file_stat_inp{};
        rstrcpy(file_stat_inp.fileName, _info.filePath, MAX_NAME_LEN);
        rstrcpy(file_stat_inp.rescHier, _info.rescHier, MAX_NAME_LEN);
        rstrcpy(file_stat_inp.objPath, _info.objPath, MAX_NAME_LEN);
        return rsFileStat(&_comm, &file_stat_inp, _out);
    }

    // -------------------------------------------------------------------------
    // ArchiveCollectionDriver
    // -------------------------------------------------------------------------

    auto ArchiveCollectionDriver::stat_path(
        RsComm& _comm,
        const resolved_path& _resolved,
        rodsObjStat_t** _out) const -> int
    {
        return mounted_driver_.stat_path(_comm, _resolved, _out);
    }

    auto ArchiveCollectionDriver::create_directory(
        RsComm& _comm,
        const resolved_path& _resolved,
        collInp_t& _inp) const -> int
    {
        return mounted_driver_.create_directory(_comm, _resolved, _inp);
    }

    auto ArchiveCollectionDriver::remove_directory(
        RsComm& _comm,
        const resolved_path& _resolved,
        collInp_t& _inp) const -> int
    {
        return mounted_driver_.remove_directory(_comm, _resolved, _inp);
    }

    auto ArchiveCollectionDriver::sync(
        RsComm& _comm,
        const resolved_path& _resolved,
        int _opr_type) const -> int
    {
        specColl_t legacy_spec{};
        populate_legacy_spec_coll(_resolved.governing_coll.string_view(), _resolved.info, legacy_spec);
        return _rsSyncMountedColl(&_comm, &legacy_spec, _opr_type);
    }

    auto ArchiveCollectionDriver::open_dir(
        RsComm& _comm,
        dataObjInfo_t& _info) const -> int
    {
        std::string location;
        if (const auto ret = irods::get_loc_for_hier_string(_info.rescHier, location); !ret.ok()) {
            irods::log(PASSMSG("ArchiveCollectionDriver::open_dir - failed in get_loc_for_hier_string", ret));
            return -1;
        }

        subFile_t inp{};
        rstrcpy(inp.subFilePath, _info.subPath, MAX_NAME_LEN);
        rstrcpy(inp.addr.hostAddr, location.c_str(), NAME_LEN);
        inp.specColl = _info.specColl;
        return rsSubStructFileOpendir(&_comm, &inp);
    }

    auto ArchiveCollectionDriver::read_dir(
        RsComm& _comm,
        const dataObjInfo_t& _info,
        int _l3_desc_inx,
        rodsDirent_t** _out) const -> int
    {
        std::string location;
        if (const auto ret = irods::get_loc_for_hier_string(_info.rescHier, location); !ret.ok()) {
            irods::log(PASSMSG("ArchiveCollectionDriver::read_dir - failed in get_loc_for_hier_string", ret));
            return -1;
        }

        subStructFileFdOprInp_t inp{};
        inp.type = _info.specColl->type;
        inp.fd = _l3_desc_inx;
        rstrcpy(inp.addr.hostAddr, location.c_str(), NAME_LEN);
        rstrcpy(inp.resc_hier, _info.rescHier, MAX_NAME_LEN);
        return rsSubStructFileReaddir(&_comm, &inp, _out);
    }

    auto ArchiveCollectionDriver::close_dir(
        RsComm& _comm,
        const dataObjInfo_t& _info,
        int _l3_desc_inx) const -> int
    {
        std::string location;
        if (const auto ret = irods::get_loc_for_hier_string(_info.rescHier, location); !ret.ok()) {
            irods::log(PASSMSG("ArchiveCollectionDriver::close_dir - failed in get_loc_for_hier_string", ret));
            return -1;
        }

        subStructFileFdOprInp_t inp{};
        inp.type = _info.specColl->type;
        inp.fd = _l3_desc_inx;
        rstrcpy(inp.addr.hostAddr, location.c_str(), NAME_LEN);
        rstrcpy(inp.resc_hier, _info.rescHier, MAX_NAME_LEN);
        return rsSubStructFileClosedir(&_comm, &inp);
    }

    auto ArchiveCollectionDriver::stat_entry(
        RsComm& _comm,
        dataObjInfo_t& _info,
        rodsStat_t** _out) const -> int
    {
        std::string location;
        if (const auto ret = irods::get_loc_for_hier_string(_info.rescHier, location); !ret.ok()) {
            irods::log(PASSMSG("ArchiveCollectionDriver::stat_entry - failed in get_loc_for_hier_string", ret));
            return -1;
        }

        subFile_t sub_file{};
        rstrcpy(sub_file.subFilePath, _info.subPath, MAX_NAME_LEN);
        rstrcpy(sub_file.addr.hostAddr, location.c_str(), NAME_LEN);
        sub_file.specColl = _info.specColl;
        return rsSubStructFileStat(&_comm, &sub_file, _out);
    }

    // -------------------------------------------------------------------------
    // Factory & Handle Resolution
    // -------------------------------------------------------------------------

    auto driver_for_collection_info(const collection_info_variant& _info) noexcept -> const collection_driver&
    {
        if (std::holds_alternative<mounted_collection_info>(_info)) {
            return g_mounted_driver;
        }
        if (std::holds_alternative<archive_collection_info>(_info)) {
            return g_archive_driver;
        }
        return g_catalog_driver;
    }

    auto driver_for_legacy_spec_coll(const specColl_t* _spec_coll) noexcept -> const collection_driver&
    {
        if (!_spec_coll || _spec_coll->collClass == NO_SPEC_COLL) {
            return g_catalog_driver;
        }
        if (_spec_coll->collClass == STRUCT_FILE_COLL && getStructFileType(const_cast<specColl_t*>(_spec_coll)) >= 0) {
            return g_archive_driver;
        }
        if (_spec_coll->collClass == MOUNTED_COLL) {
            return g_mounted_driver;
        }
        return g_catalog_driver;
    }

    auto resolve_collection_handle(
        RsComm& _comm,
        std::string_view _logical_path,
        link_resolution_mode _mode) -> std::pair<int, collection_handle>
    {
        resolved_path resolved{};
        const int rc = resolve_collection_path(_comm, _logical_path, _mode, resolved);
        return {rc, collection_handle{std::move(resolved)}};
    }

} // namespace irods::experimental::collections
