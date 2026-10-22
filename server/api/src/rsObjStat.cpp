/*** Copyright (c), The Unregents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/* rsObjStat.c
 */

#include "irods/rsObjStat.hpp"
#include "irods/objStat.h"
#include "irods/rcMisc.h"
#include "irods/querySpecColl.h"
#include "irods/objMetaOpr.hpp"
#include "irods/collection.hpp"
#include "irods/rodsPath.h"
#include "irods/specColl.hpp"
#include "irods/resource.hpp"
#include "irods/rcGlobalExtern.h"
#include "irods/rsGlobalExtern.hpp"
#include "irods/dataObjClose.h"
#include "irods/miscServerFunct.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/filesystem/path.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/rs_genquery2.hpp"
#include "irods/irods_resource_backport.hpp"
#include "irods/collection_driver.hpp"
#include "irods/rsSubStructFileStat.hpp"
#include "irods/rsFileStat.hpp"

#include <charconv>
#include <cstdlib>
#include <cstring>
#include <string>

int rsObjStat(rsComm_t *rsComm,
              dataObjInp_t *dataObjInp,
              rodsObjStat_t **rodsObjStatOut )
{
    remove_trailing_path_separators(dataObjInp->objPath);

    int status;
    rodsServerHost_t *rodsServerHost = NULL;
    specCollCache_t *specCollCache = NULL;
    int linkCnt;

    linkCnt = resolveLinkedPath( rsComm, dataObjInp->objPath, &specCollCache,
                                 NULL );

    *rodsObjStatOut = NULL;
    status = getAndConnRcatHost(rsComm, SECONDARY_RCAT, (const char*) dataObjInp->objPath, &rodsServerHost);
    if ( status < 0 || NULL == rodsServerHost ) { // JMC cppcheck - nullptr
        return status;
    }
    if ( rodsServerHost->localFlag == LOCAL_HOST ) {
        std::string svc_role;
        irods::error ret = get_catalog_service_role(svc_role);
        if(!ret.ok()) {
            irods::log(PASS(ret));
            return ret.code();
        }

        if( irods::KW_CFG_SERVICE_ROLE_PROVIDER == svc_role ) {
            status = _rsObjStat( rsComm, dataObjInp, rodsObjStatOut );
        } else if( irods::KW_CFG_SERVICE_ROLE_CONSUMER == svc_role ) {
            status = SYS_NO_RCAT_SERVER_ERR;
        } else {
            rodsLog(
                LOG_ERROR,
                "role not supported [%s]",
                svc_role.c_str() );
            status = SYS_SERVICE_ROLE_NOT_SUPPORTED;
        }
    }
    else {
        if ( isLocalZone( dataObjInp->objPath ) ) {
            /* see if it is a sub path of a specColl cached locally. If it is,
             * it will save time resolving it */
            status = statPathInSpecColl( rsComm, dataObjInp->objPath, 1, rodsObjStatOut );

            if ( status >= 0 ) {
                /* the path is in a specColl */
                return status;
            }
            else if ( status != SYS_SPEC_COLL_NOT_IN_CACHE ) {
                /* path is in the path of specColl cache but does not exist */
                if ( linkCnt > 0 && *rodsObjStatOut != NULL ) {
                    /* a soft link - returns specColl */
                    if ( ( *rodsObjStatOut )->specColl == NULL ) {
                        replSpecColl( &specCollCache->specColl,
                                      &( *rodsObjStatOut )->specColl );
                    }
                    rstrcpy( ( *rodsObjStatOut )->specColl->objPath,
                             dataObjInp->objPath, MAX_NAME_LEN );
                }

                return status;
            }
            /* falls through if the path is not in a cached specColl */
        }
        /* not in cache, need to do a remote call */
        status = rcObjStat( rodsServerHost->conn, dataObjInp, rodsObjStatOut );
        if ( status >= 0 && *rodsObjStatOut != NULL && ( *rodsObjStatOut )->specColl != NULL ) {
            /* queue it in cache */
            queueSpecCollCacheWithObjStat( *rodsObjStatOut );
        }
    }

    if ( linkCnt > 0 && *rodsObjStatOut != NULL ) {
        /* fill in (*rodsObjStatOut)->specColl if it is linked */
        if ( ( *rodsObjStatOut )->specColl == NULL ) {
            replSpecColl( &specCollCache->specColl,
                          &( *rodsObjStatOut )->specColl );
        }
        rstrcpy( ( *rodsObjStatOut )->specColl->objPath, dataObjInp->objPath, MAX_NAME_LEN );

    }
    return status;
}

int
_rsObjStat( rsComm_t *rsComm, dataObjInp_t *dataObjInp,
            rodsObjStat_t **rodsObjStatOut ) {

    int status;
    char *tmpStr;
    specCollCache_t *specCollCache;

    /* do data first to catch data registered in spec Coll */
    if ( ( tmpStr = getValByKey( &dataObjInp->condInput, SEL_OBJ_TYPE_KW ) ) ==
            NULL || strcmp( tmpStr, "dataObj" ) == 0 ) {
        status = dataObjStat( rsComm, dataObjInp, rodsObjStatOut );
        if ( status >= 0 ) {
            return status;
        }
    }

    if ( tmpStr == NULL || strcmp( tmpStr, "collection" ) == 0 ) {
        status = collStat( rsComm, dataObjInp, rodsObjStatOut );
        /* specColl may already been obtained from collStat */
        if ( status >= 0 ) {
            if ( ( *rodsObjStatOut )->specColl == NULL ) {
                if ( getSpecCollCache( rsComm, dataObjInp->objPath, 0,
                                       &specCollCache ) >= 0 ) {
                    replSpecColl( &specCollCache->specColl,
                                  &( *rodsObjStatOut )->specColl );
                }
            }
            return status;
        }
    }

    /*  not normal dataObj or coll. now check specColl */
    /* XXXX need to check a rule if it supports spec collection */
    status = statPathInSpecColl( rsComm, dataObjInp->objPath, 0, rodsObjStatOut );
    /* use USER_FILE_DOES_NOT_EXIST instead of OBJ_PATH_DOES_NOT_EXIST
     * because icommand depends on it */
    if ( status < 0 ) {
        status = USER_FILE_DOES_NOT_EXIST;
    }
    return status;
}

int
dataObjStat( rsComm_t *rsComm, dataObjInp_t *dataObjInp,
             rodsObjStat_t **rodsObjStatOut ) {
    namespace fs = irods::filesystem;
    namespace gq2 = irods::experimental::genquery2;

    if (!rsComm || !dataObjInp || !rodsObjStatOut) {
        return USER__NULL_INPUT_ERR;
    }

    const fs::path p{dataObjInp->objPath};
    const auto coll_view = p.parent_path_view();
    const auto data_view = p.object_name_view();
    if (coll_view.empty() || data_view.empty()) {
        return OBJ_PATH_DOES_NOT_EXIST;
    }

    using gq2::builder::col;
    const auto q = gq2::builder::select({"DATA_ID",
                                         "DATA_SIZE",
                                         "DATA_MODE",
                                         "DATA_REPL_STATUS",
                                         "DATA_CHECKSUM",
                                         "DATA_OWNER_NAME",
                                         "DATA_OWNER_ZONE",
                                         "DATA_CREATE_TIME",
                                         "DATA_MODIFY_TIME",
                                         "DATA_RESC_HIER"})
                       .where(col("COLL_NAME") == coll_view && col("DATA_NAME") == data_view)
                       .limit(MAX_SQL_ROWS)
                       .build();

    nlohmann::json rows;
    if (const int ec = gq2::try_execute(*rsComm, q, rows); ec < 0) {
        return ec;
    }

    if (rows.empty()) {
        return CAT_NO_ROWS_FOUND;
    }

    *rodsObjStatOut = static_cast<rodsObjStat_t*>(std::malloc(sizeof(rodsObjStat_t)));
    std::memset(*rodsObjStatOut, 0, sizeof(rodsObjStat_t));
    (*rodsObjStatOut)->objType = DATA_OBJ_T;

    const auto populate_stat = [](rodsObjStat_t& _stat, const nlohmann::json& _row, const bool _include_mode) {
        const auto& data_id = _row[0].get_ref<const std::string&>();
        const auto& data_size = _row[1].get_ref<const std::string&>();
        const auto& data_mode = _row[2].get_ref<const std::string&>();
        const auto& chksum = _row[4].get_ref<const std::string&>();
        const auto& owner_name = _row[5].get_ref<const std::string&>();
        const auto& owner_zone = _row[6].get_ref<const std::string&>();
        const auto& create_time = _row[7].get_ref<const std::string&>();
        const auto& modify_time = _row[8].get_ref<const std::string&>();
        const auto& resc_hier = _row[9].get_ref<const std::string&>();

        rstrcpy(_stat.dataId, data_id.c_str(), NAME_LEN);
        std::from_chars(data_size.data(), data_size.data() + data_size.size(), _stat.objSize);
        if (_include_mode) {
            std::from_chars(data_mode.data(), data_mode.data() + data_mode.size(), _stat.dataMode);
        }
        rstrcpy(_stat.chksum, chksum.c_str(), CHKSUM_LEN);
        rstrcpy(_stat.ownerName, owner_name.c_str(), NAME_LEN);
        rstrcpy(_stat.ownerZone, owner_zone.c_str(), NAME_LEN);
        rstrcpy(_stat.createTime, create_time.c_str(), TIME_LEN);
        rstrcpy(_stat.modifyTime, modify_time.c_str(), TIME_LEN);
        rstrcpy(_stat.rescHier, resc_hier.c_str(), MAX_NAME_LEN);
    };

    for (const auto& row : rows) {
        const auto& repl_status_str = row[3].get_ref<const std::string&>();
        int repl_status = 0;
        std::from_chars(repl_status_str.data(), repl_status_str.data() + repl_status_str.size(), repl_status);
        if (repl_status == 1) {
            populate_stat(**rodsObjStatOut, row, true);
            break;
        }
    }

    if (std::strlen((*rodsObjStatOut)->dataId) == 0) {
        populate_stat(**rodsObjStatOut, rows[0], false);
    }

    return static_cast<int>(DATA_OBJ_T);
}

int l3Stat(
    rsComm_t *rsComm,
    dataObjInfo_t *dataObjInfo,
    rodsStat_t **myStat)
{
    if (!rsComm || !dataObjInfo || !myStat) {
        return USER__NULL_INPUT_ERR;
    }
    const auto& driver = irods::experimental::collections::driver_for_legacy_spec_coll(dataObjInfo->specColl);
    return driver.stat_entry(*rsComm, *dataObjInfo, myStat);
} // l3Stat

