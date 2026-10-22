/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/* collection.c - collection operation */

#include "irods/rcMisc.h"
#include "irods/collection.hpp"
#include "irods/specColl.hpp"
#include "irods/genQuery.h"
#include "irods/specificQuery.h"
#include "irods/dataObjRename.h"
#include "irods/readCollection.h"
#include "irods/closeCollection.h"
#include "irods/collCreate.h"
#include "irods/objMetaOpr.hpp"
#include "irods/icatHighLevelRoutines.hpp"
#include "irods/rsSpecificQuery.hpp"
#include "irods/rsOpenCollection.hpp"
#include "irods/rsReadCollection.hpp"
#include "irods/rsCloseCollection.hpp"
#include "irods/rsObjStat.hpp"
#include "irods/rsCollCreate.hpp"
#include "irods/rsDataObjRename.hpp"
#define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#include "irods/filesystem.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/rs_genquery2.hpp"
#include "irods/scoped_privileged_client.hpp"

#include "irods/irods_virtual_path.hpp"

#include <charconv>
#include <system_error>


/* checkCollAccessPerm - Check whether the user is allowed to perform
 * operation specified by the accessPerm string on the collection.
 * Various accessPerm are defined in  icatDefines.h. (e.g.,
 * ACCESS_DELETE_OBJECT for create and delete, ACCESS_READ_OBJECT for read).
 * A return of >= 0 means allow and a negative value means disallow.
 */

int
checkCollAccessPerm( rsComm_t *rsComm, char *collection, char *accessPerm ) {
    if ( rsComm == NULL || collection == NULL || accessPerm == NULL ) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    return chlCheckAndGetObjectID( rsComm, "-c", collection, accessPerm );
}

int
isCollEmpty( rsComm_t *rsComm, char *collection ) {
    if ( rsComm == NULL || collection == NULL ) {
        rodsLog( LOG_ERROR,
                 "isCollEmpty: Input rsComm or collection is NULL" );
        return True;
    }

    std::error_code ec;
    const bool empty = irods::filesystem::server::is_empty(*rsComm, collection, ec);
    if ( ec ) {
        rodsLog( LOG_ERROR,
                 "isCollEmpty: is_empty of %s error. status = %d",
                 collection, ec.value() );
        return True;
    }

    return empty ? True : False;
}

/* collStat - query metaData of a collection. */

int
collStat( rsComm_t *rsComm, const dataObjInp_t *dataObjInp,
          rodsObjStat_t **rodsObjStatOut ) {
    if (!rsComm || !dataObjInp || !rodsObjStatOut) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    const auto q = gq2::builder::select({"COLL_ID",
                                         "COLL_NAME",
                                         "COLL_OWNER_NAME",
                                         "COLL_OWNER_ZONE",
                                         "COLL_CREATE_TIME",
                                         "COLL_MODIFY_TIME",
                                         "COLL_TYPE",
                                         "COLL_INFO1",
                                         "COLL_INFO2"})
                       .where(col("COLL_NAME") == dataObjInp->objPath)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    int status = gq2::try_execute(*rsComm, q, rows);
    if ( status < 0 ) {
        return status;
    }
    if ( rows.empty() ) {
        return CAT_NO_ROWS_FOUND;
    }

    const auto& row = rows.front();
    const auto& data_id_str = row.at(0).get_ref<const std::string&>();
    const auto& coll_name_str = row.at(1).get_ref<const std::string&>();
    const auto& owner_name_str = row.at(2).get_ref<const std::string&>();
    const auto& owner_zone_str = row.at(3).get_ref<const std::string&>();
    const auto& create_time_str = row.at(4).get_ref<const std::string&>();
    const auto& modify_time_str = row.at(5).get_ref<const std::string&>();
    const auto& coll_type_str = row.at(6).get_ref<const std::string&>();
    const auto& coll_info1_str = row.at(7).get_ref<const std::string&>();
    const auto& coll_info2_str = row.at(8).get_ref<const std::string&>();

    *rodsObjStatOut = ( rodsObjStat_t * ) malloc( sizeof( rodsObjStat_t ) );
    memset( *rodsObjStatOut, 0, sizeof( rodsObjStat_t ) );
    ( *rodsObjStatOut )->objType = COLL_OBJ_T;
    status = ( int )COLL_OBJ_T;

    rstrcpy( ( *rodsObjStatOut )->dataId, data_id_str.c_str(), NAME_LEN );
    rstrcpy( ( *rodsObjStatOut )->ownerName, owner_name_str.c_str(), NAME_LEN );
    rstrcpy( ( *rodsObjStatOut )->ownerZone, owner_zone_str.c_str(), NAME_LEN );
    rstrcpy( ( *rodsObjStatOut )->createTime, create_time_str.c_str(), TIME_LEN );
    rstrcpy( ( *rodsObjStatOut )->modifyTime, modify_time_str.c_str(), TIME_LEN );

    if ( !coll_type_str.empty() ) {
        if ( specCollCache_t *specCollCache = matchSpecCollCache( dataObjInp->objPath ); specCollCache != NULL ) {
            replSpecColl( &specCollCache->specColl, &( *rodsObjStatOut )->specColl );
        }
        else {
            char tmp_coll_type[NAME_LEN]{};
            char tmp_coll_name[MAX_NAME_LEN]{};
            char tmp_coll_info1[MAX_NAME_LEN]{};
            char tmp_coll_info2[MAX_NAME_LEN]{};
            rstrcpy(tmp_coll_type, coll_type_str.c_str(), sizeof(tmp_coll_type));
            rstrcpy(tmp_coll_name, coll_name_str.c_str(), sizeof(tmp_coll_name));
            rstrcpy(tmp_coll_info1, coll_info1_str.c_str(), sizeof(tmp_coll_info1));
            rstrcpy(tmp_coll_info2, coll_info2_str.c_str(), sizeof(tmp_coll_info2));

            specColl_t tmp_spec_coll{};
            int spec_status = resolveSpecCollType(tmp_coll_type, tmp_coll_name, tmp_coll_info1, tmp_coll_info2, &tmp_spec_coll);
            if ( spec_status < 0 ) {
                freeRodsObjStat( *rodsObjStatOut );
                *rodsObjStatOut = NULL;
                return spec_status;
            }

            if ( tmp_spec_coll.collClass == STRUCT_FILE_COLL &&
                 tmp_spec_coll.type      == TAR_STRUCT_FILE_T ) {
                spec_status = getPhyPath( rsComm, tmp_spec_coll.objPath, tmp_spec_coll.resource, tmp_spec_coll.phyPath, tmp_spec_coll.rescHier );
                if ( spec_status < 0 ) {
                    rodsLog( LOG_ERROR, "collStat - getPhyPath failed for [%s] on resource [%s] with cache dir [%s] and collection [%s]",
                             tmp_spec_coll.objPath, tmp_spec_coll.resource, tmp_spec_coll.cacheDir, tmp_spec_coll.collection );
                    freeRodsObjStat( *rodsObjStatOut );
                    *rodsObjStatOut = NULL;
                    return spec_status;
                }
            }

            replSpecColl( &tmp_spec_coll, &( *rodsObjStatOut )->specColl );
            queueSpecCollCacheWithObjStat( *rodsObjStatOut );
        }
    }

    return status;
}

/* collStatAllKinds - Stat of a collection. The path can be part of a
 * specColl or not.
 */

int
collStatAllKinds( rsComm_t *rsComm, dataObjInp_t *dataObjInp,
                  rodsObjStat_t **rodsObjStatOut ) {
    int status;

    *rodsObjStatOut = NULL;
    addKeyVal( &dataObjInp->condInput, SEL_OBJ_TYPE_KW, "collection" );
    status = _rsObjStat( rsComm, dataObjInp, rodsObjStatOut );
    rmKeyVal( &dataObjInp->condInput, SEL_OBJ_TYPE_KW );
    if ( status >= 0 ) {
        if ( ( *rodsObjStatOut )->objType != COLL_OBJ_T ) {
            status = OBJ_PATH_DOES_NOT_EXIST;
        }
    }
    if ( status < 0 && *rodsObjStatOut != NULL ) {
        freeRodsObjStat( *rodsObjStatOut );
        *rodsObjStatOut = NULL;
    }
    return status;
}

/* mk the collection recursively */

int
rsMkCollR( rsComm_t *rsComm, const char *startColl, const char *destColl ) {
    int status;
    int startLen;
    int pathLen, tmpLen;
    char tmpPath[MAX_NAME_LEN];

    startLen = strlen( startColl );
    pathLen = strlen( destColl );

    // sanitize destColl
    std::string dest_coll( destColl );
    const std::string log_sep = irods::get_virtual_path_separator();
    size_t pos = dest_coll.find_last_of( log_sep );
    if ( ( dest_coll.size() - 1 ) == pos ) {
        dest_coll.erase( pos, std::string::npos );
    }

    rstrcpy( tmpPath, dest_coll.c_str(), MAX_NAME_LEN );

    tmpLen = pathLen;

    while ( tmpLen > startLen ) {
        if ( isCollAllKinds( rsComm, tmpPath, NULL ) >= 0 ) {
            break;
        }

        /* Go backward */

        while ( tmpLen && tmpPath[tmpLen] != '/' ) {
            tmpLen --;
        }
        tmpPath[tmpLen] = '\0';
    }

    /* Now we go forward and make the required coll */
    while ( tmpLen < pathLen ) {
        collInp_t collCreateInp;

        /* Put back the '/' */
        tmpPath[tmpLen] = '/';
        memset( &collCreateInp, 0, sizeof( collCreateInp ) );
        rstrcpy( collCreateInp.collName, tmpPath, MAX_NAME_LEN );
        status = rsCollCreate( rsComm, &collCreateInp );

        if ( status == CAT_NAME_EXISTS_AS_DATAOBJ && isTrashPath( tmpPath ) ) {
            /* name conflict with a data object in the trash collection */
            dataObjCopyInp_t dataObjRenameInp;
            memset( &dataObjRenameInp, 0, sizeof( dataObjRenameInp ) );

            dataObjRenameInp.srcDataObjInp.oprType =
                dataObjRenameInp.destDataObjInp.oprType = RENAME_DATA_OBJ;
            rstrcpy( dataObjRenameInp.srcDataObjInp.objPath, tmpPath,
                     MAX_NAME_LEN );
            rstrcpy( dataObjRenameInp.destDataObjInp.objPath, tmpPath,
                     MAX_NAME_LEN );
            appendRandomToPath( dataObjRenameInp.destDataObjInp.objPath );

            status = rsDataObjRename( rsComm, &dataObjRenameInp );
            if ( status >= 0 ) {
                status = rsCollCreate( rsComm, &collCreateInp );
            }
        }
        /* something may be added by rsCollCreate */
        clearKeyVal( &collCreateInp.condInput );
        if ( status < 0 ) {
            // =-=-=-=-=-=-=-
            // JMC - backport 4819
            if ( status == CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME ) {
                rodsLog( LOG_DEBUG,
                         "rsMkCollR: rsCollCreate - coll %s already exist.stat = %d",
                         tmpPath, status );
                status = 0;
            }
            else {
                rodsLog( LOG_ERROR,
                         "rsMkCollR: rsCollCreate failed for %s, status =%d",
                         tmpPath, status );
            }
            // =-=-=-=-=-=-=-
            return status;
        }
        while ( tmpLen && tmpPath[tmpLen] != '\0' ) {
            tmpLen ++;
        }
    }
    return 0;
}
