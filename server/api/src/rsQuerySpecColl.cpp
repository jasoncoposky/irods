/*** Copyright (c), The Unregents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/* rsQuerySpecColl.c
 */

#include "irods/querySpecColl.h"
#include "irods/rcMisc.h"
#include "irods/fileOpendir.h"
#include "irods/fileReaddir.h"
#include "irods/fileClosedir.h"
#include "irods/objMetaOpr.hpp"
#include "irods/specColl.hpp"
#include "irods/dataObjClose.h"
#include "irods/subStructFileOpendir.h"
#include "irods/subStructFileReaddir.h"
#include "irods/subStructFileClosedir.h"
#include "irods/fileStat.h"
#include "irods/genQuery.h"
#include "irods/rsGlobalExtern.hpp"
#include "irods/rcGlobalExtern.h"
#include "irods/rsQuerySpecColl.hpp"
#include "irods/rsSubStructFileReaddir.hpp"
#include "irods/rsDataObjClose.hpp"
#include "irods/rsFileReaddir.hpp"
#include "irods/rsSubStructFileClosedir.hpp"
#include "irods/rsFileClosedir.hpp"
#include "irods/rsSubStructFileOpendir.hpp"
#include "irods/rsFileOpendir.hpp"
#include "irods/rsObjStat.hpp"

#include "irods/irods_resource_backport.hpp"
#include "irods/irods_resource_redirect.hpp"
#include "irods/irods_stacktrace.hpp"
#include "irods/irods_hierarchy_parser.hpp"
#include "irods/collection_driver.hpp"
#include "irods/collection_entry_stream.hpp"


int
rsQuerySpecColl( rsComm_t *rsComm, dataObjInp_t *dataObjInp,
                 genQueryOut_t **genQueryOut ) {

    int specCollInx;
    int status;
    int continueFlag;   /* continue query */
    int remoteFlag;
    rodsServerHost_t *rodsServerHost;
    remoteFlag = getAndConnRcatHost(rsComm, SECONDARY_RCAT, (const char*) dataObjInp->objPath, &rodsServerHost);

    if ( remoteFlag < 0 ) {
        return remoteFlag;
    }
    else if ( remoteFlag == REMOTE_HOST ) {
        status = rcQuerySpecColl( rodsServerHost->conn, dataObjInp,
                                  genQueryOut );
        return status;
    }

    // =-=-=-=-=-=-=-
    // working on the "home zone", determine if we need to redirect to a different
    // server in this zone for this operation.  if there is a RESC_HIER_STR_KW then
    // we know that the redirection decision has already been made
    char* hier_kw = getValByKey( &dataObjInp->condInput, RESC_HIER_STR_KW );
    if (!hier_kw) {
        try {
            auto result = irods::resolve_resource_hierarchy(irods::OPEN_OPERATION, rsComm, *dataObjInp);
            const auto& hier = std::get<std::string>(result);
            addKeyVal( &dataObjInp->condInput, RESC_HIER_STR_KW, hier.c_str() );
        }
        catch (const irods::exception& e ) { 
            irods::log(e);
            return e.code();
        }
    }

    if ( ( specCollInx = dataObjInp->openFlags ) <= 0 ) {
        specCollInx = openSpecColl( rsComm, dataObjInp, -1 );
        if ( specCollInx < 0 ) {
            rodsLog( LOG_NOTICE,
                     "rsQuerySpecColl: openSpecColl error for %s, status = %d",
                     dataObjInp->objPath, specCollInx );
            return specCollInx;
        }
        continueFlag = 0;
    }
    else {
        continueFlag = 1;
    }

    initOutForQuerySpecColl( genQueryOut );

    status = _rsQuerySpecColl( rsComm, specCollInx, dataObjInp,
                               *genQueryOut, continueFlag );

    if ( status < 0 ) {
        freeGenQueryOut( genQueryOut );
    }
    return status;
}

int
openSpecColl( rsComm_t *rsComm, dataObjInp_t *dataObjInp, int parentInx ) {
    int specCollInx;
    dataObjInfo_t *dataObjInfo = NULL;
    int status;
    int l3descInx;

    status = resolvePathInSpecColl( rsComm, dataObjInp->objPath,
                                    //READ_COLL_PERM, 0, &dataObjInfo);
                                    UNKNOWN_COLL_PERM, 0, &dataObjInfo );

    if ( status < 0 || NULL == dataObjInfo ) { // JMC cppcheck - nullptr
        rodsLog( LOG_NOTICE,
                 "rsQuerySpecColl: resolveSpecColl error for %s, status = %d",
                 dataObjInp->objPath, status );
        return status;
    }

    if ( dataObjInfo->specColl->collClass == LINKED_COLL ) {
        rodsLog( LOG_ERROR,
                 "rsQuerySpecColl: %s is a linked collection",
                 dataObjInp->objPath );
        return SYS_UNKNOWN_SPEC_COLL_CLASS;
    }

    char* resc_hier = getValByKey( &dataObjInp->condInput, RESC_HIER_STR_KW );
    if ( resc_hier ) {
        strncpy( dataObjInfo->rescHier, resc_hier, MAX_NAME_LEN );
        irods::error ret = resc_mgr.hier_to_leaf_id(resc_hier,dataObjInfo->rescId);
        if( !ret.ok() ) {
            irods::log(PASS(ret));
        }
    }

    l3descInx = l3Opendir( rsComm, dataObjInfo );

    if ( l3descInx < 0 ) {
        rodsLog( LOG_NOTICE,
                 "openSpecColl: specCollOpendir error for %s, status = %d",
                 dataObjInp->objPath, l3descInx );
        return l3descInx;
    }
    specCollInx = allocSpecCollDesc();
    if ( specCollInx < 0 ) {
        freeDataObjInfo( dataObjInfo );
        return specCollInx;
    }
    SpecCollDesc[specCollInx].l3descInx = l3descInx;
    SpecCollDesc[specCollInx].dataObjInfo = dataObjInfo;
    SpecCollDesc[specCollInx].parentInx = parentInx;

    return specCollInx;
}

int
initOutForQuerySpecColl( genQueryOut_t **genQueryOut ) {
    return irods::experimental::collections::init_spec_coll_query_out( genQueryOut );
}

int
_rsQuerySpecColl( rsComm_t *rsComm, int specCollInx,
                  dataObjInp_t *dataObjInp, genQueryOut_t *genQueryOut, int continueFlag ) {
    int status = 0;
    rodsDirent_t *rodsDirent = NULL;
    dataObjInfo_t *dataObjInfo;
    objType_t selObjType;
    char *tmpStr;
    dataObjInp_t newDataObjInp;
    int recurFlag;

    if ( SpecCollDesc[specCollInx].inuseFlag != FD_INUSE ) {
        rodsLog( LOG_ERROR,
                 "_rsQuerySpecColl: Input specCollInx %d not active", specCollInx );
        return BAD_INPUT_DESC_INDEX;
    }

    if ( ( tmpStr = getValByKey( &dataObjInp->condInput, SEL_OBJ_TYPE_KW ) ) !=
            NULL ) {
        if ( strcmp( tmpStr, "dataObj" ) == 0 ) {
            selObjType = DATA_OBJ_T;
        }
        else {
            selObjType = COLL_OBJ_T;
        }
    }
    else {
        selObjType = UNKNOWN_OBJ_T;
    }

    if ( getValByKey( &dataObjInp->condInput, RECURSIVE_OPR__KW ) != NULL ) {
        recurFlag = 1;
    }
    else {
        recurFlag = 0;
    }

    dataObjInfo = SpecCollDesc[specCollInx].dataObjInfo;

    while ( genQueryOut->rowCnt < MAX_SPEC_COLL_ROW ) {
        dataObjInfo_t myDataObjInfo;
        rodsDirent_t myRodsDirent;

        status = specCollReaddir( rsComm, specCollInx, &rodsDirent );

        if ( status < 0 ) {
            break;
        }

        myRodsDirent = *rodsDirent;
        free( rodsDirent );

        if ( strcmp( myRodsDirent.d_name, "." ) == 0 ||
                strcmp( myRodsDirent.d_name, ".." ) == 0 ) {
            continue;
        }

        myDataObjInfo = *dataObjInfo;

        snprintf( myDataObjInfo.subPath, MAX_NAME_LEN, "%s/%s",
                  dataObjInfo->subPath, myRodsDirent.d_name );
        snprintf( myDataObjInfo.filePath, MAX_NAME_LEN, "%s/%s",
                  dataObjInfo->filePath, myRodsDirent.d_name );

        rodsStat_t *fileStatOut = NULL;
        status = l3Stat( rsComm, &myDataObjInfo, &fileStatOut );
        if ( status < 0 ) {
            rodsLog( LOG_ERROR,
                     "_rsQuerySpecColl: l3Stat for %s error, status = %d",
                     myDataObjInfo.filePath, status );
            /* XXXXX need clean up */
            return status;
        }

        if ( ( fileStatOut->st_mode & S_IFREG ) != 0 ) { /* a file */
            if ( selObjType == COLL_OBJ_T ) {
                free( fileStatOut );
                continue;
            }
            irods::experimental::collections::collection_entry entry{};
            entry.obj_type = DATA_OBJ_T;
            entry.coll_name = dataObjInfo->subPath;
            entry.data_name = myRodsDirent.d_name;
            entry.create_time = std::to_string( fileStatOut->st_ctim );
            entry.modify_time = std::to_string( fileStatOut->st_mtim );
            entry.data_size = fileStatOut->st_size;

            free( fileStatOut );

            irods::experimental::collections::append_to_spec_coll_query_out( entry, *genQueryOut );
        }
        else {
            if ( selObjType != DATA_OBJ_T ) {
                irods::experimental::collections::collection_entry entry{};
                entry.obj_type = COLL_OBJ_T;
                entry.coll_name = myDataObjInfo.subPath;
                entry.create_time = std::to_string( fileStatOut->st_ctim );
                entry.modify_time = std::to_string( fileStatOut->st_mtim );
                entry.data_size = fileStatOut->st_size;

                irods::experimental::collections::append_to_spec_coll_query_out( entry, *genQueryOut );
            }

            free( fileStatOut );

            if ( recurFlag > 0 ) {
                /* need to drill down */
                int newSpecCollInx;
                newDataObjInp = *dataObjInp;
                rstrcpy( newDataObjInp.objPath, myDataObjInfo.subPath,
                         MAX_NAME_LEN );
                newSpecCollInx =
                    openSpecColl( rsComm, &newDataObjInp, specCollInx );
                if ( newSpecCollInx < 0 ) {
                    rodsLog( LOG_ERROR,
                             "_rsQuerySpecColl: openSpecColl err for %s, stat = %d",
                             newDataObjInp.objPath, newSpecCollInx );
                    status = newSpecCollInx;
                    break;
                }
                status = _rsQuerySpecColl( rsComm, newSpecCollInx,
                                           &newDataObjInp, genQueryOut, 0 );
                if ( status < 0 ) {
                    break;
                }
            }
        }
    } // while

    if ( status == EOF || status == CAT_NO_ROWS_FOUND ) {
        status = 0;
    }

    if ( genQueryOut->rowCnt < MAX_SPEC_COLL_ROW ) {
        int parentInx;
        /* get to the end or error */
        specCollClosedir( rsComm, specCollInx );
        parentInx = SpecCollDesc[specCollInx].parentInx;
        freeSpecCollDesc( specCollInx );
        if ( status >= 0 && recurFlag && continueFlag && parentInx > 0 ) {
            newDataObjInp = *dataObjInp;
            rstrcpy( newDataObjInp.objPath,
                     SpecCollDesc[parentInx].dataObjInfo->objPath, MAX_NAME_LEN );
            status = _rsQuerySpecColl( rsComm, parentInx,
                                       &newDataObjInp, genQueryOut, continueFlag );
        }
        else {
            /* no more */
            genQueryOut->continueInx = -1;
        }
        if ( status == EOF || status == CAT_NO_ROWS_FOUND ) {
            status = 0;
        }
    }
    else {
        /* more to come */
        if ( genQueryOut->continueInx < 0 ) {
            /* if one does not already exist */
            genQueryOut->continueInx = specCollInx;
        }
    }

    if ( status >= 0 && genQueryOut->rowCnt == 0 ) {
        status = CAT_NO_ROWS_FOUND;
    }

    return status;
}

int
specCollReaddir( rsComm_t *rsComm, int specCollInx, rodsDirent_t **rodsDirent ) {
    dataObjInfo_t *dataObjInfo = SpecCollDesc[specCollInx].dataObjInfo;
    if ( dataObjInfo == NULL || dataObjInfo->specColl == NULL ) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const auto& driver = irods::experimental::collections::driver_for_legacy_spec_coll( dataObjInfo->specColl );
    return driver.read_dir( *rsComm, *dataObjInfo, SpecCollDesc[specCollInx].l3descInx, rodsDirent );
}

int
specCollClosedir( rsComm_t *rsComm, int specCollInx ) {
    dataObjInfo_t *dataObjInfo = SpecCollDesc[specCollInx].dataObjInfo;
    if ( dataObjInfo == NULL || dataObjInfo->specColl == NULL ) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const auto& driver = irods::experimental::collections::driver_for_legacy_spec_coll( dataObjInfo->specColl );
    return driver.close_dir( *rsComm, *dataObjInfo, SpecCollDesc[specCollInx].l3descInx );
}

int
l3Opendir( rsComm_t *rsComm, dataObjInfo_t *dataObjInfo ) {
    if ( dataObjInfo == NULL ) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    if ( dataObjInfo->specColl == NULL ) {
        irods::experimental::collections::MountedCollectionDriver mounted_driver{};
        return mounted_driver.open_dir( *rsComm, *dataObjInfo );
    }

    const auto& driver = irods::experimental::collections::driver_for_legacy_spec_coll( dataObjInfo->specColl );
    return driver.open_dir( *rsComm, *dataObjInfo );
}
