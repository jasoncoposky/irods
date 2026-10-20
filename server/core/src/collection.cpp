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
#include "irods/rsGenQuery.hpp"
#include "irods/rsSpecificQuery.hpp"
#include "irods/rsOpenCollection.hpp"
#include "irods/rsReadCollection.hpp"
#include "irods/rsCloseCollection.hpp"
#include "irods/rsObjStat.hpp"
#include "irods/rsCollCreate.hpp"
#include "irods/rsDataObjRename.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/rs_genquery2.hpp"

#include "irods/irods_virtual_path.hpp"


/* checkCollAccessPerm - Check whether the user is allowed to perform
 * operation specified by the accessPerm string on the collection.
 * Various accessPerm are defined in  icatDefines.h. (e.g.,
 * ACCESS_DELETE_OBJECT for create and delete, ACCESS_READ_OBJECT for read).
 * A return of >= 0 means allow and a negative value means disallow.
 */

int
checkCollAccessPerm( rsComm_t *rsComm, char *collection, char *accessPerm ) {
    char accStr[LONG_NAME_LEN];
    char condStr[MAX_NAME_LEN];
    genQueryInp_t genQueryInp;
    genQueryOut_t *genQueryOut = NULL;
    int status;

    if ( collection == NULL || accessPerm == NULL ) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    memset( &genQueryInp, 0, sizeof( genQueryInp ) );

    snprintf( accStr, LONG_NAME_LEN, "%s", rsComm->clientUser.userName );
    addKeyVal( &genQueryInp.condInput, USER_NAME_CLIENT_KW, accStr );

    snprintf( accStr, LONG_NAME_LEN, "%s", rsComm->clientUser.rodsZone );
    addKeyVal( &genQueryInp.condInput, RODS_ZONE_CLIENT_KW, accStr );

    snprintf( accStr, LONG_NAME_LEN, "%s", accessPerm );
    addKeyVal( &genQueryInp.condInput, ACCESS_PERMISSION_KW, accStr );

    snprintf( condStr, MAX_NAME_LEN, "='%s'", collection );
    addInxVal( &genQueryInp.sqlCondInp, COL_COLL_NAME, condStr );

    addInxIval( &genQueryInp.selectInp, COL_COLL_ID, 1 );

    genQueryInp.maxRows = MAX_SQL_ROWS;

    status = rsGenQuery( rsComm, &genQueryInp, &genQueryOut );

    clearGenQueryInp( &genQueryInp );
    freeGenQueryOut( &genQueryOut );

    return status;
}

/*
   Try a specific-query for the a particular Data-object in collection
   query first, for performance improvements, before going back to a
   general-query.  This is called from rsQueryDataObjInCollReCur below.
   See the svn commit log (r5682) for the specific query definition.
 */
int trySpecificQueryDataObjInCollReCur(
    rsComm_t *rsComm,
    char *collection,
    genQueryOut_t **genQueryOut ) {

    int status = 0;
    char collNamePercent[MAX_NAME_LEN + 2];

    specificQueryInp_t specificQueryInp;

    rstrcpy( collNamePercent, collection, MAX_NAME_LEN );
    rstrcat( collNamePercent, "/%", MAX_NAME_LEN );

    memset( &specificQueryInp, 0, sizeof( specificQueryInp_t ) );

    specificQueryInp.maxRows = MAX_SQL_ROWS;
    specificQueryInp.continueInx = 0;

    specificQueryInp.sql = "DataObjInCollReCur";
    specificQueryInp.args[0] = collection;
    specificQueryInp.args[1] = collNamePercent;

    status = rsSpecificQuery( rsComm, &specificQueryInp, genQueryOut );

    if ( status == 0 ) {
        /*
           Set the attriInx values so the server-side code can locate
           the fields (avoid a UNMATCHED_KEY_OR_INDEX error) the way
           general-query is handled.  The specific-query can't set these
           because the columns being returned are not known (unlike for
           general-query).  So this code assumes the DataObjInCollReCur
           is defined correctly and is returning the following columns.
           The result->attriCnt == 6 test is a sanity check.
         */
        genQueryOut_t *result;
        result = *genQueryOut;
        if ( result->attriCnt == 7 ) {

            result->sqlResult[0].attriInx = COL_D_DATA_ID;
            result->sqlResult[1].attriInx = COL_COLL_NAME;
            result->sqlResult[2].attriInx = COL_DATA_NAME;
            result->sqlResult[3].attriInx = COL_DATA_REPL_NUM;
            result->sqlResult[4].attriInx = COL_D_RESC_NAME;
            result->sqlResult[5].attriInx = COL_D_DATA_PATH;
            result->sqlResult[6].attriInx = COL_D_RESC_ID;
        }
    }
    return status;
}



int
rsQueryDataObjInCollReCur( rsComm_t *rsComm, char *collection,
                           genQueryInp_t *genQueryInp, genQueryOut_t **genQueryOut, char *accessPerm,
                           int singleFlag ) {
    char collQCond[MAX_NAME_LEN * 2];
    int status = 0;
    char accStr[LONG_NAME_LEN];

    if ( genQueryInp == NULL ||
            collection  == NULL ||
            genQueryOut == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    memset( genQueryInp, 0, sizeof( genQueryInp_t ) );

    genAllInCollQCond( collection, collQCond );

    addInxVal( &genQueryInp->sqlCondInp, COL_COLL_NAME, collQCond );

    addInxIval( &genQueryInp->selectInp, COL_D_DATA_ID, 1 );
    addInxIval( &genQueryInp->selectInp, COL_COLL_NAME, 1 );
    addInxIval( &genQueryInp->selectInp, COL_DATA_NAME, 1 );

    if ( singleFlag == 0 ) {
        addInxIval( &genQueryInp->selectInp, COL_DATA_REPL_NUM, 1 );
        addInxIval( &genQueryInp->selectInp, COL_D_RESC_NAME, 1 );
        addInxIval( &genQueryInp->selectInp, COL_D_DATA_PATH, 1 );
    }

    addInxIval( &genQueryInp->selectInp, COL_D_RESC_ID, 1 );

    if ( accessPerm != NULL ) {
        snprintf( accStr, LONG_NAME_LEN, "%s", rsComm->clientUser.userName );
        addKeyVal( &genQueryInp->condInput, USER_NAME_CLIENT_KW, accStr );

        snprintf( accStr, LONG_NAME_LEN, "%s", rsComm->clientUser.rodsZone );
        addKeyVal( &genQueryInp->condInput, RODS_ZONE_CLIENT_KW, accStr );

        snprintf( accStr, LONG_NAME_LEN, "%s", accessPerm );
        addKeyVal( &genQueryInp->condInput, ACCESS_PERMISSION_KW, accStr );
        /* have to set it to 1 because it only check the first one */
        genQueryInp->maxRows = 1;
        status = rsGenQuery( rsComm, genQueryInp, genQueryOut );
        rmKeyVal( &genQueryInp->condInput, USER_NAME_CLIENT_KW );
        rmKeyVal( &genQueryInp->condInput, RODS_ZONE_CLIENT_KW );
        rmKeyVal( &genQueryInp->condInput, ACCESS_PERMISSION_KW );
    }
    else {
        genQueryInp->maxRows = MAX_SQL_ROWS;
        status = trySpecificQueryDataObjInCollReCur(
                     rsComm,
                     collection,
                     genQueryOut );
        if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
            rodsLog(
                LOG_NOTICE,
                "Note: DataObjInCollReCur specific-Query failed (not defined?), running standard query, status=%d",
                status );
            /* remove the level 0 error msg added by the specific-query failure */
            freeRErrorContent( &rsComm->rError );

            namespace gq2 = irods::experimental::genquery2;
            using gq2::builder::col;

            auto cond = (std::strcmp(collection, "/") == 0)
                ? col("COLL_NAME").like("/%")
                : (col("COLL_NAME") == collection || col("COLL_NAME").like(fmt::format("{}/%", collection))).group();

            std::vector<std::string> cols{"DATA_ID", "COLL_NAME", "DATA_NAME"};
            if ( singleFlag == 0 ) {
                cols.emplace_back("DATA_REPL_NUM");
                cols.emplace_back("RESC_NAME");
                cols.emplace_back("DATA_PATH");
            }
            cols.emplace_back("DATA_RESC_ID");

            const int offset = genQueryInp->rowOffset > 0 ? genQueryInp->rowOffset : 0;
            const auto q = gq2::builder::select(std::move(cols))
                               .where(std::move(cond))
                               .order_by("COLL_NAME")
                               .order_by("DATA_NAME")
                               .offset(offset)
                               .limit(MAX_SQL_ROWS)
                               .build();

            nlohmann::json rows;
            status = gq2::try_execute( *rsComm, q, rows );
            if ( status >= 0 ) {
                const int next_inx = (rows.size() == static_cast<std::size_t>(MAX_SQL_ROWS)) ? (offset + MAX_SQL_ROWS) : 0;
                if ( singleFlag == 0 ) {
                    status = gq2::rows_to_genquery_out(
                        rows,
                        {COL_D_DATA_ID, COL_COLL_NAME, COL_DATA_NAME, COL_DATA_REPL_NUM, COL_D_RESC_NAME, COL_D_DATA_PATH, COL_D_RESC_ID},
                        genQueryOut,
                        next_inx );
                }
                else {
                    status = gq2::rows_to_genquery_out(
                        rows,
                        {COL_D_DATA_ID, COL_COLL_NAME, COL_DATA_NAME, COL_D_RESC_ID},
                        genQueryOut,
                        next_inx );
                }
            }
        }

    }

    return status;

}

int
rsQueryCollInColl( rsComm_t *rsComm, char *collection,
                   genQueryInp_t *genQueryInp, genQueryOut_t **genQueryOut ) {
    if ( rsComm == NULL || collection == NULL || genQueryOut == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    if ( genQueryInp != NULL ) {
        memset( genQueryInp, 0, sizeof( genQueryInp_t ) );
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    const auto q = gq2::builder::select({"COLL_NAME"})
                       .where(col("COLL_PARENT_NAME") == collection)
                       .order_by("COLL_NAME")
                       .limit(MAX_SQL_ROWS)
                       .build();

    nlohmann::json rows;
    int status = gq2::try_execute( *rsComm, q, rows );
    if ( status >= 0 ) {
        const int next_inx = (rows.size() == static_cast<std::size_t>(MAX_SQL_ROWS)) ? MAX_SQL_ROWS : 0;
        status = gq2::rows_to_genquery_out( rows, {COL_COLL_NAME}, genQueryOut, next_inx );
    }

    return status;
}

int
isCollEmpty( rsComm_t *rsComm, char *collection ) {
    collInp_t openCollInp;
    collEnt_t *collEnt;
    int handleInx;
    int entCnt = 0;

    if ( rsComm == NULL || collection == NULL ) {
        rodsLog( LOG_ERROR,
                 "isCollEmpty: Input rsComm or collection is NULL" );
        return True;
    }

    memset( &openCollInp, 0, sizeof( openCollInp ) );
    rstrcpy( openCollInp.collName, collection, MAX_NAME_LEN );
    /* cannot query recur because collection is sorted in wrong order */
    openCollInp.flags = 0;
    handleInx = rsOpenCollection( rsComm, &openCollInp );
    if ( handleInx < 0 ) {
        rodsLog( LOG_ERROR,
                 "isCollEmpty: rsOpenCollection of %s error. status = %d",
                 openCollInp.collName, handleInx );
        return True;
    }

    while ( rsReadCollection( rsComm, &handleInx, &collEnt ) >= 0 ) {
        entCnt++;
        free( collEnt );    /* just free collEnt but not content */
    }

    rsCloseCollection( rsComm, &handleInx );

    if ( entCnt > 0 ) {
        return False;
    }
    else {
        return True;
    }
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
