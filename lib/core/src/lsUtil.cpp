#include "irods/lsUtil.h"

#include "irods/irods_at_scope_exit.hpp"
#include "irods/rcMisc.h"
#include "irods/rodsPath.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/miscUtil.h"
#include "irods/rcGlobalExtern.h"
#include "irods/filesystem.hpp"
#include "irods/genQuery.h"
#include "irods/genquery2_builder.hpp"

#include <fmt/format.h>

#include <charconv>
#include <cstring>

namespace
{
    template <typename T>
    auto parse_int(const char* _str, T _default = 0) noexcept -> T
    {
        if (!_str) {
            return _default;
        }
        while (*_str == ' ' || *_str == '\t') {
            ++_str;
        }
        T val = _default;
        std::from_chars(_str, _str + std::strlen(_str), val);
        return val;
    }
} // anonymous namespace

char zoneHint[MAX_NAME_LEN];

int lsUtil(rcComm_t *conn,
           rodsEnv *myRodsEnv,
           rodsArguments_t *myRodsArgs,
           rodsPathInp_t *rodsPathInp)
{
    if (!rodsPathInp) {
        return USER__NULL_INPUT_ERR;
    }

    if ( myRodsArgs->ticket == True ) {
        if ( myRodsArgs->ticketString == NULL ) {
            rodsLog( LOG_ERROR, "initCondForPut: NULL ticketString error" );
            return USER__NULL_INPUT_ERR;
        }

        setSessionTicket( conn, myRodsArgs->ticketString );
    }

    int savedStatus = 0;

    for ( int i = 0; i < rodsPathInp->numSrc; i++ ) {
        int status = 0;
        rstrcpy( zoneHint, rodsPathInp->srcPath[i].outPath, MAX_NAME_LEN );

        namespace fs = irods::filesystem;
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto* src_path = rodsPathInp->srcPath[i].outPath;
        const auto object_status = fs::client::status(*conn, src_path, fs::status_options::basic);

        if (!fs::client::exists(object_status)) {
            rodsLog(LOG_ERROR, "lsUtil: srcPath %s does not exist or user lacks access permission", src_path);
            savedStatus = USER_INPUT_PATH_ERR;
            continue;
        }

        // Need to populate dataId in srcPath as it is needed for getting permissions info
        const fs::path src_p{rodsPathInp->srcPath[i].outPath};
        const auto q = gq2::builder::select({"DATA_ID"})
                           .where(col("DATA_NAME") == src_p.object_name_view() &&
                                  col("COLL_NAME") == src_p.parent_path_view())
                           .limit(1)
                           .build();
        if (const auto rows = gq2::try_execute(*conn, q); rows && !rows->empty()) {
            const auto& id = rows->front().at(0).get_ref<const std::string&>();
            if (!id.empty()) {
                rstrcpy(rodsPathInp->srcPath[i].dataId, id.c_str(), sizeof(rodsPathInp->srcPath[i].dataId));
            }
        }

        if (fs::client::is_data_object(object_status)) {
            status = lsDataObjUtil(conn, &rodsPathInp->srcPath[i], myRodsArgs);
        }
        else if (fs::client::is_collection(object_status)) {
            status = lsCollUtil( conn, &rodsPathInp->srcPath[i], myRodsEnv, myRodsArgs );
        }
        else {
            /* should not be here */
            rodsLog(LOG_ERROR, "lsUtil: invalid ls objType %d for %s", rodsPathInp->srcPath[i].objType, src_path);
            return USER_INPUT_PATH_ERR;
        }

        /* XXXX may need to return a global status */
        if ( status < 0 && status != CAT_NO_ROWS_FOUND && status != SYS_SPEC_COLL_OBJ_NOT_EXIST ) {
            rodsLogError(LOG_ERROR, status, "lsUtil: ls error for %s, status = %d", src_path, status);
            savedStatus = status;
        }
    }

    return savedStatus;
}

int initCondForLs(GenQueryInp* genQueryInp)
{
    return 0;
}

int lsDataObjUtil(rcComm_t *conn, rodsPath_t *srcPath, rodsArguments_t *rodsArgs)
{
    int status = 0;

    if (rodsArgs->longOption == True) {
        if (srcPath->rodsObjStat && srcPath->rodsObjStat->specColl) {
            if (srcPath->rodsObjStat->specColl->collClass == LINKED_COLL) {
                lsDataObjUtilLong(conn, srcPath->rodsObjStat->specColl->objPath, rodsArgs, nullptr);
            }
            else {
                lsSpecDataObjUtilLong(srcPath, rodsArgs);
            }
        }
        else {
            lsDataObjUtilLong(conn, srcPath->outPath, rodsArgs, nullptr);
        }
    }
    else if ( rodsArgs->bundle == True ) {
        lsSubfilesInBundle( conn, srcPath->outPath );
    }
    else {
        printLsStrShort( srcPath->outPath );
        if ( rodsArgs->accessControl == True ) {
            printDataAcl( conn, srcPath->dataId );
        }
    }

    if ( srcPath == NULL ) {
        rodsLog( LOG_ERROR, "lsDataObjUtil: NULL srcPath input" );
        return USER__NULL_INPUT_ERR;
    }

    return status;
}

int printLsStrShort( char *srcPath ) {
    printf( "  %s\n", srcPath );
    return 0;
}

int lsDataObjUtilLong(RcComm* conn, char* srcPath, RodsArguments* rodsArgs, GenQueryInp*)
{
    if (!conn || !srcPath || !rodsArgs) {
        return USER__NULL_INPUT_ERR;
    }

    genQueryOut_t* genQueryOut{};
    irods::at_scope_exit cleanup{[&genQueryOut] { freeGenQueryOut(&genQueryOut); }};

    namespace fs = irods::filesystem;
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    const fs::path p{srcPath};
    if (p.parent_path_view().empty() && p.object_name_view().empty()) {
        return USER_INPUT_PATH_ERR;
    }

    const int queryFlags = setQueryFlag(rodsArgs);

    std::vector<std::string> proj = {"COLL_NAME",
                                     "DATA_NAME",
                                     "DATA_ID",
                                     "DATA_MODE",
                                     "DATA_SIZE",
                                     "DATA_MODIFY_TIME",
                                     "DATA_CREATE_TIME",
                                     "RESC_NAME",
                                     "DATA_RESC_HIER",
                                     "DATA_OWNER_NAME",
                                     "DATA_REPL_NUM",
                                     "DATA_REPL_STATUS"};
    std::vector<int> col_indices = {COL_COLL_NAME,
                                    COL_DATA_NAME,
                                    COL_D_DATA_ID,
                                    COL_DATA_MODE,
                                    COL_DATA_SIZE,
                                    COL_D_MODIFY_TIME,
                                    COL_D_CREATE_TIME,
                                    COL_D_RESC_NAME,
                                    COL_D_RESC_HIER,
                                    COL_D_OWNER_NAME,
                                    COL_DATA_REPL_NUM,
                                    COL_D_REPL_STATUS};

    if ((queryFlags & VERY_LONG_METADATA_FG) != 0) {
        proj.insert(proj.end(), {"DATA_PATH", "DATA_CHECKSUM", "DATA_TYPE_NAME"});
        col_indices.insert(col_indices.end(), {COL_D_DATA_PATH, COL_D_DATA_CHECKSUM, COL_DATA_TYPE_NAME});
    }

    const auto q = gq2::builder::select(std::move(proj))
                       .where(col("COLL_NAME") == p.parent_path_view() &&
                              col("DATA_NAME") == p.object_name_view())
                       .order_by("DATA_REPL_NUM")
                       .limit(MAX_SQL_ROWS)
                       .build();

    nlohmann::json rows;
    int status = gq2::try_execute(*conn, q, rows, srcPath);
    if (status >= 0) {
        if (rows.empty()) {
            status = CAT_NO_ROWS_FOUND;
        }
        else {
            auto* res = static_cast<GenQueryOut*>(std::calloc(1, sizeof(GenQueryOut)));
            if (!res) {
                return SYS_MALLOC_ERR;
            }
            res->rowCnt = static_cast<int>(rows.size());
            res->attriCnt = static_cast<int>(col_indices.size());
            res->totalRowCount = res->rowCnt;
            for (std::size_t c = 0; c < col_indices.size(); ++c) {
                std::size_t max_len = 1;
                for (const auto& row : rows) {
                    if (c < row.size() && row[c].is_string()) {
                        max_len = std::max(max_len, row[c].get_ref<const std::string&>().size() + 1);
                    }
                }
                res->sqlResult[c].attriInx = col_indices[c];
                res->sqlResult[c].len = static_cast<int>(max_len);
                res->sqlResult[c].value = static_cast<char*>(std::calloc(rows.size(), max_len));
                for (std::size_t r = 0; r < rows.size(); ++r) {
                    if (c < rows[r].size() && rows[r][c].is_string()) {
                        const auto& s = rows[r][c].get_ref<const std::string&>();
                        std::memcpy(&res->sqlResult[c].value[r * max_len], s.data(), s.size());
                    }
                }
            }
            genQueryOut = res;
        }
    }

    if (status < 0) {
        if (status == CAT_NO_ROWS_FOUND) {
            rodsLog(LOG_ERROR, "%s does not exist or user lacks access permission", srcPath);
        }
        else {
            rodsLogError(LOG_ERROR, status, "lsDataObjUtilLong: rcGenQuery error for %s", srcPath);
        }
        return status;
    }

    printLsLong(conn, rodsArgs, genQueryOut);

    return 0;
}

int
printLsLong( rcComm_t *conn, rodsArguments_t *rodsArgs,
             genQueryOut_t *genQueryOut ) {
    int i = 0;
    sqlResult_t *dataName = 0, *replNum = 0, *dataSize = 0, *rescName = 0,
                 *replStatus = 0, *dataModify = 0, *dataOwnerName = 0, *dataId = 0;
    sqlResult_t *chksumStr = 0, *dataPath = 0, *dataType = 0,*rescHier;

    char *tmpDataId = 0;
    int queryFlags = 0;

    if ( genQueryOut == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    queryFlags = setQueryFlag( rodsArgs );

    if ( rodsArgs->veryLongOption == True ) {
        if ( ( chksumStr = getSqlResultByInx( genQueryOut, COL_D_DATA_CHECKSUM ) )
                == NULL ) {
            rodsLog( LOG_ERROR,
                     "printLsLong: getSqlResultByInx for COL_D_DATA_CHECKSUM failed" );
            return UNMATCHED_KEY_OR_INDEX;
        }

        if ( ( dataPath = getSqlResultByInx( genQueryOut, COL_D_DATA_PATH ) )
                == NULL ) {
            rodsLog( LOG_ERROR,
                     "printLsLong: getSqlResultByInx for COL_D_DATA_PATH failed" );
            return UNMATCHED_KEY_OR_INDEX;
        }

        if ( ( dataType = getSqlResultByInx( genQueryOut, COL_DATA_TYPE_NAME ) ) == NULL ) {

            rodsLog( LOG_ERROR,
                     "printLsLong: getSqlResultByInx for COL_DATA_TYPE_NAME failed" );
            return UNMATCHED_KEY_OR_INDEX;
        }
    }

    if ( ( dataId = getSqlResultByInx( genQueryOut, COL_D_DATA_ID ) ) == NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_D_DATA_ID failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( dataName = getSqlResultByInx( genQueryOut, COL_DATA_NAME ) ) == NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_DATA_NAME failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( replNum = getSqlResultByInx( genQueryOut, COL_DATA_REPL_NUM ) ) ==
            NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_DATA_REPL_NUM failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( dataSize = getSqlResultByInx( genQueryOut, COL_DATA_SIZE ) ) == NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_DATA_SIZE failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( rescName = getSqlResultByInx( genQueryOut, COL_D_RESC_NAME ) ) == NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_D_RESC_NAME failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( rescHier = getSqlResultByInx( genQueryOut, COL_D_RESC_HIER ) ) == NULL ) {
        // If the index is not found then COL_D_RESC_HIER was most likely stripped
        // from the query input to talk to an older zone.
        // use resource name instead
        rescHier = rescName;
    }

    if ( ( replStatus = getSqlResultByInx( genQueryOut, COL_D_REPL_STATUS ) ) ==
            NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_D_REPL_STATUS failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( dataModify = getSqlResultByInx( genQueryOut, COL_D_MODIFY_TIME ) ) ==
            NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_D_MODIFY_TIME failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( dataOwnerName = getSqlResultByInx( genQueryOut, COL_D_OWNER_NAME ) ) ==
            NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsLong: getSqlResultByInx for COL_D_OWNER_NAME failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    queryHandle_t query_handle;
    rclInitQueryHandle( &query_handle, conn );
    for ( i = 0; i < genQueryOut->rowCnt; i++ ) {
        collEnt_t collEnt;

        std::memset(&collEnt, 0, sizeof(collEnt));

        collEnt.dataName = &dataName->value[dataName->len * i];
        collEnt.replNum = parse_int<int>( &replNum->value[replNum->len * i] );
        collEnt.dataSize = parse_int<rodsLong_t>( &dataSize->value[dataSize->len * i] );
        collEnt.resource = &rescName->value[rescName->len * i];
        collEnt.ownerName = &dataOwnerName->value[dataOwnerName->len * i];
        collEnt.replStatus = parse_int<int>( &replStatus->value[replStatus->len * i] );
        collEnt.modifyTime = &dataModify->value[dataModify->len * i];
        collEnt.resc_hier = &rescHier->value[rescHier->len * i];

        if ( rodsArgs->veryLongOption == True ) {
            collEnt.chksum = &chksumStr->value[chksumStr->len * i];
            collEnt.phyPath = &dataPath->value[dataPath->len * i];
            collEnt.dataType = &dataType->value[dataType->len * i];
        }

        printDataCollEntLong( &collEnt, queryFlags );

        if ( rodsArgs->accessControl == True ) {
            tmpDataId = &dataId->value[dataId->len * i];
            printDataAcl( conn, tmpDataId );
        }
    }

    return 0;
}

int
lsSpecDataObjUtilLong( rodsPath_t *srcPath, rodsArguments_t *rodsArgs ) {
    char sizeStr[NAME_LEN];
    int status;
    rodsObjStat_t *rodsObjStat = srcPath->rodsObjStat;

    snprintf( sizeStr, NAME_LEN, "%lld", rodsObjStat->objSize );
    status = printSpecLsLong( srcPath->outPath, rodsObjStat->ownerName,
                              sizeStr, rodsObjStat->modifyTime, rodsObjStat->specColl,
                              rodsArgs );

    return status;
}


int
printLsShort( rcComm_t *conn,  rodsArguments_t *rodsArgs,
              genQueryOut_t *genQueryOut ) {
    int i;

    sqlResult_t *dataName, *dataId;
    char *tmpDataName, *tmpDataId;

    if ( genQueryOut == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    if ( ( dataId = getSqlResultByInx( genQueryOut, COL_D_DATA_ID ) ) == NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsShort: getSqlResultByInx for COL_D_DATA_ID failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( dataName = getSqlResultByInx( genQueryOut, COL_DATA_NAME ) ) == NULL ) {
        rodsLog( LOG_ERROR,
                 "printLsShort: getSqlResultByInx for COL_DATA_NAME failed" );
        return UNMATCHED_KEY_OR_INDEX;
    }

    for ( i = 0; i < genQueryOut->rowCnt; i++ ) {
        tmpDataName = &dataName->value[dataName->len * i];
        printLsStrShort( tmpDataName );

        if ( rodsArgs->accessControl == True ) {
            tmpDataId = &dataId->value[dataId->len * i];
            printDataAcl( conn, tmpDataId );
        }
    }

    return 0;
}

int
lsCollUtil( rcComm_t *conn, rodsPath_t *srcPath, rodsEnv *myRodsEnv,
            rodsArguments_t *rodsArgs ) {
    int savedStatus = 0;
    char *srcColl;
    int status;
    int queryFlags;
    collHandle_t collHandle;
    collEnt_t collEnt;

    if ( srcPath == NULL ) {
        rodsLog( LOG_ERROR, "lsCollUtil: NULL srcPath input" );
        return USER__NULL_INPUT_ERR;
    }

    srcColl = srcPath->outPath;

    /* print this collection */
    printf( "%s:\n", srcColl );

    if ( rodsArgs->accessControl == True ) {
        printCollAcl( conn, srcColl );
        printCollInheritance( conn, srcColl );
    }

    // Return immediately if the user passed the "-d" option.
    // This option allows "ils" to mirror the behavior of "ls -d <directory>".
    if (True == rodsArgs->dataObjects) {
        return 0;
    }

    queryFlags = DATA_QUERY_FIRST_FG;
    if ( rodsArgs->veryLongOption == True ) {
        /* need to check veryLongOption first since it will have both
         * veryLongOption and longOption flags on. */
        queryFlags = queryFlags | VERY_LONG_METADATA_FG | NO_TRIM_REPL_FG;
    }
    else if ( rodsArgs->longOption == True ) {
        queryFlags |= LONG_METADATA_FG | NO_TRIM_REPL_FG;;
    }

    status = rclOpenCollection( conn, srcColl, queryFlags,
                                &collHandle );

    if ( status < 0 ) {
        rodsLog( LOG_ERROR,
                 "lsCollUtil: rclOpenCollection of %s error. status = %d",
                 srcColl, status );
        return status;
    }
    const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};
    while ( ( status = rclReadCollection( conn, &collHandle, &collEnt ) ) >= 0 ) {
        if ( collEnt.objType == DATA_OBJ_T ) {
            if ( rodsArgs->bundle == True ) {
                char myPath[MAX_NAME_LEN];
                snprintf( myPath, MAX_NAME_LEN, "%s/%s", collEnt.collName,
                          collEnt.dataName );
                lsSubfilesInBundle( conn, myPath );
            }
            else {
                printDataCollEnt( &collEnt, queryFlags );
                if ( rodsArgs->accessControl == True ) {
                    printDataAcl( conn, collEnt.dataId );
                }
            }
        }
        else {
            printCollCollEnt( &collEnt, queryFlags );
            /* need to drill down */
            if ( rodsArgs->recursive == True &&
                    strcmp( collEnt.collName, "/" ) != 0 ) {
                rodsPath_t tmpPath;
                memset( &tmpPath, 0, sizeof( tmpPath ) );
                rstrcpy( tmpPath.outPath, collEnt.collName, MAX_NAME_LEN );
                status = lsCollUtil( conn, &tmpPath, myRodsEnv, rodsArgs );
                if ( status < 0 ) {
                    savedStatus = status;
                }
            }
        }
    }
    if ( savedStatus < 0 && savedStatus != CAT_NO_ROWS_FOUND ) {
        return savedStatus;
    }
    else {
        return 0;
    }
}

int
printDataCollEnt( collEnt_t *collEnt, int flags ) {
    if ( ( flags & ( LONG_METADATA_FG | VERY_LONG_METADATA_FG ) ) == 0 ) {
        /* short */
        printLsStrShort( collEnt->dataName );
    }
    else {
        printDataCollEntLong( collEnt, flags );
    }
    return 0;
}

int
printDataCollEntLong( collEnt_t *collEnt, int flags ) {
    char *tmpReplStatus;
    char localTimeModify[TIME_LEN];
    char typeStr[NAME_LEN];

    switch (collEnt->replStatus) {
        case STALE_REPLICA:
            tmpReplStatus = "X";
            break;

        case GOOD_REPLICA:
            tmpReplStatus = "&";
            break;

        case INTERMEDIATE_REPLICA:
        default:
            tmpReplStatus = "?";
            break;
    }

    getLocalTimeFromRodsTime( collEnt->modifyTime, localTimeModify );
    if ( collEnt->specColl.collClass == NO_SPEC_COLL ||
            collEnt->specColl.collClass == LINKED_COLL ) {
        printf( "  %-12.12s %6d %s %12lld %16.16s %s %s\n",
                collEnt->ownerName, collEnt->replNum, collEnt->resc_hier,
                collEnt->dataSize, localTimeModify, tmpReplStatus, collEnt->dataName );
    }
    else {
        getSpecCollTypeStr( &collEnt->specColl, typeStr );
        printf( "  %-12.12s %6.6s %s %12lld %16.16s %s %s\n",
                collEnt->ownerName, typeStr, collEnt->resc_hier,
                collEnt->dataSize, localTimeModify, tmpReplStatus, collEnt->dataName );
    }


    if ( ( flags & VERY_LONG_METADATA_FG ) != 0 ) {
        printf( "    %s    %s    %s\n", collEnt->chksum, collEnt->dataType, collEnt->phyPath );

    }
    return 0;
}

int
printCollCollEnt( collEnt_t *collEnt, int flags ) {
    char typeStr[NAME_LEN];

    typeStr[0] = '\0';
    if ( collEnt->specColl.collClass != NO_SPEC_COLL ) {
        getSpecCollTypeStr( &collEnt->specColl, typeStr );
    }
    if ( ( flags & LONG_METADATA_FG ) != 0 ) {
        printf( "  C- %s  %s\n", collEnt->collName, typeStr );
    }
    else if ( ( flags & VERY_LONG_METADATA_FG ) != 0 ) {
        if ( collEnt->specColl.collClass == NO_SPEC_COLL ) {
            printf( "  C- %s\n", collEnt->collName );
        }
        else {
            if ( collEnt->specColl.collClass == MOUNTED_COLL ||
                    collEnt->specColl.collClass == LINKED_COLL ) {
                printf( "  C- %s  %6.6s  %s  %s   %s\n",
                        collEnt->collName, typeStr,
                        collEnt->specColl.objPath, collEnt->specColl.phyPath,
                        collEnt->specColl.rescHier );
            }
            else {
                printf( "  C- %s  %6.6s  %s  %s;;;%s;;;%d\n",
                        collEnt->collName, typeStr,
                        collEnt->specColl.objPath, collEnt->specColl.cacheDir,
                        collEnt->specColl.rescHier, collEnt->specColl.cacheDirty );
            }
        }
    }
    else {
        /* short */
        printf( "  C- %s\n", collEnt->collName );
    }
    return 0;
}

int
printSpecLsLong( char *objPath, char *ownerName, char *objSize,
                 char *modifyTime, specColl_t *specColl, rodsArguments_t *rodsArgs ) {
    collEnt_t collEnt;
    int queryFlags;

    std::memset(&collEnt, 0, sizeof(collEnt));

    collEnt.dataName = objPath;
    collEnt.ownerName = ownerName;
    collEnt.dataSize = parse_int<rodsLong_t>( objSize );
    collEnt.resource = specColl->resource;
    collEnt.resc_hier = specColl->rescHier;
    collEnt.modifyTime = modifyTime;
    collEnt.specColl = *specColl;
    collEnt.objType = DATA_OBJ_T;
    queryFlags = setQueryFlag( rodsArgs );

    printDataCollEntLong( &collEnt, queryFlags );
    return 0;
}

int
printDataAcl( rcComm_t *conn, char *dataId ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    printf( "        ACL - " );

    const auto q = gq2::builder::select({"DATA_ACCESS_USER_NAME", "DATA_ACCESS_USER_ZONE", "DATA_ACCESS_NAME", "DATA_ACCESS_USER_TYPE"})
        .where(col("DATA_ID") == dataId)
        .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*conn, q, rows, zoneHint); status < 0) {
        printf( "\n" );
        return status;
    }

    for (const auto& row : rows) {
        const auto& user_name = row[0].get_ref<const std::string&>();
        const auto& user_zone = row[1].get_ref<const std::string&>();
        const auto& access_name = row[2].get_ref<const std::string&>();
        const auto& user_type = row[3].get_ref<const std::string&>();
        const char* group_prefix = (user_type == "rodsgroup") ? "g:" : "";
        printf( "%s%s#%s:%s   ", group_prefix, user_name.c_str(), user_zone.c_str(), access_name.c_str() );
    }

    printf( "\n" );
    return 0;
}

int
printCollAcl( rcComm_t *conn, char *collName ) {
    genQueryOut_t *genQueryOut = NULL;
    const auto free_out = irods::at_scope_exit{[&genQueryOut] { freeGenQueryOut(&genQueryOut); }};
    int status;

    printf( "        ACL - " );
    /* First try a specific-query.  If this is defined, it should be
        used as it returns the group names without expanding them to
        individual users and this is important to some sites (iPlant,
        in particular).  If this fails, go on the the general-query.
     */
    status = queryCollAclSpecific( conn, collName, zoneHint, &genQueryOut );
    if ( status >= 0 ) {
        int i, j;
        for ( i = 0; i < genQueryOut->rowCnt; i++ ) {
            char *tResult[10];
            char empty = 0;
            char typeStr[8];
            tResult[3] = 0;

            for ( j = 0; j < 10; j++ ) {
                tResult[j] = &empty;
                if ( j < genQueryOut->attriCnt ) {
                    tResult[j] = genQueryOut->sqlResult[j].value;
                    tResult[j] += i * genQueryOut->sqlResult[j].len;
                }
            }
            typeStr[0] = '\0';
            if ( tResult[3] != 0 && strncmp( tResult[3], "rodsgroup", 9 ) == 0 ) {
                strncpy( typeStr, "g:", 3 );
            }
            printf( "%s%s#%s:%s   ",  typeStr, tResult[0], tResult[1], tResult[2] );

        }
        printf( "\n" );
        return status;
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    const auto q = gq2::builder::select({"COLL_ACCESS_USER_NAME", "COLL_ACCESS_USER_ZONE", "COLL_ACCESS_NAME"})
        .where(col("COLL_NAME") == collName)
        .build();

    nlohmann::json rows;
    if (status = gq2::try_execute(*conn, q, rows, zoneHint); status < 0) {
        printf( "\n" );
        return status;
    }

    for (const auto& row : rows) {
        const auto& user_name = row[0].get_ref<const std::string&>();
        const auto& user_zone = row[1].get_ref<const std::string&>();
        const auto& access_name = row[2].get_ref<const std::string&>();
        printf( "%s#%s:%s   ", user_name.c_str(), user_zone.c_str(), access_name.c_str() );
    }

    printf( "\n" );
    return 0;
}

int
printCollInheritance( rcComm_t *conn, char *collName ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    const auto q = gq2::builder::select({"COLL_INHERITANCE"})
        .where(col("COLL_NAME") == collName)
        .limit(1)
        .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*conn, q, rows, collName); status < 0) {
        return status;
    }
    if (rows.empty() || rows[0].empty()) {
        return CAT_NO_ROWS_FOUND;
    }

    const auto& inherit_str = rows[0][0].get_ref<const std::string&>();
    printf( "        Inheritance - " );
    if ( !inherit_str.empty() && inherit_str[0] == '1' ) {
        printf( "Enabled\n" );
    }
    else {
        printf( "Disabled\n" );
    }

    return 0;
}

void
printCollOrDir( char *myName, objType_t myType, rodsArguments_t *rodsArgs,
                specColl_t *specColl ) {
    char *typeStr;

    if ( rodsArgs->verbose == False ) {
        return;
    }

    if ( myType == COLL_OBJ_T ) {
        typeStr = "C";
    }
    else {
        typeStr = "D";
    }

    if ( specColl != NULL ) {
        char objType[NAME_LEN];
        int status;
        status = getSpecCollTypeStr( specColl, objType );
        if ( status < 0 ) {
            objType[0] = '\0';
        }
        fprintf( stdout, "%s- %s    %-5.5s :\n", typeStr, myName, objType );
    }
    else {
        fprintf( stdout, "%s- %s :\n", typeStr, myName );
    }
}

int
lsSubfilesInBundle( rcComm_t *conn, char *srcPath ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    fprintf( stdout, "Bundle file: %s\n", srcPath );
    fprintf( stdout, "Subfiles:\n" );

    const auto q = gq2::builder::select({"COLL_NAME", "DATA_NAME", "DATA_SIZE"})
        .where(col("DATA_PATH") == srcPath &&
               col("RESC_CLASS_NAME") == BUNDLE_RESC_CLASS)
        .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*conn, q, rows, srcPath); status < 0) {
        if ( status != CAT_NO_ROWS_FOUND ) {
            rodsLogError( LOG_ERROR, status,
                          "lsSubfilesInBundle: rsGenQuery error for %s",
                          srcPath );
        }
        return status;
    }

    for (const auto& row : rows) {
        fprintf( stdout, "    %s/%s    %s\n",
                 row[0].get_ref<const std::string&>().c_str(),
                 row[1].get_ref<const std::string&>().c_str(),
                 row[2].get_ref<const std::string&>().c_str() );
    }

    return 0;
}

