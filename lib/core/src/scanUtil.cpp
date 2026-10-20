#include "irods/scanUtil.h"

#include "irods/rodsPath.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/miscUtil.h"
#include "irods/genquery2_builder.hpp"
#include "irods/filesystem/path.hpp"
#include "irods/rcGlobalExtern.h"
#include "irods/rcMisc.h"

#include <fmt/format.h>

#include <charconv>
#include <filesystem>

namespace fs = std::filesystem;

int
scanObj( rcComm_t *conn,
         rodsArguments_t *myRodsArgs,
         rodsPathInp_t *rodsPathInp,
         const char * hostname ) {

    if ( rodsPathInp->numSrc != 1 ) {
        rodsLog( LOG_ERROR, "scanObj: gave %i input source path, should give one and only one", rodsPathInp->numSrc );
        return USER_INPUT_PATH_ERR;
    }

    char * inpPathO = rodsPathInp->srcPath[0].outPath;
    if ( rodsPathInp->srcPath[0].objType == LOCAL_FILE_T ||
            rodsPathInp->srcPath[0].objType == LOCAL_DIR_T ) {
        fs::path p( inpPathO );
        if ( !fs::exists( p ) ) {
            rodsLog( LOG_ERROR, "scanObj: %s does not exist", inpPathO );
            return USER_INPUT_PATH_ERR;
        }
        /* don't do anything if it is symlink */
        if ( fs::is_symlink( p ) ) {
            return 0;
        }

        char inpPath[ MAX_NAME_LEN ];
        snprintf( inpPath, sizeof( inpPath ), "%s", inpPathO );
        // if it is part of a mounted collection, abort
        if ( fs::is_directory( p ) ) {
            if ( int status = checkIsMount( conn, inpPath ) ) {
                rodsLog( LOG_ERROR, "The directory %s or one of its "
                         "subdirectories to be scanned is declared as being "
                         "used for a mounted collection: abort!", inpPath );
                return status;
            }
        }
        return scanObjDir( conn, myRodsArgs, inpPath, hostname );
    }
    else if ( rodsPathInp->srcPath[0].objType == UNKNOWN_OBJ_T ||
              rodsPathInp->srcPath[0].objType == COLL_OBJ_T ) {
        return scanObjCol( conn, myRodsArgs, inpPathO );
    }
    else {
        rodsLog( LOG_ERROR, "scanObj: %s does not exist", inpPathO );
        return USER_INPUT_PATH_ERR;
    }
}

int scanObjDir(rcComm_t* conn,
               rodsArguments_t* myRodsArgs,
               const char* inpPath,
               const char* hostname)
{
    const fs::path srcDirPath(inpPath);

    // check if it is a directory.
    if (fs::is_symlink(srcDirPath)) {
        return 0;
    }

    if (!fs::is_directory(srcDirPath)) {
        return chkObjExist(conn, inpPath, hostname);
    }

    // This variable will contain either the 0, or the last error
    // encountered as the loop below iterates through all of the
    // entries in the physical directory.

    int return_status = 0;

    try {
        int status = 0;
        char fullPath[MAX_NAME_LEN] = "\0";

        for (const auto& e : fs::directory_iterator{srcDirPath}) {
            snprintf(fullPath, MAX_NAME_LEN, "%s", e.path().c_str());

            if (e.is_symlink()) {
                continue;
            }

            if (e.is_directory()) {
                if (True == myRodsArgs->recursive) {
                    status = scanObjDir(conn, myRodsArgs, fullPath, hostname);
                }
            }
            else {
                status = chkObjExist(conn, fullPath, hostname);
            }

            if (status != 0) {
                return_status = status;
            }
        }
    }
    catch (const fs::filesystem_error& e) {
        rodsLog(LOG_ERROR, "scanObjDir: %s", e.what());
        return e.code().value();
    }

    return return_status;
}

int
scanObjCol( rcComm_t *conn, rodsArguments_t *myRodsArgs, const char *inpPath ) {
    namespace gq2 = irods::experimental::genquery2;
    namespace ifs = irods::experimental::filesystem;
    using gq2::builder::col;

    const ifs::path logical_path{inpPath ? inpPath : ""};
    const auto firstPart = logical_path.parent_path_view();
    const auto lastPart = logical_path.object_name_view();

    const auto q1 = gq2::builder::select({"COLL_ID"})
                        .where(col("COLL_NAME") == firstPart && col("DATA_NAME") == lastPart)
                        .limit(1)
                        .build();

    nlohmann::json rows1;
    const int status1 = gq2::try_execute(*conn, q1, rows1);
    const bool isColl = (status1 == CAT_NO_ROWS_FOUND || (status1 == 0 && rows1.empty()));

    auto cond2 = isColl
        ? ((myRodsArgs && myRodsArgs->recursive == True)
               ? col("COLL_NAME").like(fmt::format("{}%", inpPath))
               : (col("COLL_NAME") == inpPath))
        : (col("COLL_NAME") == firstPart && col("DATA_NAME") == lastPart);

    const auto q2 = gq2::builder::select({"DATA_PATH",
                                          "DATA_SIZE",
                                          "RESC_LOC",
                                          "RESC_ZONE_NAME",
                                          "DATA_NAME",
                                          "COLL_NAME",
                                          "DATA_RESC_ID"})
                        .where(std::move(cond2))
                        .build();

    nlohmann::json rows2;
    const int status2 = gq2::try_execute(*conn, q2, rows2);
    if (status2 < 0 || rows2.empty()) {
        printf( "Could not find the requested data object or collection in iRODS.\n" );
        return (status2 < 0) ? status2 : CAT_NO_ROWS_FOUND;
    }

    int rcPriv = 0, rcStat = 0, rcOther = 0;

    for (const auto& row : rows2) {
        const auto& dataPath = row.at(0).get_ref<const std::string&>();
        const auto& loc = row.at(2).get_ref<const std::string&>();
        const auto& zone = row.at(3).get_ref<const std::string&>();
        const auto& dataName = row.at(4).get_ref<const std::string&>();
        const auto& collName = row.at(5).get_ref<const std::string&>();
        const auto& rescID = row.at(6).get_ref<const std::string&>();

        fileStatInp_t fileStatInp{};
        rstrcpy( fileStatInp.addr.hostAddr, loc.c_str(), sizeof( fileStatInp.addr.hostAddr ) );
        rstrcpy( fileStatInp.addr.zoneName, zone.c_str(), sizeof( fileStatInp.addr.zoneName ) );
        rstrcpy( fileStatInp.fileName, dataPath.c_str(), sizeof( fileStatInp.fileName ) );
        std::from_chars(rescID.data(), rescID.data() + rescID.size(), fileStatInp.rescId);
        snprintf( fileStatInp.objPath, sizeof( fileStatInp.objPath ), "%s/%s", collName.c_str(), dataName.c_str() );

        rodsStat_t *fileStatOut = nullptr;
        const int status = rcFileStat( conn, &fileStatInp, &fileStatOut );
        free( fileStatOut );
        if ( SYS_NO_API_PRIV == status ) {
            printf( "User must be a rodsadmin to scan iRODS data objects.\n" );
            rcPriv = status;
        }
        else if ( status < 0 ) {
            if (getIrodsErrno(status) == UNIX_FILE_STAT_ERR) {
                printf( "Physical file %s on server %s is missing, corresponding to "
                        "iRODS object %s/%s\n", dataPath.c_str(), loc.c_str(), collName.c_str(), dataName.c_str() );
                rcStat = status;
            }
            else {
                rcOther = status;
            }
        }
    }

    if (rcPriv != 0) { return rcPriv; }
    if (rcStat != 0) { return rcStat; }
    if (rcOther != 0) { return rcOther; }

    return 0;
}

int
statPhysFile( rcComm_t *conn, genQueryOut_t *genQueryOut2 ) {

    int rcPriv = 0, rcStat = 0, rcOther = 0;

    for ( int i = 0; i < genQueryOut2->rowCnt; i++ ) {
        sqlResult_t *dataPathStruct = getSqlResultByInx( genQueryOut2, COL_D_DATA_PATH );
        sqlResult_t *dataSizeStruct = getSqlResultByInx( genQueryOut2, COL_DATA_SIZE );
        sqlResult_t *locStruct = getSqlResultByInx( genQueryOut2, COL_R_LOC );
        sqlResult_t *zoneStruct = getSqlResultByInx( genQueryOut2, COL_R_ZONE_NAME );
        sqlResult_t *dataNameStruct = getSqlResultByInx( genQueryOut2, COL_DATA_NAME );
        sqlResult_t *collNameStruct = getSqlResultByInx( genQueryOut2, COL_COLL_NAME );
        sqlResult_t *rescIDStruct = getSqlResultByInx( genQueryOut2, COL_D_RESC_ID );
        if ( dataPathStruct == NULL || dataSizeStruct == NULL || locStruct == NULL ||
                zoneStruct == NULL || dataNameStruct == NULL || collNameStruct == NULL ||
                rescIDStruct == NULL ) {
            printf( "getSqlResultByInx returned null in statPhysFile." );
            return -1;
        }

        char *dataPath = &dataPathStruct->value[dataPathStruct->len * i];
        char *loc = &locStruct->value[locStruct->len * i];
        char *zone = &zoneStruct->value[zoneStruct->len * i];
        char *dataName = &dataNameStruct->value[dataNameStruct->len * i];
        char *collName = &collNameStruct->value[collNameStruct->len * i];
        char *rescID = &rescIDStruct->value[rescIDStruct->len * i];

        /* check if the physical file does exist on the filesystem */
        fileStatInp_t fileStatInp;
        rstrcpy( fileStatInp.addr.hostAddr, loc, sizeof( fileStatInp.addr.hostAddr ) );
        rstrcpy( fileStatInp.addr.zoneName, zone, sizeof( fileStatInp.addr.zoneName ) );
        rstrcpy( fileStatInp.fileName, dataPath, sizeof( fileStatInp.fileName ) );
        fileStatInp.rescId = strtoll( rescID, 0, 0 );
        snprintf( fileStatInp.objPath, sizeof( fileStatInp.objPath ), "%s/%s", collName, dataName );
        rodsStat_t *fileStatOut;
        int status = rcFileStat( conn, &fileStatInp, &fileStatOut );
        if ( SYS_NO_API_PRIV == status ) {
            printf( "User must be a rodsadmin to scan iRODS data objects.\n" );
            rcPriv = status;
        }
        else if ( status < 0 ) {
            if (getIrodsErrno(status) == UNIX_FILE_STAT_ERR) {
              printf( "Physical file %s on server %s is missing, corresponding to "
                      "iRODS object %s/%s\n", dataPath, loc, collName, dataName );
              rcStat = status;
            }
            else {
              rcOther = status;
            }
        }
    } // for each data object i = 0 to rowCnt-1

    /*
     *    Prioritize the error returned
     */
    if (rcPriv != 0) { return rcPriv; }
    else if (rcStat != 0) { return rcStat; }
    else if (rcOther!= 0) { return rcOther; }

    return 0;
}

int
chkObjExist( rcComm_t *conn, const char *inpPath, const char *hostname ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    const auto q = gq2::builder::select({"DATA_ID"})
        .where(col("DATA_PATH") == inpPath &&
               (col("RESC_LOC").like(fmt::format("{}%", hostname)) || col("RESC_LOC") == hostname))
        .limit(1)
        .build();

    nlohmann::json rows;
    const int status = gq2::try_execute(*conn, q, rows);
    if ( status < 0 ) {
        return status;
    }
    if ( rows.empty() ) {
        printf( "%s is not registered in iRODS\n", inpPath );
        return CAT_NO_ROWS_FOUND;
    }

    return 0;
}

int
checkIsMount( rcComm_t *conn, const char *inpPath ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    const auto q = gq2::builder::select({"COLL_INFO_1"})
        .where(col("COLL_TYPE") == "mountPoint")
        .build();

    nlohmann::json rows;
    if (gq2::try_execute(*conn, q, rows) < 0 || rows.empty()) {
        return 0; /* there is no mounted collection, so no potential problem */
    }

    int status = 0;
    const std::string_view inp_view{inpPath};
    for (const auto& row : rows) {
        if (row.empty()) {
            continue;
        }
        const auto& dir_mpath = row[0].get_ref<const std::string&>();
        const auto min_len = std::min(dir_mpath.size(), inp_view.size());
        if (std::strncmp(dir_mpath.data(), inp_view.data(), min_len) == 0) {
            status = -1;
        }
    }

    return status;
}
