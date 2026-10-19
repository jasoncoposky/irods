#include "irods/scanUtil.h"

#include "irods/rodsPath.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/miscUtil.h"
#include "irods/genquery2_builder.hpp"
#include "irods/rcGlobalExtern.h"
#include "irods/rcMisc.h"

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/convenience.hpp>
#include <fmt/format.h>

using namespace boost::filesystem;

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
        path p( inpPathO );
        if ( !exists( p ) ) {
            rodsLog( LOG_ERROR, "scanObj: %s does not exist", inpPathO );
            return USER_INPUT_PATH_ERR;
        }
        /* don't do anything if it is symlink */
        if ( is_symlink( p ) ) {
            return 0;
        }

        char inpPath[ MAX_NAME_LEN ];
        snprintf( inpPath, sizeof( inpPath ), "%s", inpPathO );
        // if it is part of a mounted collection, abort
        if ( is_directory( p ) ) {
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
    const path srcDirPath(inpPath);

    // check if it is a directory.
    if (is_symlink(srcDirPath)) {
        return 0;
    }

    if (!is_directory(srcDirPath)) {
        return chkObjExist(conn, inpPath, hostname);
    }

    // This variable will contain either the 0, or the last error
    // encountered as the loop below iterates through all of the
    // entries in the physical directory.

    int return_status = 0;

    try {
        int status = 0;
        char fullPath[MAX_NAME_LEN] = "\0";

        for (const auto& e : directory_iterator{srcDirPath}) {
            snprintf(fullPath, MAX_NAME_LEN, "%s", e.path().c_str());

            if (is_symlink(e)) {
                continue;
            }

            if (is_directory(e)) {
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
    catch (const filesystem_error& e) {
        rodsLog(LOG_ERROR, "scanObjDir: %s", e.what());
        return e.code().value();
    }

    return return_status;
}

int
scanObjCol( rcComm_t *conn, rodsArguments_t *myRodsArgs, const char *inpPath ) {
    int isColl, status;
    genQueryInp_t genQueryInp1, genQueryInp2;
    genQueryOut_t *genQueryOut1 = NULL, *genQueryOut2 = NULL;
    char condStr1[MAX_NAME_LEN], condStr2[MAX_NAME_LEN];
    char firstPart[MAX_NAME_LEN] = "";

    /* check if inpPath is a file or a collection */
    const char *lastPart = strrchr( inpPath, '/' ) + 1;
    strncpy( firstPart, inpPath, strlen( inpPath ) - strlen( lastPart ) - 1 );
    memset( &genQueryInp1, 0, sizeof( genQueryInp1 ) );
    addInxIval( &genQueryInp1.selectInp, COL_COLL_ID, 1 );
    genQueryInp1.maxRows = MAX_SQL_ROWS;

    snprintf( condStr1, MAX_NAME_LEN, "='%s'", firstPart );
    addInxVal( &genQueryInp1.sqlCondInp, COL_COLL_NAME, condStr1 );
    snprintf( condStr1, MAX_NAME_LEN, "='%s'", lastPart );
    addInxVal( &genQueryInp1.sqlCondInp, COL_DATA_NAME, condStr1 );

    status =  rcGenQuery( conn, &genQueryInp1, &genQueryOut1 );
    if ( status == CAT_NO_ROWS_FOUND ) {
        isColl = 1;
    }
    else {
        isColl = 0;
    }

    /* for each files check if the physical file associated to it exists on the
       physical resource */
    memset( &genQueryInp2, 0, sizeof( genQueryInp2 ) );
    addInxIval( &genQueryInp2.selectInp, COL_D_DATA_PATH, 1 );
    addInxIval( &genQueryInp2.selectInp, COL_DATA_SIZE, 1 );
    addInxIval( &genQueryInp2.selectInp, COL_R_LOC, 1 );
    addInxIval( &genQueryInp2.selectInp, COL_R_ZONE_NAME, 1 );
    addInxIval( &genQueryInp2.selectInp, COL_DATA_NAME, 1 );
    addInxIval( &genQueryInp2.selectInp, COL_COLL_NAME, 1 );
    addInxIval( &genQueryInp2.selectInp, COL_D_RESC_ID, 1 );
    genQueryInp2.maxRows = MAX_SQL_ROWS;

    if ( isColl ) {
        if ( myRodsArgs->recursive == True ) {
            snprintf( condStr2, MAX_NAME_LEN, "like '%s%s'", inpPath, "%" );
        }
        else {
            snprintf( condStr2, MAX_NAME_LEN, "='%s'", inpPath );
        }
        addInxVal( &genQueryInp2.sqlCondInp, COL_COLL_NAME, condStr2 );
    }
    else {
        snprintf( condStr2, MAX_NAME_LEN, "='%s'", firstPart );
        addInxVal( &genQueryInp2.sqlCondInp, COL_COLL_NAME, condStr2 );
        snprintf( condStr2, MAX_NAME_LEN, "='%s'", lastPart );
        addInxVal( &genQueryInp2.sqlCondInp, COL_DATA_NAME, condStr2 );
    }

    /* check if the physical file corresponding to the iRODS object does exist */
    status =  rcGenQuery( conn, &genQueryInp2, &genQueryOut2 );
    if (0 == status) {
        status = statPhysFile( conn, genQueryOut2 );
    }
    else {
        printf( "Could not find the requested data object or collection in iRODS.\n" );
    }

    while ( (0 == status || getIrodsErrno(status) == UNIX_FILE_STAT_ERR)
             && genQueryOut2->continueInx > 0)
    {
        genQueryInp2.continueInx = genQueryOut2->continueInx;
        status = rcGenQuery( conn, &genQueryInp2, &genQueryOut2 );
        if ( 0 == status ) {
            status = statPhysFile( conn, genQueryOut2 );
        }
    }

    freeGenQueryOut( &genQueryOut1 );
    freeGenQueryOut( &genQueryOut2 );

    return status;

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
