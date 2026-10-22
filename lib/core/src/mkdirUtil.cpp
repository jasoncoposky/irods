/*** Copyright (c), The Regents of the University of California            ***
 *** For more infomkdiration please refer to files in the COPYRIGHT directory ***/
#include "irods/rodsPath.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/mkdirUtil.h"
#include "irods/miscUtil.h"
#include "irods/rcGlobalExtern.h"
#include "irods/filesystem.hpp"

#include <system_error>

int
mkdirUtil( rcComm_t *conn, rodsArguments_t *myRodsArgs,
           rodsPathInp_t *rodsPathInp ) {
    if ( rodsPathInp == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    namespace fs = irods::filesystem;

    int status = 0;
    int savedStatus = 0;

    for ( int i = 0; i < rodsPathInp->numSrc; i++ ) {
        const fs::path target_path{rodsPathInp->srcPath[i].outPath};
        std::error_code ec;

        /* -p for parent */
        if ( myRodsArgs->physicalPath == True ) {
            fs::client::create_collections( *conn, target_path, ec );
        }
        else {
            if ( !fs::client::create_collection( *conn, target_path, ec ) && !ec ) {
                ec = irods::make_error_code( CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME );
            }
        }

        status = ec.value();
        if ( status < 0 ) {
            rodsLogError( LOG_ERROR, status,
                          "mkdirUtil: mkColl of %s error.",
                          rodsPathInp->srcPath[i].outPath );
            savedStatus = status;
        }
    }

    if ( savedStatus < 0 ) {
        return savedStatus;
    }
    return status;
}

