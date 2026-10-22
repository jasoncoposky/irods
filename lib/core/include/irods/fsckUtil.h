/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/* fsckUtil.h - Header for fsckUtil.c */

#ifndef FSCK_UTIL_H__
#define FSCK_UTIL_H__

#include "irods/rodsClient.h"
#include "irods/parseCommandLine.h"
#include "irods/rodsPath.h"

#ifdef __cplusplus
extern "C" {
#endif

int
fsckObj( rcComm_t *conn, rodsArguments_t *myRodsArgs, rodsPathInp_t *rodsPathInp, const char* hostname );
int
fsckObjDir( rcComm_t *conn, rodsArguments_t *myRodsArgs, char *inpPath, const char* hostname );
int
chkObjConsistency( rcComm_t *conn, rodsArguments_t *myRodsArgs, char *inpPath, const char* hostname );

#ifdef __cplusplus
}
#endif
#endif  // FSCK_UTIL_H__
