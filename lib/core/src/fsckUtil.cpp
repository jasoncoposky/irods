#include "irods/fsckUtil.h"

#include "irods/rodsPath.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/miscUtil.h"
#include "irods/scanUtil.h"
#include "irods/checksum.h"
#include "irods/genquery2_builder.hpp"
#include "irods/rcGlobalExtern.h"

#include <fmt/format.h>

#include <charconv>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>

namespace fs = std::filesystem;
namespace gq2 = irods::experimental::genquery2;
using gq2::builder::col;

int fsckObj(rcComm_t* conn,
            rodsArguments_t* myRodsArgs,
            rodsPathInp_t* rodsPathInp,
            const char* hostname)
{
    if (rodsPathInp->numSrc != 1) {
        rodsLog(LOG_ERROR, "fsckObj: gave %i input source path, "
                "should give one and only one", rodsPathInp->numSrc);
        return USER_INPUT_PATH_ERR;
    }

    char* inpPathO = rodsPathInp->srcPath[0].outPath;
    fs::path p(inpPathO);
    if (!fs::exists(p)) {
        rodsLog(LOG_ERROR, "fsckObj: %s does not exist", inpPathO);
        return USER_INPUT_PATH_ERR;
    }

    if (fs::is_symlink(p)) {
        return 0;
    }

    int lenInpPath = std::strlen(inpPathO);
    if (lenInpPath > 0 && '/' == inpPathO[lenInpPath - 1]) {
        lenInpPath--;
    }
    if (lenInpPath >= MAX_PATH_ALLOWED) {
        rodsLog(LOG_ERROR, "Path %s is longer than %ju characters in fsckObj",
                 inpPathO, (intmax_t) MAX_PATH_ALLOWED);
        return USER_STRLEN_TOOLONG;
    }

    char inpPath[MAX_PATH_ALLOWED];
    std::strncpy(inpPath, inpPathO, lenInpPath);
    inpPath[lenInpPath] = '\0';

    // If it is part of a mounted collection, abort.
    if (fs::is_directory(p)) {
        if (const int status = checkIsMount(conn, inpPath); status) {
            rodsLog(LOG_ERROR, "The directory %s or one of its "
                    "subdirectories to be checked is declared as being "
                    "used for a mounted collection: abort!", inpPath);
            return status;
        }
    }

    return fsckObjDir(conn, myRodsArgs, inpPath, hostname);
}

int fsckObjDir(rcComm_t* conn,
               rodsArguments_t* myRodsArgs,
               char* inpPath,
               const char* hostname)
{
    fs::path srcDirPath(inpPath);

    if (fs::is_symlink(srcDirPath)) {
        return 0;
    }

    if (!fs::is_directory(srcDirPath)) {
        return chkObjConsistency(conn, myRodsArgs, inpPath, hostname);
    }

    char fullPath[MAX_PATH_ALLOWED] = "\0";
    int status = 0;

    try {
        for (const auto& e : fs::directory_iterator{srcDirPath}) {
            std::snprintf(fullPath, MAX_PATH_ALLOWED, "%s", e.path().c_str());

            // Don't do anything if it is symlink.
            if (fs::is_symlink(e.path())) {
                continue;
            }

            if (fs::is_directory(e.path())) {
                if (myRodsArgs->recursive == True) {
                    const int tmp_status = fsckObjDir(conn, myRodsArgs, fullPath, hostname);
                    if (status == 0) {
                        status = tmp_status;
                    }
                }
            }
            else {
                const int tmp_status = chkObjConsistency(conn, myRodsArgs, fullPath, hostname);
                if (status == 0) {
                    status = tmp_status;
                }
            }
        }
    }
    catch (const fs::filesystem_error& e) {
        if (e.code() == std::errc::permission_denied) {
            rodsLog(LOG_ERROR, "Permission denied: \"%s\"", e.path1().c_str());
        }

        return e.code().value();
    }

    return status;
}

int chkObjConsistency(rcComm_t* conn,
                      rodsArguments_t* myRodsArgs,
                      char* inpPath,
                      const char* hostname)
{
    const fs::path p(inpPath);
    if (fs::is_symlink(p)) {
        return 0;
    }

    auto cond = (col("DATA_PATH") == inpPath);
    if (myRodsArgs && myRodsArgs->resource) {
        const std::string_view resc_hier = myRodsArgs->resourceString ? myRodsArgs->resourceString : "";
        cond = std::move(cond) && (col("DATA_RESC_HIER") == resc_hier);
    }
    else {
        const std::string_view host = hostname ? hostname : "";
        cond = std::move(cond) && (col("RESC_LOC").like(fmt::format("{}%", host)) || col("RESC_LOC") == host);
    }

    const auto q = gq2::builder::select({"DATA_NAME", "COLL_NAME", "DATA_SIZE", "DATA_CHECKSUM"})
                       .where(std::move(cond))
                       .build();

    nlohmann::json rows;
    int status = gq2::try_execute(*conn, q, rows);

    if (status == 0 && !rows.empty()) {
        const auto& row = rows.front();
        const auto& objName = row.at(0).get_ref<const std::string&>();
        const auto& objPath = row.at(1).get_ref<const std::string&>();
        const auto& sizeStr = row.at(2).get_ref<const std::string&>();
        const auto& objChksum = row.at(3).get_ref<const std::string&>();

        intmax_t objSize = 0;
        const auto [ptr, ec] = std::from_chars(sizeStr.data(), sizeStr.data() + sizeStr.size(), objSize);
        if (ec != std::errc{}) {
            std::cerr << "ERROR: could not parse object size into integer [size => " << sizeStr << ", path => " << inpPath << "].\n";
            return SYS_INTERNAL_ERR;
        }

        const intmax_t srcSize = fs::file_size(p);

        if (srcSize == objSize) {
            if (myRodsArgs->verifyChecksum == True) {
                if (!objChksum.empty()) {
                    status = verifyChksumLocFile(inpPath, objChksum.c_str(), nullptr);
                    if (status == USER_CHKSUM_MISMATCH) {
                        std::printf("CORRUPTION: local file [%s] checksum not consistent with iRODS object [%s/%s] checksum.\n", inpPath, objPath.c_str(), objName.c_str());
                    }
                    else if (status != 0) {
                        std::printf("ERROR chkObjConsistency: verifyChksumLocFile failed: status [%d] file [%s] objPath [%s] objName [%s] objChksum [%s]\n", status, inpPath, objPath.c_str(), objName.c_str(), objChksum.c_str());
                    }
                }
                else {
                    std::printf("WARNING: checksum not available for iRODS object [%s/%s], no checksum comparison possible with local file [%s] .\n", objPath.c_str(), objName.c_str(), inpPath);
                }
            }
        }
        else {
            std::printf("CORRUPTION: local file [%s] size [%ji] not consistent with iRODS object [%s/%s] size [%ji].\n", inpPath, srcSize, objPath.c_str(), objName.c_str(), objSize);
            status = SYS_INTERNAL_ERR;
        }
    }
    else if (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) {
        status = CAT_NO_ROWS_FOUND;
        std::printf("WARNING: local file [%s] is not registered in iRODS.\n", inpPath);
    }
    else {
        std::printf("ERROR chkObjConsistency: GenQuery2 failed: status [%d] file [%s]\n", status, inpPath);
    }

    return status;
}

