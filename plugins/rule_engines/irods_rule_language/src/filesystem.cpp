/* For copyright information please refer to files in the COPYRIGHT directory
 */

#include <sys/stat.h>
#include <errno.h>
#include "irods/private/re/debug.hpp"
#include "irods/private/re/utils.hpp"
#include "irods/private/re/datetime.hpp"
#include "irods/private/re/filesystem.hpp"
#include "irods/irods_get_full_path_for_config_file.hpp"
#include "irods/irods_log.hpp"
#include "irods/rodsConnect.h"

char *getRuleBasePath(const char *ruleBaseName, char rulesFileName[MAX_NAME_LEN] ) {
    std::string cfg_file, fn( ruleBaseName );
    fn += ".re";
    irods::error ret = irods::get_full_path_for_config_file( fn, cfg_file );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        return nullptr;
    }
    return rstrcpy( rulesFileName, cfg_file.c_str(), MAX_NAME_LEN);
}

int getModifiedTime( const char *fn, time_type *timestamp ) {
    struct stat st{};
    if ( stat( fn, &st ) != 0 ) {
        rodsLog(LOG_ERROR, "getModifiedTime: stat call failed on [%s] with errno: [%d]", fn, errno);
        return -1;
    }
    time_type_set( *timestamp, st.st_mtime );
    return 0;
}
