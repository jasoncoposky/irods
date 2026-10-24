#include <sys/types.h>
#include <sys/wait.h>
#include "irods/objMetaOpr.hpp"
#include "irods/resource.hpp"
#include "irods/collection.hpp"
#include "irods/specColl.hpp"
#include "irods/genQuery.h"
#include "irods/icatHighLevelRoutines.hpp"
#include "irods/miscUtil.h"
#include "irods/modAVUMetadata.h"
#include "irods/rsIcatOpr.hpp"
#include "irods/rsModAVUMetadata.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/irods_resource_redirect.hpp"
#include "irods/irods_hierarchy_parser.hpp"
#include "irods/filesystem/path.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/rs_genquery2.hpp"

#include <fmt/format.h>

#include <charconv>
#include <cstring>

namespace
{
    namespace fs = irods::filesystem;
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;
} // anonymous namespace

int
isData(rsComm_t *rsComm, char *objName, rodsLong_t *dataId) {
    if (!rsComm || !objName) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const fs::path p{objName};
    const auto parent = p.parent_path_view();
    const auto name = p.object_name_view();
    if (parent.empty() || name.empty()) {
        const auto err{ERROR(USER_INPUT_PATH_ERR, fmt::format("splitPathByKey failed for [{}]", objName))};
        irods::log(err);
        return err.code();
    }

    const auto q = gq2::builder::select({"DATA_ID", "COLL_ID"})
                       .where(col("DATA_NAME") == name && col("COLL_NAME") == parent)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*rsComm, q, rows); status < 0) {
        return status;
    }
    if (rows.empty()) {
        return CAT_NO_ROWS_FOUND;
    }

    if (nullptr != dataId) {
        *dataId = 0;
        const auto& id_str = rows.front().at(0).get_ref<const std::string&>();
        std::from_chars(id_str.data(), id_str.data() + id_str.size(), *dataId);
    }

    return 0;
}

int // JMC - backport 4680
getPhyPath(
    rsComm_t* _comm,
    char*     _obj_name,
    char*     _resource,
    char*     _phy_path,
    char*     _resc_hier ) {
    // =-=-=-=-=-=-=-
    // if no resc hier is specified, call resolve on the object to
    // ask the resource composition to pick a valid hier for open
    std::string resc_hier{};

    if ( 0 == strlen( _resc_hier ) ) {
        dataObjInp_t data_inp{};
        snprintf(data_inp.objPath, sizeof( data_inp.objPath ), "%s", _obj_name);
        try {
            auto result = irods::resolve_resource_hierarchy(irods::OPEN_OPERATION, _comm, data_inp);
            resc_hier = std::get<std::string>(result);
        }
        catch (const irods::exception& e) {
            irods::log(e);
            return SYS_INVALID_INPUT_PARAM;
        }
    }
    else {
        resc_hier = _resc_hier;
    }

    std::string              root_resc;
    irods::hierarchy_parser parser;
    parser.set_string( resc_hier );
    parser.first_resc( root_resc );

    const fs::path p{_obj_name};
    const auto q = gq2::builder::select({"DATA_PATH"})
                       .where(col("DATA_NAME") == p.object_name_view() &&
                              col("COLL_NAME") == p.parent_path_view() &&
                              col("DATA_RESC_HIER") == resc_hier)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    const int status = gq2::try_execute(*_comm, q, rows);
    if ( status < 0 ) {
        return status;
    }
    if ( rows.empty() ) {
        return CAT_NO_ROWS_FOUND;
    }

    const auto& phy_path_str = rows.front().at(0).get_ref<const std::string&>();
    rstrcpy( _phy_path,  phy_path_str.c_str(), MAX_NAME_LEN );
    rstrcpy( _resource,  root_resc.c_str(),    NAME_LEN );
    rstrcpy( _resc_hier, resc_hier.c_str(),    MAX_NAME_LEN );

    return 0;
}

int
isCollAllKinds( rsComm_t *rsComm, char *objName, rodsLong_t *collId ) {
    dataObjInp_t dataObjInp;
    int status;
    rodsObjStat_t *rodsObjStatOut = NULL;
    const auto free_stat = irods::at_scope_exit{[&rodsObjStatOut] { freeRodsObjStat(rodsObjStatOut); }};

    std::memset(&dataObjInp, 0, sizeof(dataObjInp));
    rstrcpy( dataObjInp.objPath, objName, MAX_NAME_LEN );
    status = collStatAllKinds( rsComm, &dataObjInp, &rodsObjStatOut );
    if ( status >= 0 && collId != NULL && NULL != rodsObjStatOut ) { // JMC cppcheck - nullptr
        *collId = 0;
        std::from_chars(rodsObjStatOut->dataId, rodsObjStatOut->dataId + std::strlen(rodsObjStatOut->dataId), *collId);
    }
    return status;
}

int
isColl( rsComm_t *rsComm, char *objName, rodsLong_t *collId ) {
    if (!rsComm || !objName) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const auto q = gq2::builder::select({"COLL_ID"})
                       .where(col("COLL_NAME") == objName)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    const int status = gq2::try_execute(*rsComm, q, rows);
    if ( status < 0 ) {
        return status;
    }
    if ( rows.empty() ) {
        return CAT_NO_ROWS_FOUND;
    }

    if ( collId != NULL ) {
        *collId = 0;
        const auto& id_str = rows.front().at(0).get_ref<const std::string&>();
        std::from_chars(id_str.data(), id_str.data() + id_str.size(), *collId);
    }

    return 0;
}

int
isUser( rsComm_t *rsComm, char *objName ) {
    if (!rsComm || !objName) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const auto q = gq2::builder::select({"USER_ID"})
                       .where(col("USER_NAME") == objName)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*rsComm, q, rows); status < 0) {
        return status;
    }
    return rows.empty() ? CAT_NO_ROWS_FOUND : 0;
}

int
isResc( rsComm_t *rsComm, char *objName ) {
    if (!rsComm || !objName) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const auto q = gq2::builder::select({"RESC_ID"})
                       .where(col("RESC_NAME") == objName)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*rsComm, q, rows); status < 0) {
        return status;
    }
    return rows.empty() ? CAT_NO_ROWS_FOUND : 0;
}

int
isMeta( rsComm_t*, char* ) {
    /* needs to be filled in later */
    return INVALID_OBJECT_TYPE;
}

int
isToken( rsComm_t*, char* ) {
    /* needs to be filled in later */
    return INVALID_OBJECT_TYPE;
}

int
getObjType( rsComm_t *rsComm, char *objName, char * objType ) {
    if ( isData( rsComm, objName, NULL ) >= 0 ) {
        strcpy( objType, "-d" );
    }
    else if ( isColl( rsComm, objName, NULL ) >= 0 ) {
        strcpy( objType, "-c" );
    }
    else if ( isResc( rsComm, objName ) == 0 ) {
        strcpy( objType, "-r" );
    }
    else if ( isUser( rsComm, objName ) == 0 ) {
        strcpy( objType, "-u" );
    }
    else if ( isMeta( rsComm, objName ) == 0 ) {
        strcpy( objType, "-m" );
    }
    else if ( isToken( rsComm, objName ) == 0 ) {
        strcpy( objType, "-t" );
    }
    else {
        return INVALID_OBJECT_TYPE;
    }
    return 0;
}

int
addAVUMetadataFromKVPairs( rsComm_t *rsComm, char *objName, char *inObjType,
                           keyValPair_t *kVP ) {
    char  objType[10];
    if ( strcmp( inObjType, "-1" ) ) {
        if ( strlen( inObjType ) >= sizeof( objType ) ) {
            rodsLog( LOG_ERROR, "inObjType: [%s] must be fewer than %ju characters", inObjType, ( uintmax_t )sizeof( objType ) );
            return SYS_INVALID_INPUT_PARAM;
        }
        strcpy( objType, inObjType );
    }
    else {
        int status = getObjType( rsComm, objName, objType );
        if ( status < 0 ) {
            return status;
        }
    }

    for ( int i = 0; i < kVP->len ; i++ ) {
        /* Call rsModAVUMetadata to call chlAddAVUMetadata.
           rsModAVUMetadata connects to the icat-enabled server if the
           local host isn't.
        */
        modAVUMetadataInp_t modAVUMetadataInp;
        memset( &modAVUMetadataInp, 0, sizeof( modAVUMetadataInp ) );
        modAVUMetadataInp.arg0 = "add";
        modAVUMetadataInp.arg1 = objType;
        modAVUMetadataInp.arg2 = objName;
        modAVUMetadataInp.arg3 = kVP->keyWord[i];
        modAVUMetadataInp.arg4 = kVP->value[i];
        modAVUMetadataInp.arg5 = "";
        int status = rsModAVUMetadata( rsComm, &modAVUMetadataInp );
        if ( status < 0 ) {
            return status;
        }
    }
    return 0;
}
// =-=-=-=-=-=-=-
// JMC - backport 4836
int
setAVUMetadataFromKVPairs( rsComm_t *rsComm, char *objName, char *inObjType,
                           keyValPair_t *kVP ) {

    char  objType[10];
    if ( strcmp( inObjType, "-1" ) ) {
        snprintf( objType, sizeof( objType ), "%s", inObjType );
    }
    else {
        int status = getObjType( rsComm, objName, objType );
        if ( status < 0 ) {
            return status;
        }
    }
    for ( int i = 0; i < kVP->len ; i++ ) {
        /* Call rsModAVUMetadata to call chlSetAVUMetadata.
           rsModAVUMetadata connects to the icat-enabled server if the
           local host isn't.
        */
        modAVUMetadataInp_t modAVUMetadataInp;
        memset( &modAVUMetadataInp, 0, sizeof( modAVUMetadataInp ) );
        modAVUMetadataInp.arg0 = "set";
        modAVUMetadataInp.arg1 = objType;
        modAVUMetadataInp.arg2 = objName;
        modAVUMetadataInp.arg3 = kVP->keyWord[i];
        modAVUMetadataInp.arg4 = kVP->value[i];
        modAVUMetadataInp.arg5 = NULL;
        int status = rsModAVUMetadata( rsComm, &modAVUMetadataInp );
        if ( status < 0 ) {
            return status;
        }
    }
    return 0;
}

int
getStructFileType( specColl_t *specColl ) {
    if ( specColl == NULL ) {
        return -1;
    }

    if ( specColl->collClass == STRUCT_FILE_COLL ) {
        return ( int ) specColl->type;
    }

    return -1;
}

int
removeAVUMetadataFromKVPairs( rsComm_t *rsComm, char *objName, char *inObjType,
                              keyValPair_t *kVP ) {
    char  objType[10];
    if ( strcmp( inObjType, "-1" ) ) {
        snprintf( objType, sizeof( objType ), "%s", inObjType );
    }
    else {
        int status = getObjType( rsComm, objName, objType );
        if ( status < 0 ) {
            return status;
        }
    }

    for ( int i = 0; i < kVP->len ; i++ ) {
        /* Call rsModAVUMetadata to call chlAddAVUMetadata.
           rsModAVUMetadata connects to the icat-enabled server if the
           local host isn't.
        */
        modAVUMetadataInp_t modAVUMetadataInp;
        memset( &modAVUMetadataInp, 0, sizeof( modAVUMetadataInp ) );
        modAVUMetadataInp.arg0 = "rm";
        modAVUMetadataInp.arg1 = objType;
        modAVUMetadataInp.arg2 = objName;
        modAVUMetadataInp.arg3 = kVP->keyWord[i];
        modAVUMetadataInp.arg4 = kVP->value[i];
        modAVUMetadataInp.arg5 = NULL;
        int status = rsModAVUMetadata( rsComm, &modAVUMetadataInp );
        if ( status < 0 ) {
            return status;
        }
    }
    return 0;
}

rodsLong_t
getTokenId( rsComm_t *rsComm, char *tokenNameSpace, char *tokenName ) {
    if (!rsComm || !tokenNameSpace || !tokenName) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const auto q = gq2::builder::select({"TOKEN_ID"})
                       .where(col("TOKEN_NAMESPACE") == tokenNameSpace && col("TOKEN_NAME") == tokenName)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*rsComm, q, rows); status < 0) {
        return status;
    }
    if (rows.empty()) {
        return CAT_NO_ROWS_FOUND;
    }

    const auto& id_str = rows.front().at(0).get_ref<const std::string&>();
    rodsLong_t token_id = 0;
    std::from_chars(id_str.data(), id_str.data() + id_str.size(), token_id);
    return token_id;
}

rodsLong_t
getUserId( rsComm_t *rsComm, char *userName, char *zoneName ) {
    if (!rsComm || !userName || !zoneName) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const auto q = gq2::builder::select({"USER_ID"})
                       .where(col("USER_NAME") == userName && col("USER_ZONE") == zoneName)
                       .limit(1)
                       .build();

    nlohmann::json rows;
    if (const int status = gq2::try_execute(*rsComm, q, rows); status < 0) {
        return status;
    }
    if (rows.empty()) {
        return CAT_NO_ROWS_FOUND;
    }

    const auto& id_str = rows.front().at(0).get_ref<const std::string&>();
    rodsLong_t user_id = 0;
    std::from_chars(id_str.data(), id_str.data() + id_str.size(), user_id);
    return user_id;
}

namespace
{
    int checkPermitForDataObject(rsComm_t* rsComm, char* objName, rodsLong_t userId, rodsLong_t operId)
    {
        const fs::path p{objName};
        const auto q = gq2::builder::select({"DATA_ID"})
                           .where(col("DATA_NAME") == p.object_name_view() &&
                                  col("COLL_NAME") == p.parent_path_view() &&
                                  col("DATA_ACCESS_USER_ID") == fmt::to_string(userId) &&
                                  col("DATA_ACCESS_TYPE") >= fmt::to_string(operId))
                           .limit(1)
                           .build();

        const auto rows = gq2::try_execute(*rsComm, q);
        return (rows && !rows->empty()) ? 1 : 0;
    }

    int checkPermitForCollection(rsComm_t* rsComm, char* objName, rodsLong_t userId, rodsLong_t operId)
    {
        const auto q = gq2::builder::select({"COLL_ID"})
                           .where(col("COLL_NAME") == objName &&
                                  col("COLL_ACCESS_USER_ID") == fmt::to_string(userId) &&
                                  col("COLL_ACCESS_TYPE") >= fmt::to_string(operId))
                           .limit(1)
                           .build();

        const auto rows = gq2::try_execute(*rsComm, q);
        return (rows && !rows->empty()) ? 1 : 0;
    }

    int checkPermitForResource(rsComm_t* rsComm, char* objName, rodsLong_t userId, rodsLong_t operId)
    {
        const auto q = gq2::builder::select({"RESC_ID"})
                           .where(col("RESC_NAME") == objName &&
                                  col("RESC_ACCESS_USER_ID") == fmt::to_string(userId) &&
                                  col("RESC_ACCESS_TYPE") >= fmt::to_string(operId))
                           .limit(1)
                           .build();

        const auto rows = gq2::try_execute(*rsComm, q);
        return (rows && !rows->empty()) ? 1 : 0;
    }
} // anonymous namespace

int
checkPermissionByObjType( rsComm_t *rsComm, char *objName, char *objType, char *user, char *zone, char *oper ) {
    const rodsLong_t operId = getTokenId(rsComm, "access_type", oper);
    if ( operId < 0 ) {
        // jjames - if they provide an invalid oper, give a better return code
        if ( operId == CAT_NO_ROWS_FOUND ) {
            return SYS_INVALID_INPUT_PARAM;
        }
        return operId;
    }

    const rodsLong_t userId = getUserId(rsComm, user, zone);
    if ( userId < 0 ) {
        return userId;
    }

    int i{};
    if ( !strcmp( objType, "-d" ) ) {
        i = checkPermitForDataObject( rsComm, objName, userId, operId );
    }
    else  if ( !strcmp( objType, "-c" ) ) {
        i = checkPermitForCollection( rsComm, objName, userId, operId );
    }
    else  if ( !strcmp( objType, "-r" ) ) {
        i = checkPermitForResource( rsComm, objName, userId, operId );
    }
    else {
        return INVALID_OBJECT_TYPE;
    }

    if ( i == 0 ) { // not found - check groups
        const auto q = gq2::builder::select({"USER_GROUP_ID"})
                           .where(col("USER_NAME") == user && col("USER_ZONE") == zone)
                           .build();

        if (const auto rows = gq2::try_execute(*rsComm, q); rows) {
            for (const auto& row : *rows) {
                const auto& group_id_str = row.at(0).get_ref<const std::string&>();
                rodsLong_t groupId = -1;
                if (std::from_chars(group_id_str.data(), group_id_str.data() + group_id_str.size(), groupId).ec != std::errc{} ||
                    groupId < 0 || groupId == userId)
                {
                    continue;
                }

                if ( !strcmp( objType, "-d" ) ) {
                    i = checkPermitForDataObject( rsComm, objName, groupId, operId );
                }
                else if ( !strcmp( objType, "-c" ) ) {
                    i = checkPermitForCollection( rsComm, objName, groupId, operId );
                }
                else if ( !strcmp( objType, "-r" ) ) {
                    i = checkPermitForResource( rsComm, objName, groupId, operId );
                }

                if (i == 1) {
                    break;
                }
            }
        }
    }

    return i;
}

