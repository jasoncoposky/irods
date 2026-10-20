#include "irods/rcMisc.h"
#include "irods/getRescQuota.h"
#include "irods/miscUtil.h"

#include "irods/irods_logger.hpp"
#include "irods/objMetaOpr.hpp"
#include "irods/resource.hpp"
#include "irods/rsGetRescQuota.hpp"
#include "irods/genQuery.h"
#include "irods/genquery2_builder.hpp"
#include "irods/rs_genquery2.hpp"
#include "irods/scoped_privileged_client.hpp"

#include "irods/irods_resource_backport.hpp"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <string>
#include <vector>

using log_api = irods::experimental::log::server;

int
rsGetRescQuota( rsComm_t *rsComm, getRescQuotaInp_t *getRescQuotaInp,
                rescQuota_t **rescQuota ) {
    rodsServerHost_t *rodsServerHost;
    int status = 0;

    status = getAndConnRcatHost(rsComm, SECONDARY_RCAT, (const char*) getRescQuotaInp->zoneHint, &rodsServerHost);

    if ( status < 0 ) {
        return status;
    }

    if ( rodsServerHost->localFlag == LOCAL_HOST ) {
        status = _rsGetRescQuota( rsComm, getRescQuotaInp, rescQuota );
    }
    else {
        status = rcGetRescQuota( rodsServerHost->conn, getRescQuotaInp,
                                 rescQuota );
    }

    return status;
}

int _rsGetRescQuota(
    rsComm_t*          rsComm,
    getRescQuotaInp_t* getRescQuotaInp,
    rescQuota_t**      rescQuota ) {
    int status = 0;

    genQueryOut_t* genQueryOut = NULL;

    if ( rescQuota == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    *rescQuota = NULL;

    status = getQuotaByResc(
                 rsComm,
                 getRescQuotaInp->userName,
                 getRescQuotaInp->rescName,
                 &genQueryOut );
    if ( status >= 0 ) {
        queRescQuota( rescQuota, genQueryOut );
    }

    freeGenQueryOut( &genQueryOut );

    return 0;

}

/* getQuotaByResc - get the quoto for an individual resource. The code is
 * mostly from doTest7 of iTestGenQuery.c
 */

int getQuotaByResc(
    rsComm_t*       rsComm,
    char*           userName,
    char*           rescName,
    genQueryOut_t** genQueryOut ) {
    if ( genQueryOut == NULL || rsComm == NULL || userName == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    *genQueryOut = NULL;

    char user_name[NAME_LEN]{};
    char user_zone[NAME_LEN]{};
    if ( const int status = parseUserName( userName, user_name, user_zone ); status != 0 ) {
        return status;
    }
    if ( user_zone[0] == '\0' ) {
        if ( rsComm->clientUser.rodsZone[0] != '\0' ) {
            rstrcpy( user_zone, rsComm->clientUser.rodsZone, sizeof( user_zone ) );
        }
        else if ( const char* local_zone = getLocalZoneName(); local_zone != nullptr ) {
            rstrcpy( user_zone, local_zone, sizeof( user_zone ) );
        }
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    irods::experimental::scoped_privileged_client spc{*rsComm};

    const auto q_user = gq2::builder::select({"USER_ID"})
                            .where(col("USER_NAME") == user_name && col("USER_ZONE") == user_zone)
                            .limit(1)
                            .build();

    nlohmann::json user_rows;
    if ( const int status = gq2::try_execute( *rsComm, q_user, user_rows ); status < 0 ) {
        return status;
    }
    if ( user_rows.empty() ) {
        return CAT_NO_ROWS_FOUND;
    }

    const auto& user_id = user_rows.front().at(0).get_ref<const std::string&>();
    std::vector<std::string> subject_ids{user_id};

    const auto q_groups = gq2::builder::select({"GROUP_ID"})
                              .where(col("GROUP_MEMBER_ID") == user_id)
                              .build();

    nlohmann::json group_rows;
    if ( gq2::try_execute( *rsComm, q_groups, group_rows ) >= 0 ) {
        for ( const auto& row : group_rows ) {
            const auto& gid = row.at(0).get_ref<const std::string&>();
            if ( std::find( subject_ids.begin(), subject_ids.end(), gid ) == subject_ids.end() ) {
                subject_ids.push_back( gid );
            }
        }
    }

    struct quota_row
    {
        std::string user_id;
        std::string resc_name;
        std::string quota_limit;
        std::string quota_over;
        std::string resc_id;
        long long over_val{};
    };
    std::vector<quota_row> collected;

    // 1. Global quotas (RESC_ID = 0)
    const auto q_global = gq2::builder::select({"QUOTA_USER_ID", "QUOTA_LIMIT", "QUOTA_OVER", "QUOTA_RESC_ID"})
                              .where(col("QUOTA_USER_ID").in(subject_ids) && col("QUOTA_RESC_ID") == "0")
                              .build();

    nlohmann::json global_rows;
    if ( gq2::try_execute( *rsComm, q_global, global_rows ) >= 0 ) {
        const std::string global_resc_name = ( rescName != NULL && rescName[0] != '\0' ) ? rescName : "";
        for ( const auto& row : global_rows ) {
            quota_row qr{
                row.at(0).get<std::string>(),
                global_resc_name,
                row.at(1).get<std::string>(),
                row.at(2).get<std::string>(),
                row.at(3).get<std::string>(),
                0};
            std::from_chars( qr.quota_over.data(), qr.quota_over.data() + qr.quota_over.size(), qr.over_val );
            collected.push_back( std::move( qr ) );
        }
    }

    // 2. Per-resource quotas (joined with R_RESC_MAIN)
    auto resc_cond = col("QUOTA_USER_ID").in(subject_ids);
    if ( rescName != NULL && strlen( rescName ) > 0 ) {
        resc_cond = std::move( resc_cond ) && col("RESC_NAME") == rescName;
    }

    const auto q_resc = gq2::builder::select({"QUOTA_USER_ID", "RESC_NAME", "QUOTA_LIMIT", "QUOTA_OVER", "QUOTA_RESC_ID"})
                            .where(std::move(resc_cond))
                            .build();

    nlohmann::json resc_rows;
    if ( gq2::try_execute( *rsComm, q_resc, resc_rows ) >= 0 ) {
        for ( const auto& row : resc_rows ) {
            quota_row qr{
                row.at(0).get<std::string>(),
                row.at(1).get<std::string>(),
                row.at(2).get<std::string>(),
                row.at(3).get<std::string>(),
                row.at(4).get<std::string>(),
                0};
            std::from_chars( qr.quota_over.data(), qr.quota_over.data() + qr.quota_over.size(), qr.over_val );
            collected.push_back( std::move( qr ) );
        }
    }

    if ( collected.empty() ) {
        return CAT_NO_ROWS_FOUND;
    }

    std::stable_sort( collected.begin(), collected.end(), []( const quota_row& a, const quota_row& b ) {
        return a.over_val < b.over_val;
    } );

    nlohmann::json combined = nlohmann::json::array();
    for ( const auto& qr : collected ) {
        combined.push_back( nlohmann::json::array(
            {qr.user_id, qr.resc_name, qr.quota_limit, qr.quota_over, qr.resc_id} ) );
    }

    return gq2::rows_to_genquery_out(
        combined,
        {COL_QUOTA_USER_ID, COL_R_RESC_NAME, COL_QUOTA_LIMIT, COL_QUOTA_OVER, COL_QUOTA_RESC_ID},
        genQueryOut );
}

int queRescQuota(
    rescQuota_t**  rescQuotaHead,
    genQueryOut_t* genQueryOut ) {

    sqlResult_t *quotaLimit, *quotaOver, *rescName, *quotaRescId, *quotaUserId;
    char *tmpQuotaLimit, *tmpQuotaOver, *tmpRescName, *tmpQuotaRescId,
         *tmpQuotaUserId;
    int i;
    rescQuota_t *tmpRescQuota;

    if ( ( quotaLimit = getSqlResultByInx( genQueryOut, COL_QUOTA_LIMIT ) ) ==
            NULL ) {
        log_api::error("queRescQuota: getSqlResultByInx for COL_QUOTA_LIMIT failed");
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( quotaOver = getSqlResultByInx( genQueryOut, COL_QUOTA_OVER ) ) == NULL ) {
        log_api::error("queRescQuota: getSqlResultByInx for COL_QUOTA_OVER failed");
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( rescName = getSqlResultByInx( genQueryOut, COL_R_RESC_NAME ) ) == NULL ) {
        log_api::error("queRescQuota: getSqlResultByInx for COL_R_RESC_NAME failed");
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( quotaRescId = getSqlResultByInx( genQueryOut, COL_QUOTA_RESC_ID ) ) ==
            NULL ) {
        log_api::error("queRescQuota: getSqlResultByInx for COL_QUOTA_RESC_ID failed");
        return UNMATCHED_KEY_OR_INDEX;
    }

    if ( ( quotaUserId = getSqlResultByInx( genQueryOut, COL_QUOTA_USER_ID ) ) ==
            NULL ) {
        log_api::error("queRescQuota: getSqlResultByInx for COL_QUOTA_USER_ID failed");
        return UNMATCHED_KEY_OR_INDEX;
    }

    for ( i = 0; i < genQueryOut->rowCnt; i++ ) {
        tmpQuotaLimit =  &quotaLimit->value[quotaLimit->len * i];
        tmpQuotaOver =  &quotaOver->value[quotaOver->len * i];
        tmpRescName =  &rescName->value[rescName->len * i];
        tmpQuotaRescId =  &quotaRescId->value[quotaRescId->len * i];
        tmpQuotaUserId =  &quotaUserId->value[quotaUserId->len * i];
        tmpRescQuota = ( rescQuota_t* )malloc( sizeof( rescQuota_t ) );
        fillRescQuotaStruct( tmpRescQuota, tmpQuotaLimit, tmpQuotaOver,
                             tmpRescName, tmpQuotaRescId, tmpQuotaUserId );
        tmpRescQuota->next = *rescQuotaHead;
        *rescQuotaHead = tmpRescQuota;
    }

    return 0;
}

int fillRescQuotaStruct(
    rescQuota_t* rescQuota,
    char*        tmpQuotaLimit,
    char*        tmpQuotaOver,
    char*        tmpRescName,
    char*        tmpQuotaRescId,
    char*        tmpQuotaUserId ) {

    std::memset(rescQuota, 0, sizeof(rescQuota_t));

    rescQuota->quotaLimit = strtoll( tmpQuotaLimit, 0, 0 );
    rescQuota->quotaOverrun = strtoll( tmpQuotaOver, 0, 0 );

    if ( strtoll( tmpQuotaRescId, 0, 0 ) > 0 ) {
        /* quota by resource */
        rstrcpy( rescQuota->rescName, tmpRescName, NAME_LEN );
    }
    else {
        rescQuota->flags = GLOBAL_QUOTA;
    }

    rstrcpy( rescQuota->userId, tmpQuotaUserId, NAME_LEN );

    return 0;
}

int setRescQuota(
    rsComm_t*   _comm,
    const char* _obj_path,
    const char* _resc_name,
    rodsLong_t  _data_size ) {
    int status = chkRescQuotaPolicy( _comm );
    if ( status != RESC_QUOTA_ON ) {
        return 0;
    }

    rodsLong_t resc_id = 0;
    irods::error ret = resc_mgr.hier_to_leaf_id(_resc_name,resc_id);
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        return ret.code();
    }

    rodsLong_t resc_overrun = 0;
    ret = irods::get_resource_property<rodsLong_t>(resc_id, irods::RESOURCE_QUOTA_OVERRUN, resc_overrun);

    // KEY_NOT_FOUND is not an error here, since it just means the property is unset
    if (!ret.ok() && ret.code() != KEY_NOT_FOUND) {
        log_api::error(
            "{}: error in get_resource_property for RESOURCE_QUOTA_OVERRUN, status = [{}]", __func__, ret.code());
        return static_cast<int>(ret.code());
    }

    // fetch a global quota for the user if it exists
    getRescQuotaInp_t get_resc_quota_inp;
    std::memset(&get_resc_quota_inp, 0, sizeof(get_resc_quota_inp));
    rstrcpy(
        get_resc_quota_inp.zoneHint,
        _obj_path,
        MAX_NAME_LEN );
    snprintf(
        get_resc_quota_inp.userName,
        NAME_LEN, "%s#%s",
        _comm->clientUser.userName,
        _comm->clientUser.rodsZone );

    rstrcpy(
        get_resc_quota_inp.rescName,
        _resc_name,
        NAME_LEN );

    rescQuota_t *resc_quota = NULL;
    status = rsGetRescQuota(
                 _comm,
                 &get_resc_quota_inp,
                 &resc_quota );
    if( status < 0 || !resc_quota ) {
        return status;
    }

    rodsLong_t compare_overrun = resc_quota->quotaOverrun;

    freeAllRescQuota(resc_quota);
    if ( compare_overrun != 0 && // disabled if 0
         compare_overrun + resc_overrun + _data_size > 0 ) {
        return SYS_RESC_QUOTA_EXCEEDED;
    }

    return 0;

} // setRescQuota

int updatequotaOverrun(
    const char* _resc_hier,
    rodsLong_t  _data_size,
    int         _flags ) {
    if( ( _flags & GLB_QUOTA ) > 0 ) {
        GlobalQuotaOverrun += _data_size;
    }

    if( ( _flags & RESC_QUOTA ) > 0 ) {
        irods::hierarchy_parser parser;
        parser.set_string( _resc_hier );

        std::string root;
        parser.first_resc( root );

        rodsLong_t resc_id = 0;
        irods::error ret = resc_mgr.hier_to_leaf_id(root, resc_id);
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }

        rodsLong_t over_run = 0;
        ret = irods::get_resource_property<rodsLong_t>(
                               resc_id,
                               irods::RESOURCE_QUOTA_OVERRUN,
                               over_run );
        // property may not be set yet so skip error
        over_run += _data_size;
        ret = irods::set_resource_property<rodsLong_t>(
                  root,
                  irods::RESOURCE_QUOTA_OVERRUN,
                  over_run );
        if( !ret.ok() ) {
            irods::log(PASS(ret));
            return ret.code();
        }
    }
    return 0;
}

int chkRescQuotaPolicy( rsComm_t *rsComm ) {
    if ( RescQuotaPolicy == RESC_QUOTA_UNINIT ) {
        ruleExecInfo_t rei;
        initReiWithDataObjInp( &rei, rsComm, NULL );
        int status = applyRule(
                         "acRescQuotaPolicy",
                         NULL, &rei,
                         NO_SAVE_REI );
        clearKeyVal(rei.condInputData);
        free(rei.condInputData);
        if ( status < 0 ) {
            log_api::error("queRescQuota: acRescQuotaPolicy error status = {}", status);
            RescQuotaPolicy = RESC_QUOTA_OFF;
        }
    }
    return RescQuotaPolicy;
}
