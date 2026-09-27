#include "irods/administration_utilities.hpp"
#include "irods/authenticate.h"
#include "irods/catalog.hpp"
#include "irods/catalog_utilities.hpp"
#include "irods/checksum.h"
#include "irods/icatHighLevelRoutines.hpp"
#include "irods/icatStructs.hpp"
#include "irods/irods_auth_constants.hpp"
#include "irods/irods_auth_factory.hpp"
#include "irods/irods_auth_manager.hpp"
#include "irods/irods_auth_object.hpp"
#include "irods/irods_auth_plugin.hpp"
#include "irods/irods_children_parser.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_database_plugin.hpp"
#include "irods/irods_hierarchy_parser.hpp"
#include "irods/irods_lexical_cast.hpp"
#include "irods/irods_logger.hpp"
#include "irods/irods_pam_auth_object.hpp"
#include "irods/irods_random.hpp"
#include "irods/irods_resource_manager.hpp"
#include "irods/irods_rs_comm_query.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_stacktrace.hpp"
#include "irods/irods_virtual_path.hpp"
#include "irods/key_value_proxy.hpp"
#include "irods/miscServerFunct.hpp"
#include "irods/modAccessControl.h"
#include "irods/msParam.h"
#include "irods/private/irods_catalog_properties.hpp"
#include "irods/private/low_level.hpp"
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/nanodbc_executor.hpp"
#include "irods/private/database_session.hpp"
#include "irods/private/db_flavor_table.hpp"
#include "irods/private/catalog_access_control.hpp"
#include "irods/rcConnect.h"
#include "irods/rcMisc.h"
#include "irods/rods.h"
#include "irods/rodsDef.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsQuota.h"
#include "irods/user_validation_utilities.hpp"

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <nanodbc/nanodbc.h>
#include <nlohmann/json.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/date_time.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <locale>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// clang-format off
using log_db        = irods::experimental::log::database;
using log_sql       = irods::experimental::log::sql;
using leaf_bundle_t = irods::resource_manager::leaf_bundle_t;
namespace gq2       = irods::experimental::genquery2;
using gq2::builder::col;
// clang-format on

extern irods::resource_manager resc_mgr;

extern int get64RandomBytes( char *buf );
extern int icatApplyRule( rsComm_t *rsComm, char *ruleName, char *arg1 );

static char prevChalSig[200]; // A 'signature' of the previous challenge.
                              // This is used as a sessionSignature on the catalog provider server
                              // side. Also see getSessionSignatureClientside function.

// Legal values for accessLevel in chlModAccessControl (Access Parameter).
// Defined here since other code does not need them (except for help messages)
#define AP_READ  "read"
#define AP_WRITE "write"
#define AP_OWN   "own"
#define AP_NULL  "null"

/* TEMP_PASSWORD_TIME is the number of seconds the temporary, one-time
   password can be used.  chlCheckAuth also checks for this column
   to be < TEMP_PASSWORD_MAX_TIME (1000) to differentiate the row
   from regular passwords and PAM passwords.
   This time, 120 seconds, should be long enough to give the iDrop and
   iDrop-lite applets enough time to download and go through their
   startup sequence.  iDrop and iDrop-lite disconnect when idle to
   reduce the number of open connections and active agents.  */

#define PASSWORD_SCRAMBLE_PREFIX ".E_"
#define PASSWORD_KEY_ENV_VAR     "IRODS_DATABASE_USER_PASSWORD_SALT"
#define PASSWORD_DEFAULT_KEY     "a9_3fker"

#define MAX_HOST_STR             2700

size_t log_sql_flg = 0;
icatSessionStruct icss; // JMC :: only for testing!!!
extern int logSQL;

int  creatingUserByGroupAdmin;
char mySessionTicket[NAME_LEN];
char mySessionClientAddr[NAME_LEN];

// =-=-=-=-=-=-=-
// property constants
const std::string ICSS_PROP( "irods_icss_property" );
const std::string ZONE_PROP( "irods_zone_property" );

static const auto intermediate_replica_status_str = std::to_string(INTERMEDIATE_REPLICA);

namespace
{
    // This structure holds authentication configuration values.
    struct auth_config
    {
        static constexpr bool default_password_extend_lifetime = true;
        static constexpr rodsLong_t default_password_max_time = 1209600;
        static constexpr rodsLong_t default_password_min_time = 121;

        // Holds the value for the irods::KW_CFG_PAM_PASSWORD_EXTEND_LIFETIME configuration.
        bool password_extend_lifetime = default_password_extend_lifetime;

        // Holds the value for the irods::KW_CFG_PAM_PASSWORD_MAX_TIME configuration.
        rodsLong_t password_max_time = default_password_max_time;

        // Holds the value for the irods::KW_CFG_PAM_PASSWORD_MIN_TIME configuration.
        rodsLong_t password_min_time = default_password_min_time;
    };

    auto translate_nanodbc_error(const nanodbc::database_error& e) -> int
    {
        const std::string state = e.state();
        const std::string msg = e.what();

        if (state == "23505" || state == "23000" ||
            msg.find("duplicate key") != std::string::npos ||
            msg.find("Duplicate entry") != std::string::npos ||
            msg.find("unique constraint") != std::string::npos) {
            return CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME;
        }

        return CAT_SQL_ERR;
    }

    auto get_auth_config(const char* _namespace, auth_config& _out) -> irods::error
    {
        try {
            auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

            namespace gq2 = irods::experimental::genquery2;
            using gq2::builder::col;

            auto q_res = irods::experimental::catalog::execute_catalog(
                executor, db_conn,
                gq2::builder::select({"option_name", "option_value"})
                    .from("GRID_CONFIGURATION")
                    .where(col("namespace") == _namespace)
                    .build());

            if (q_res.query_result) {
                while (q_res.query_result->next()) {
                    const auto option_name = q_res.query_result->get<std::string>(0);
                    const auto option_value = q_res.query_result->get<std::string>(1);

                // Given the level of nesting that occurs here to maintain specificity in the error messages, the logic
                // has been condensed for reuse. The option_name still needs to be differentiated to apply the correct
                // setting, and given that there are only two alternatives for this case, it is sufficient to use only
                // one for a boolean flag. Therefore, check for the password_min_time option_name once to determine
                // which option_name this is and avoid duplicating the string comparison.
                if (const bool is_min_option = (option_name == irods::KW_CFG_PAM_PASSWORD_MIN_TIME);
                    is_min_option || option_name == irods::KW_CFG_PAM_PASSWORD_MAX_TIME)
                {
                    const auto default_value =
                        is_min_option ? auth_config::default_password_min_time : auth_config::default_password_max_time;
                    auto& config = is_min_option ? _out.password_min_time : _out.password_max_time;
                    try {
                        config = std::stoll(option_value);
                        if (config < 0) {
                            config = default_value;
                            log_db::warn("Invalid R_GRID_CONFIGURATION value. namespace:[{}], option:[{}], value:[{}]. "
                                         "Using default value [{}].",
                                         _namespace,
                                         option_name,
                                         option_value,
                                         config);
                        }
                    }
                    catch (const std::exception& e) {
                        config = default_value;
                        log_db::warn("Error occurred getting R_GRID_CONFIGURATION value. namespace:[{}], option:[{}], "
                                     "value:[{}]. Using default value [{}]. error:[{}]",
                                     _namespace,
                                     option_name,
                                     option_value,
                                     config,
                                     e.what());
                    }
                    catch (...) {
                        config = default_value;
                        log_db::warn("Error occurred getting R_GRID_CONFIGURATION value. namespace:[{}], option:[{}], "
                                     "value:[{}]. Using default value [{}]. error:[Unknown error]",
                                     _namespace,
                                     option_name,
                                     option_value,
                                     config);
                    }
                    continue;
                }

                if (option_name == irods::KW_CFG_PAM_PASSWORD_EXTEND_LIFETIME) {
                    if (option_value == "1") {
                        _out.password_extend_lifetime = true;
                    }
                    else if (option_value == "0") {
                        _out.password_extend_lifetime = false;
                    }
                    else {
                        _out.password_extend_lifetime = auth_config::default_password_extend_lifetime;
                        log_db::warn("Invalid R_GRID_CONFIGURATION value. namespace:[{}], option:[{}], value:[{}]. "
                                     "Using default value [{}].",
                                     _namespace,
                                     option_name,
                                     option_value,
                                     _out.password_extend_lifetime);
                    }
                    continue;
                }
            }
        }
        }
        catch (const std::exception& e) {
            return ERROR(
                SYS_LIBRARY_ERROR,
                fmt::format(
                    "Error occurred getting grid configurations from R_GRID_CONFIGURATION namespace [{}]. error:[{}]",
                    _namespace,
                    e.what()));
        }
        catch (...) {
            return ERROR(
                SYS_UNKNOWN_ERROR,
                fmt::format(
                    "Unknown error occurred getting grid configurations from R_GRID_CONFIGURATION namespace [{}].",
                    _namespace));
        }

        return SUCCESS();
    } // get_auth_config
} // anonymous namespace

// =-=-=-=-=-=-=-
// virtual path management
#define PATH_SEPARATOR irods::get_virtual_path_separator().c_str()

// Returns the current time as a pair of strings.
//
// The first string represents the number of seconds since the epoch. It will be left-padded
// with zeros and have a minimum width of 11 characters.
//
// The second string represents the fractional part of a second in milliseconds. It will be
// left-padded with zeros and have an exact width of 3 characters.
auto get_current_time() -> std::pair<std::string, std::string>
{
    using std::chrono::duration_cast;

    const auto now = std::chrono::system_clock::now();
    const auto secs = duration_cast<std::chrono::seconds>(now.time_since_epoch());
    const auto millis = duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) - secs;

    return {fmt::format("{:011}", secs.count()), fmt::format("{:03}", millis.count())};
} // get_current_time

/*
   Parse the input fullUserNameIn into an output userName and userZone
   and check that the username is a valid format, meaning at most one
   '@' and at most one '#'.
   Full userNames are of the form user@department[#zone].
   It is assumed the output strings are at least NAME_LEN characters long.
 */
int
validateAndParseUserName( const char *fullUserNameIn, char *userName, char *userZone ) {
    const auto user_and_zone_name = irods::user::validate_name(fullUserNameIn);
    if (!user_and_zone_name) {
        if (userName) {
            userName[0] = '\0';
        }

        if (userZone) {
            userZone[0] = '\0';
        }

        return USER_INVALID_USERNAME_FORMAT;
    }

    const auto& [user_name, zone_name] = *user_and_zone_name;

    if (userName) {
        std::strncpy(userName, user_name.data(), NAME_LEN);
    }

    if (userZone) {
        std::strncpy(userZone, zone_name.data(), NAME_LEN);
    }

    return 0;
} // validateAndParseUserName

[[nodiscard]] auto is_valid_avu(const char* _attribute, const char* _value, const char* _unit) noexcept -> bool
{
    if (_attribute == nullptr || _value == nullptr || _unit == nullptr) {
        return false;
    }
    if (*_attribute == '\0' || *_value == '\0') {
        return false;
    }

    return true;
}

namespace {

int db_execute_no_answer_sql( const char *sql, icatSessionStruct *_icss = &icss ) {
    int i = cllExecSqlNoResult( _icss, sql );
    if ( i != 0 ) {
        if ( i <= CAT_ENV_ERR ) {
            return i;
        }
        return CAT_SQL_ERR;
    }
    return 0;
}

int db_free_statement( int statementNumber, icatSessionStruct *_icss = &icss ) {
    return cllFreeStatement( _icss, statementNumber );
}

int db_get_first_row_from_sql( const char *sql, int *statement, int skipCount,
                               std::vector<std::string>& bindVars, icatSessionStruct *_icss = &icss ) {
    int i = cllExecSqlWithResultBV( _icss, statement, sql, bindVars );
    if ( i != 0 ) {
        cllFreeStatement( _icss, *statement );
        *statement = UNINITIALIZED_STATEMENT_NUMBER;
        if ( i <= CAT_ENV_ERR ) {
            return i;
        }
        return CAT_SQL_ERR;
    }

    const auto& flavor = irods::experimental::catalog::get_db_flavor( _icss->databaseType );
    if ( flavor.row_offset_requires_cursor_skip && skipCount > 0 ) {
        for ( int j = 0; j < skipCount; ++j ) {
            i = cllGetRow( _icss, *statement );
            if ( i != 0 ) {
                cllFreeStatement( _icss, *statement );
                *statement = UNINITIALIZED_STATEMENT_NUMBER;
                return CAT_GET_ROW_ERR;
            }
            if ( _icss->stmtPtr[*statement]->numOfCols == 0 ) {
                cllFreeStatement( _icss, *statement );
                *statement = UNINITIALIZED_STATEMENT_NUMBER;
                return CAT_NO_ROWS_FOUND;
            }
        }
    }

    i = cllGetRow( _icss, *statement );
    if ( i != 0 ) {
        cllFreeStatement( _icss, *statement );
        *statement = UNINITIALIZED_STATEMENT_NUMBER;
        return CAT_GET_ROW_ERR;
    }
    if ( _icss->stmtPtr[*statement]->numOfCols == 0 ) {
        cllFreeStatement( _icss, *statement );
        *statement = UNINITIALIZED_STATEMENT_NUMBER;
        return CAT_NO_ROWS_FOUND;
    }

    return 0;
}

int db_get_next_row_from_statement( int stmtNum, icatSessionStruct *_icss = &icss ) {
    if ( 0 != cllGetRow( _icss, stmtNum ) ) {
        cllFreeStatement( _icss, stmtNum );
        return CAT_GET_ROW_ERR;
    }
    if ( _icss->stmtPtr[stmtNum]->numOfCols == 0 ) {
        cllFreeStatement( _icss, stmtNum );
        return CAT_NO_ROWS_FOUND;
    }
    return 0;
}

int db_open_connection( icatSessionStruct *_icss ) {
    for ( int i = 0; i < MAX_NUM_OF_CONCURRENT_STMTS; ++i ) {
        _icss->stmtPtr[i] = nullptr;
    }

    if ( _icss->database_plugin_type[0] != '\0' ) {
        _icss->databaseType = irods::experimental::catalog::get_db_type_from_name( _icss->database_plugin_type );
    }
    else {
        _icss->databaseType = DB_TYPE_POSTGRES;
    }

    int i = cllOpenEnv( _icss );
    if ( i != 0 ) {
        return CAT_ENV_ERR;
    }

    i = cllConnect( _icss );
    if ( i != 0 ) {
        return CAT_CONNECT_ERR;
    }

    return 0;
}

int db_close_connection( icatSessionStruct *_icss ) {
    static int pending = 0;
    if ( pending == 1 ) {
        return 0;
    }
    pending = 1;

    int status = cllDisconnect( _icss );
    int stat2 = cllCloseEnv( _icss );

    pending = 0;
    if ( status ) {
        return CAT_DISCONNECT_ERR;
    }
    if ( stat2 ) {
        return CAT_CLOSE_ENV_ERR;
    }
    return 0;
}

} // anonymous namespace

// =-=-=-=-=-=-=-
//  Called internally to rollback current transaction after an error.
int _rollback( const char *functionName ) {
    // =-=-=-=-=-=-=-
    // This type of rollback is needed for Postgres since the low-level
    // now does an automatic 'begin' to create a sql block */
    int status =  db_execute_no_answer_sql( "rollback", &icss );
    if ( status == 0 ) {
        log_db::info("{} rollback succeeded", functionName);
    }
    else {
        log_db::info("{} rollback failure {}", functionName, status);
    }

    return status;

} // _rollback

// =-=-=-=-=-=-=-
//  Internal function to return the local zone (which is the default
//  zone).  The first time it's called, it gets the zone from the DB and
//  subsequent calls just return that value.
irods::error getLocalZone(
    irods::plugin_property_map& _prop_map,
    icatSessionStruct*          _icss,
    std::string&                _zone ) {
    // =-=-=-=-=-=-=-
    // try to get the zone prop, if it is not cached
    // then we hit the catalog and request it
    irods::error ret = _prop_map.get< std::string >( ZONE_PROP, _zone );
    if ( !ret.ok() ) {
        try {
            auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
            const auto zone_opt = irods::experimental::catalog::query_catalog_string(
                executor,
                db_conn,
                gq2::builder::select({"zone_name"})
                    .from("ZONE")
                    .where(col("zone_type_name") == "local")
                    .build());
            if (!zone_opt) {
                return ERROR(CAT_NO_ROWS_FOUND, "getLocalZone: local zone not found");
            }
            _zone = *zone_opt;
            ret = _prop_map.set< std::string >( ZONE_PROP, _zone );
            if ( !ret.ok() ) {
                return PASS( ret );
            }
        }
        catch (const nanodbc::database_error& e) {
            log_db::error("{}: database error: {}", __FUNCTION__, e.what());
            return ERROR(CAT_SQL_ERR, e.what());
        }
        catch (const std::exception& e) {
            log_db::error("{}: exception: {}", __FUNCTION__, e.what());
            return ERROR(SYS_INTERNAL_ERR, e.what());
        }
    } // if no zone prop

    return SUCCESS();

} // getLocalZone

// =-=-=-=-=-=-=-
// @brief query for object found of a resource
int get_object_count_of_resource_by_name(
    icatSessionStruct* _icss,
    const std::string& _resc_name,
    rodsLong_t&         _count ) {

    rodsLong_t resc_id;
    irods::error ret = resc_mgr.hier_to_leaf_id(
                         _resc_name,
                         resc_id);
    if(!ret.ok()) {
        // if we have a bad resource in the database we need
        // to ignore this in order to still remove it
        if(SYS_RESC_DOES_NOT_EXIST == ret.code()) {
            _count = 0;
            return 0;
        }
        log_db::error(PASS(ret).result());
        return ret.code();
    }

    std::string resc_id_str;
    ret = irods::lexical_cast<std::string>(resc_id, resc_id_str);
    if(!ret.ok()) {
        log_db::error(PASS(ret).result());
        return ret.code();
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        auto count_opt = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({gq2::builder::count("data_id")})
                .from("DATA_OBJECT")
                .where(col("resc_id") == resc_id_str)
                .build());
        _count = count_opt.value_or(0);
        return 0;
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return SYS_INTERNAL_ERR;
    }
} // get_object_count_of_resource_by_name

// remove AVU (user defined metadata) for an object, the metadata mapping information, if any.
int removeMetaMapAndAVU(const char* _id)
{
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto stmt = gq2::builder::remove_from("METADATA_MAP")
            .where(col("object_id") == _id)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
        return 0;
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return SYS_INTERNAL_ERR;
    }
} // removeMetaMapAndAVU

/*
 * removeAVUs - remove unused AVUs (user defined metadata), if any.
 */
static int removeAVUs() {
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        const auto& flavor = irods::experimental::catalog::get_db_flavor(icss.databaseType);
        executor.execute_dml(db_conn, flavor.remove_unused_avus_sql);

        trans.commit();
        return 0;
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return SYS_INTERNAL_ERR;
    }
}

int
_canConnectToCatalog(
    rsComm_t* _rsComm ) {
    int result = 0;
    if ( !icss.status ) {
        result = CATALOG_NOT_CONNECTED;
    }
    else if ( _rsComm->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        result = CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
    }
    else if ( _rsComm->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        result = CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
    }
    return result;
}

static
int hostname_resolves_to_ipv4(const char* _hostname) {
    struct addrinfo hint;
    memset(&hint, 0, sizeof(hint));
    hint.ai_family = AF_INET;
    struct addrinfo *p_addrinfo;
    const int ret_getaddrinfo_with_retry = getaddrinfo_with_retry(_hostname, 0, &hint, &p_addrinfo);
    if (ret_getaddrinfo_with_retry) {
        return ret_getaddrinfo_with_retry;
    }
    freeaddrinfo(p_addrinfo);
    return 0;
}


int
_resolveHostName(rsComm_t* _rsComm, const char* _hostAddress) {
    const int status = hostname_resolves_to_ipv4(_hostAddress);

    if ( status != 0 ) {
        addRErrorMsg(
            &_rsComm->rError,
            0,
            fmt::format("Warning, resource host address '{}' is not a valid DNS entry, hostname_resolves_to_ipv4 failed.", _hostAddress).c_str() );
    }
    if ( strcmp( _hostAddress, "localhost" ) == 0 ) {
        addRErrorMsg( &_rsComm->rError, 0,
                      "Warning, resource host address 'localhost' will not work properly as it maps to the local host from each client." );
    }

    return 0;
}

// Returns success if path is not root; otherwise, generates an error
irods::error
verify_non_root_vault_path(irods::plugin_context& _ctx, const std::string& path) {
    if (0 == path.compare("/")) {
        const std::string error_message = "root directory cannot be used as vault path.";
        addRErrorMsg(&_ctx.comm()->rError, 0, error_message.c_str());
        return ERROR(CAT_INVALID_RESOURCE_VAULT_PATH, error_message.c_str() );
    }
    return SUCCESS();
}

// =-=-=-=-=-=-=-
//
irods::error _childIsValid(
    irods::plugin_property_map& _prop_map,
    const std::string&          _new_child ) {
    // Get the resource name from the child string
    std::string resc_name;
    irods::children_parser parser;
    parser.set_string( _new_child );
    parser.first_child( resc_name );

    std::string zone;
    irods::error ret = getLocalZone( _prop_map, &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto q_res = irods::experimental::catalog::execute_catalog(
            executor, db_conn,
            gq2::builder::select({"resc_parent"})
                .from("RESOURCE")
                .where(col("resc_name") == resc_name && col("zone_name") == zone)
                .build());

        if (!q_res.query_result || !q_res.query_result->next()) {
            log_db::info("{}: Child resource [{}] not found", __func__, resc_name);
            return ERROR( CHILD_NOT_FOUND, "child resource not found" );
        }

        if (!q_res.query_result->is_null(0)) {
            const auto parent = q_res.query_result->get<std::string>(0);
            if (!parent.empty()) {
                log_db::info("{}: Child resource [{}] already has a parent [{}]", __func__, resc_name, parent);
                return ERROR( CHILD_HAS_PARENT, "child resource already has a parent" );
            }
        }

        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
}

irods::error update_child_parent(const std::string& _child_resc_id,
                                 const std::string& _parent_resc_id,
                                 const std::string& _parent_child_context)
{
    const auto [current_time_secs, current_time_msecs] = get_current_time();

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto stmt = gq2::builder::update("RESOURCE")
            .set("resc_parent", _parent_resc_id)
            .set("resc_parent_context", _parent_child_context)
            .set("modify_ts", current_time_secs)
            .set("modify_ts_millis", current_time_msecs)
            .where(col("resc_id") == _child_resc_id)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // update_child_parent

/**
 * @brief Returns true if the specified resource has associated data objects
 */
int
_rescHasData(
    icatSessionStruct* _icss,
    const std::string& _resc_name,
    bool&              _has_data ) {
    rodsLong_t obj_count{};

    int status = get_object_count_of_resource_by_name(
                      _icss,
                      _resc_name,
                      obj_count );
    if( 0 == status ) {
        if ( 0 == obj_count ) {
            _has_data = false;
        }
    }

    return status;
}

/// @brief function for validating a resource name
irods::error validate_resource_name( std::string _resc_name ) {

    // Must be between 1 and NAME_LEN-1 characters.
    // Must start and end with a word character.
    // May contain non consecutive dashes.
    boost::regex re( "^(?=.{1,63}$)\\w+(-\\w+)*$" );

    if ( !boost::regex_match( _resc_name, re ) ) {
        std::stringstream msg;
        msg << "validate_resource_name failed for resource [";
        msg << _resc_name;
        msg << "]";
        return ERROR( SYS_INVALID_INPUT_PARAM, msg.str() );
    }

    return SUCCESS();

} // validate_resource_name

// Not super robust
static inline auto validate_zone_connection_string(const char* _zone_conn_info, irods::plugin_context& _ctx)
    -> irods::error
{
    const std::size_t addr_len = std::strlen(_zone_conn_info);
    if (addr_len > 0) {
        // _zone_conn_info is const, so copy it
        std::vector<char> addr_buf(addr_len + 1, '\0');
        std::strncpy(addr_buf.data(), _zone_conn_info, addr_buf.size());
        rodsHostAddr_t addr{};
        auto status = parseHostAddrStr(addr_buf.data(), &addr);
        if (status < 0) {
            std::string errmsg = fmt::format("failed to validate zone connection info [{}]", status);
            log_db::error(errmsg);
            addRErrorMsg(&_ctx.comm()->rError, status, errmsg.c_str());
            return ERROR(status, errmsg);
        }
        const std::size_t host_len = std::strlen(addr.hostAddr);
        if (host_len == 0 || (host_len == 1 && addr.hostAddr[0] == ':')) {
            std::string errmsg =
                fmt::format("failed to validate zone connection info due to empty hostname [{}]", _zone_conn_info);
            log_db::error(errmsg);
            addRErrorMsg(&_ctx.comm()->rError, CAT_HOSTNAME_INVALID, errmsg.c_str());
            return ERROR(CAT_HOSTNAME_INVALID, errmsg);
        }
        if ((addr.portNum > 65535) || (addr.portNum <= 0)) {
            std::string errmsg = fmt::format(
                "failed to validate zone connection info due to invalid or unspecified port [{}]", _zone_conn_info);
            log_db::error(errmsg);
            addRErrorMsg(&_ctx.comm()->rError, CAT_INVALID_ARGUMENT, errmsg.c_str());
            return ERROR(CAT_INVALID_ARGUMENT, errmsg);
        }
    }
    return SUCCESS();
}

bool
_rescHasParentOrChild( const char* rescId ) {
    if (!rescId || *rescId == '\0') {
        return false;
    }
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto parent = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"resc_parent"}).from("RESOURCE").where(col("resc_id") == rescId).build());
        if (parent && !parent->empty()) {
            return true;
        }
        const auto child = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"resc_id"}).from("RESOURCE").where(col("resc_parent") == rescId).build());
        if (child && !child->empty()) {
            return true;
        }
        return false;
    }
    catch (const std::exception& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return false;
    }
}

bool _userInRUserAuth( const char* userName, const char* zoneName, const char* auth_name ) {
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        const auto uid_opt = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName && col("zone_name") == zoneName)
                .build());
        if (!uid_opt) {
            return false;
        }
        const auto opt = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER_AUTH")
                .where(col("user_id") == std::to_string(*uid_opt) && col("user_auth_name") == auth_name)
                .build());
        return opt.has_value();
    }
    catch (const std::exception& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return false;
    }
}

/* delCollection (internally called),
   does not do the commit.
*/
static int _delColl( rsComm_t *rsComm, collInfo_t *collInfo ) {
    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];
    char collIdNum[MAX_NAME_LEN];

    if (const auto ec = splitPathByKey(collInfo->collName, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
        log_db::error(
            "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]", __func__, __LINE__, collInfo->collName, ec);
        return ec;
    }

    if ( strlen( logicalParentDirName ) == 0 ) {
        snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
        snprintf( logicalEndName, sizeof( logicalEndName ), "%s", collInfo->collName + 1 );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        // Check that the parent collection exists and user has write permission
        auto parent_status = irods::experimental::catalog::access_control::check_collection_access(
            executor, db_conn, logicalParentDirName, rsComm->clientUser.userName, rsComm->clientUser.rodsZone, ACCESS_MODIFY_OBJECT);
        if ( parent_status < 0 ) {
            if ( parent_status == CAT_UNKNOWN_COLLECTION ) {
                addRErrorMsg( &rsComm->rError, 0, fmt::format("collection '{}' is unknown", logicalParentDirName).c_str() );
            }
            return parent_status;
        }

        // Check that the collection exists and user has DELETE permission
        auto coll_status = irods::experimental::catalog::access_control::check_collection_access(
            executor, db_conn, collInfo->collName, rsComm->clientUser.userName, rsComm->clientUser.rodsZone, ACCESS_DELETE_OBJECT);
        if ( coll_status < 0 ) {
            return coll_status;
        }
        snprintf( collIdNum, MAX_NAME_LEN, "%lld", static_cast<long long>(coll_status) );

        // Check that the collection is empty (both subdirs and files)
        const auto subcoll = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("parent_coll_name") == collInfo->collName)
                .build());
        if ( subcoll.has_value() ) {
            return CAT_COLLECTION_NOT_EMPTY;
        }

        const auto data_obj = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"data_id"})
                .from("DATA_OBJECT")
                .where(col("coll_id") == collIdNum)
                .build());
        if ( data_obj.has_value() ) {
            return CAT_COLLECTION_NOT_EMPTY;
        }

        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto del_coll = gq2::builder::remove_from("COLLECTION")
            .where(col("coll_name") == collInfo->collName && col("coll_id") == collIdNum)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_coll);

        auto del_access = gq2::builder::remove_from("ACCESS")
            .where(col("object_id") == collIdNum)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_access);

        if (const auto ec = removeMetaMapAndAVU(collIdNum); ec < 0) {
            log_db::warn("[{}:{}] - failed to remove associated AVUs [ec=[{}]]", __func__, __LINE__, ec);
        }

        trans.commit();
        return 0;
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return SYS_INTERNAL_ERR;
    }
} // _delColl

/*
   Possibly descramble a password (for user passwords stored in the ICAT).
   Called internally, from various chl functions.
*/
static int
icatDescramble( char *pw ) {
    char *cp1, *cp2, *cp3;
    int i, len;
    char pw2[MAX_PASSWORD_LEN + 10];
    char unscrambled[MAX_PASSWORD_LEN + 10];

    len = strlen( PASSWORD_SCRAMBLE_PREFIX );
    cp1 = pw;
    cp2 = PASSWORD_SCRAMBLE_PREFIX; /* if starts with this, it is scrambled */
    for ( i = 0; i < len; i++ ) {
        if ( *cp1++ != *cp2++ ) {
            return 0;                /* not scrambled, leave as is */
        }
    }
    snprintf( pw2, sizeof( pw2 ), "%s", cp1 );
    cp3 = getenv( PASSWORD_KEY_ENV_VAR );
    if ( cp3 == NULL ) {
        cp3 = PASSWORD_DEFAULT_KEY;
    }
    obfDecodeByKey( pw2, cp3, unscrambled );
    strncpy( pw, unscrambled, MAX_PASSWORD_LEN );

    return 0;
}

/*
   Scramble a password (for user passwords stored in the ICAT).
   Called internally.
*/
static int
icatScramble( char *pw ) {
    char *cp1;
    char newPw[MAX_PASSWORD_LEN + 10];
    char scrambled[MAX_PASSWORD_LEN + 10];

    cp1 = getenv( PASSWORD_KEY_ENV_VAR );
    if ( cp1 == NULL ) {
        cp1 = PASSWORD_DEFAULT_KEY;
    }
    obfEncodeByKey( pw, cp1, scrambled );
    snprintf( newPw, sizeof( newPw ), "%s%s", PASSWORD_SCRAMBLE_PREFIX, scrambled );
    strncpy( pw, newPw, MAX_PASSWORD_LEN );
    return 0;
}

/*
  de-scramble a password sent from the client.
  This isn't real encryption, but does obfuscate the pw on the network.
  Called internally, from chlModUser.
*/
int decodePw( rsComm_t *rsComm, const char *in, char *out ) {
    char *cp;
    char password[MAX_PASSWORD_LEN]{};
    char upassword[MAX_PASSWORD_LEN + 10];
    char rand[] =
        "1gCBizHWbwIYyWLo";  /* must match clients */
    int pwLen1, pwLen2;

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto uid_opt = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == rsComm->clientUser.userName && col("zone_name") == rsComm->clientUser.rodsZone)
                .build());
        if (!uid_opt) {
            return CAT_INVALID_USER;
        }

        const auto pwd_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"rcat_password"})
                .from("USER_PASSWORD")
                .where(col("user_id") == std::to_string(*uid_opt))
                .build());
        if (!pwd_opt) {
            return CAT_INVALID_USER;
        }
        rstrcpy(password, pwd_opt->c_str(), sizeof(password));
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return SYS_INTERNAL_ERR;
    }

    icatDescramble( password );

    obfDecodeByKeyV2( in, password, prevChalSig, upassword );

    pwLen1 = strlen( upassword );

    memset( password, 0, MAX_PASSWORD_LEN );

    cp = strstr( upassword, rand );
    if ( cp != NULL ) {
        *cp = '\0';
    }

    pwLen2 = strlen( upassword );

    if ( pwLen2 > MAX_PASSWORD_LEN - 5 && pwLen2 == pwLen1 ) {
        /* probable failure */
        addRErrorMsg(
            &rsComm->rError,
            0,
            "Error with password encoding.  This can be caused by not connecting directly to the ICAT host, not using password authentication (using GSI or Kerberos instead), or entering your password incorrectly (if prompted)." );
        return CAT_PASSWORD_ENCODING_ERROR;
    }
    strcpy( out, upassword );
    memset( upassword, 0, MAX_PASSWORD_LEN );

    return 0;
}

int
convertTypeOption( const char *typeStr ) {
    if ( strcmp( typeStr, "-d" ) == 0 ) {
        return ( 1 );   /* dataObj */
    }
    if ( strcmp( typeStr, "-D" ) == 0 ) {
        return ( 1 );   /* dataObj */
    }
    if ( strcmp( typeStr, "-c" ) == 0 ) {
        return ( 2 );   /* collection */
    }
    if ( strcmp( typeStr, "-C" ) == 0 ) {
        return ( 2 );   /* collection */
    }
    if ( strcmp( typeStr, "-r" ) == 0 ) {
        return ( 3 );   /* resource */
    }
    if ( strcmp( typeStr, "-R" ) == 0 ) {
        return ( 3 );   /* resource */
    }
    if ( strcmp( typeStr, "-u" ) == 0 ) {
        return ( 4 );   /* user */
    }
    if ( strcmp( typeStr, "-U" ) == 0 ) {
        return ( 4 );   /* user */
    }
    return 0;
}

/*
  Check object - get an object's ID and check that the user has access.
  Called internally.
*/
rodsLong_t checkAndGetObjectId(
    rsComm_t*                   rsComm,
    irods::plugin_property_map& prop_map,
    const char*                 type,
    const char*                 name,
    const char*                 access,
    bool                        admin_mode = false)
{
    if (admin_mode && !irods::is_privileged_client(*rsComm)) {
        return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
    }

    int itype;
    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];
    rodsLong_t status;
    rodsLong_t objId;
    char userName[NAME_LEN];
    char userZone[NAME_LEN];

    log_sql::debug("checkAndGetObjectId");

    if ( !icss.status ) {
        return CATALOG_NOT_CONNECTED;
    }

    if ( type == NULL ) {
        return CAT_INVALID_ARGUMENT;
    }

    if ( *type == '\0' ) {
        return CAT_INVALID_ARGUMENT;
    }


    if ( name == NULL ) {
        return CAT_INVALID_ARGUMENT;
    }

    if ( *name == '\0' ) {
        return CAT_INVALID_ARGUMENT;
    }


    itype = convertTypeOption( type );
    if ( itype == 0 ) {
        return CAT_INVALID_ARGUMENT;
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        if ( itype == 1 ) {
            if (const auto ec = splitPathByKey(name, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
                log_db::error("[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]", __func__, __LINE__, name, ec);
                return ec;
            }
            if ( strlen( logicalParentDirName ) == 0 ) {
                snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
                snprintf( logicalEndName, sizeof( logicalEndName ), "%s", name );
            }
            log_sql::debug("checkAndGetObjectId SQL 1 ");
            status = irods::experimental::catalog::access_control::check_data_object_only(
                executor, db_conn,
                logicalParentDirName, logicalEndName,
                rsComm->clientUser.userName,
                rsComm->clientUser.rodsZone,
                access, admin_mode );
            if ( status < 0 ) {
                _rollback( "checkAndGetObjectId" );
                return status;
            }
            objId = status;
        }

        if ( itype == 2 ) {
            /* Check that the collection exists and user has create_metadata permission,
               and get the collectionID */
            log_sql::debug("checkAndGetObjectId SQL 2");
            status = irods::experimental::catalog::access_control::check_collection_access(
                executor, db_conn,
                name,
                rsComm->clientUser.userName,
                rsComm->clientUser.rodsZone,
                access, admin_mode );
            if ( status < 0 ) {
                if ( status == CAT_UNKNOWN_COLLECTION ) {
                    addRErrorMsg( &rsComm->rError, 0, fmt::format("collection '{}' is unknown", name).c_str() );
                }
                return status;
            }
            objId = status;
        }

        if ( itype == 3 ) {
            if ( rsComm->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
                return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
            }

            std::string zone;
            irods::error ret = getLocalZone( prop_map, &icss, zone );
            if ( !ret.ok() ) {
                return PASS( ret ).code();
            }

            objId = 0;
            log_sql::debug("checkAndGetObjectId SQL 3");
            auto opt_id = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"resc_id"})
                    .from("RESOURCE")
                    .where(col("resc_name") == name && col("zone_name") == zone)
                    .build());
            if ( !opt_id ) {
                return CAT_INVALID_RESOURCE;
            }
            objId = *opt_id;
        }

        if ( itype == 4 ) {
            if ( rsComm->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
                return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
            }

            status = validateAndParseUserName( name, userName, userZone );
            if ( status ) {
                return status;
            }
            if ( userZone[0] == '\0' ) {
                std::string zone;
                irods::error ret = getLocalZone( prop_map, &icss, zone );
                if ( !ret.ok() ) {
                    return PASS( ret ).code();
                }
                snprintf( userZone, sizeof( userZone ), "%s",  zone.c_str() );
            }

            objId = 0;
            log_sql::debug("checkAndGetObjectId SQL 4");
            auto opt_id = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == userName && col("zone_name") == userZone)
                    .build());
            if ( !opt_id ) {
                return CAT_INVALID_USER;
            }
            objId = *opt_id;
        }
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __func__, e.what());
        _rollback( "checkAndGetObjectId" );
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __func__, e.what());
        _rollback( "checkAndGetObjectId" );
        return SYS_INTERNAL_ERR;
    }

    return objId;
}

// Find existing AVU triplet.
// Return code is error or the AVU ID.
rodsLong_t
findAVU( const char *attribute, const char *value, const char *units ) {
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        std::optional<int64_t> opt_id;
        if ( units != nullptr && *units != '\0' ) {
            log_sql::debug("findAVU SQL 1");
            opt_id = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"meta_id"})
                    .from("METADATA")
                    .where(col("meta_attr_name") == (attribute ? attribute : "") &&
                           col("meta_attr_value") == (value ? value : "") &&
                           col("meta_attr_unit") == units)
                    .build());
        }
        else {
            log_sql::debug("findAVU SQL 2");
            opt_id = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"meta_id"})
                    .from("METADATA")
                    .where(col("meta_attr_name") == (attribute ? attribute : "") &&
                           col("meta_attr_value") == (value ? value : "") &&
                           (col("meta_attr_unit") == "" || col("meta_attr_unit").is_null()))
                    .build());
        }

        if (opt_id) {
            return *opt_id;
        }
        return CAT_NO_ROWS_FOUND;
    }
    catch (const std::exception& e) {
        log_db::error("findAVU failed with exception: {}", e.what());
        return CAT_SQL_ERR;
    }
}

/*
  Find existing or insert a new AVU triplet.
  Return code is error, or the AVU ID.
*/
int
findOrInsertAVU( const char *attribute, const char *value, const char *units ) {
    char nextStr[MAX_NAME_LEN];
    char myTime[50];
    rodsLong_t seqNum;
    rodsLong_t iVal;
    iVal = findAVU( attribute, value, units );
    if ( iVal > 0 ) {
        return iVal;
    }
    log_sql::debug("findOrInsertAVU SQL 1");

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
        if ( seqNum < 0 ) {
            log_db::info("findOrInsertAVU get_next_sequence_value failure {}", seqNum);
            return seqNum;
        }

        snprintf( nextStr, sizeof nextStr, "%lld", seqNum );
        getNowStr( myTime );

        log_sql::debug("findOrInsertAVU SQL 2");
        namespace gq2 = irods::experimental::genquery2;
        auto ins_stmt = gq2::builder::insert_into("METADATA")
            .set("meta_id", nextStr)
            .set("meta_attr_name", attribute ? attribute : "")
            .set("meta_attr_value", value ? value : "")
            .set("meta_attr_unit", units ? units : "")
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
        return seqNum;
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return SYS_INTERNAL_ERR;
    }
}


/* create a path name with escaped SQL special characters (% and _) */
std::string
makeEscapedPath( const std::string &inPath ) {
    return boost::regex_replace( inPath, boost::regex( "[%_\\\\]" ), "\\\\$&" );
}

/* Internal routine to modify inheritance */
/* inheritFlag =1 to set, 2 to remove */
int _modInheritance( int inheritFlag, int recursiveFlag, const char *collIdStr, const char *pathName ) {

    const char* newValue = inheritFlag == 1 ? "1" : "0";

    char myTime[50];
    getNowStr( myTime );

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        if ( recursiveFlag == 0 ) {
            auto upd = gq2::builder::update("COLLECTION")
                .set("coll_inheritance", newValue)
                .set("modify_ts", myTime)
                .where(col("coll_id") == collIdStr)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
        }
        else {
            std::string pathStart = makeEscapedPath( pathName ) + "/%";
            auto upd = gq2::builder::update("COLLECTION")
                .set("coll_inheritance", newValue)
                .set("modify_ts", myTime)
                .where(col("coll_name") == pathName || col("coll_name").like(pathStart))
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
        }

        trans.commit();
        return 0;
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return CAT_SQL_ERR;
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return SYS_INTERNAL_ERR;
    }
}

/*
  Set the over_quota values (if any) using the limits and
  and the current usage; handling the various types: per-user per-resource,
  per-user total-usage, group per-resource, and group-total.

  The over_quota column is positive if over_quota and the negative value
  indicates how much space is left before reaching the quota.
*/
int setOverQuota( rsComm_t *rsComm ) {
    log_sql::debug("setOverQuota");
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        char myTime[50]{};
        getNowStr( myTime );

        // 1. Fetch all configured quotas from R_QUOTA_MAIN
        auto quota_res = irods::experimental::catalog::execute_catalog(
            executor, db_conn,
            gq2::builder::select({"user_id", "resc_id", "quota_limit", "quota_over"})
                .from("QUOTA")
                .build());

        if (!quota_res.query_result) {
            return 0;
        }

        struct QuotaRow {
            std::string user_id;
            std::string resc_id;
            int64_t quota_limit = 0;
            int64_t quota_over = 0;
        };

        std::vector<QuotaRow> quotas;
        while (quota_res.query_result->next()) {
            quotas.push_back({
                quota_res.query_result->get<std::string>(0),
                quota_res.query_result->get<std::string>(1),
                quota_res.query_result->get<int64_t>(2, 0),
                quota_res.query_result->get<int64_t>(3, 0)
            });
        }

        if (quotas.empty()) {
            return 0; // no quotas, done
        }

        // Cache user types (user vs rodsgroup) and group memberships
        std::unordered_map<std::string, bool> is_group_cache;
        std::unordered_map<std::string, std::vector<std::string>> group_members_cache;

        auto is_group = [&](const std::string& _uid) -> bool {
            auto it = is_group_cache.find(_uid);
            if (it != is_group_cache.end()) {
                return it->second;
            }
            auto type_opt = irods::experimental::catalog::query_catalog_string(
                executor, db_conn,
                gq2::builder::select({"user_type_name"})
                    .from("USER")
                    .where(col("user_id") == _uid)
                    .build());
            bool grp = type_opt && (*type_opt == "rodsgroup");
            is_group_cache[_uid] = grp;
            return grp;
        };

        auto get_group_members = [&](const std::string& _gid) -> const std::vector<std::string>& {
            auto it = group_members_cache.find(_gid);
            if (it != group_members_cache.end()) {
                return it->second;
            }
            auto members = irods::experimental::catalog::query_catalog_strings(
                executor, db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER_GROUP")
                    .where(col("group_user_id") == _gid)
                    .build());
            return group_members_cache.emplace(_gid, std::move(members)).first->second;
        };

        for (const auto& q : quotas) {
            int64_t sum_usage = 0;

            if (!is_group(q.user_id)) {
                // Individual user quota
                if (q.resc_id == "0") {
                    // Total usage across all resources
                    auto usage_opt = irods::experimental::catalog::query_catalog_integer(
                        executor, db_conn,
                        gq2::builder::select(gq2::builder::sum("quota_usage"))
                            .from("QUOTA_USAGE")
                            .where(col("user_id") == q.user_id)
                            .build());
                    sum_usage = usage_opt.value_or(0);
                }
                else {
                    // Resource-specific usage
                    auto usage_opt = irods::experimental::catalog::query_catalog_integer(
                        executor, db_conn,
                        gq2::builder::select(gq2::builder::sum("quota_usage"))
                            .from("QUOTA_USAGE")
                            .where(col("user_id") == q.user_id && col("resc_id") == q.resc_id)
                            .build());
                    sum_usage = usage_opt.value_or(0);
                }
            }
            else {
                // Group quota: sum member usages
                const auto& members = get_group_members(q.user_id);
                for (const auto& mid : members) {
                    if (q.resc_id == "0") {
                        auto usage_opt = irods::experimental::catalog::query_catalog_integer(
                            executor, db_conn,
                            gq2::builder::select(gq2::builder::sum("quota_usage"))
                                .from("QUOTA_USAGE")
                                .where(col("user_id") == mid && col("resc_id") != "0")
                                .build());
                        sum_usage += usage_opt.value_or(0);
                    }
                    else {
                        auto usage_opt = irods::experimental::catalog::query_catalog_integer(
                            executor, db_conn,
                            gq2::builder::select(gq2::builder::sum("quota_usage"))
                                .from("QUOTA_USAGE")
                                .where(col("user_id") == mid && col("resc_id") == q.resc_id)
                                .build());
                        sum_usage += usage_opt.value_or(0);
                    }
                }
            }

            int64_t over = sum_usage - q.quota_limit;
            auto upd = gq2::builder::update("QUOTA")
                .set("quota_over", std::to_string(over))
                .set("modify_ts", myTime)
                .where(col("user_id") == q.user_id && col("resc_id") == q.resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
        }

        trans.commit();
        return 0;
    }
    catch (const std::exception& e) {
        log_db::error("{}: failed: {}", __func__, e.what());
        return CAT_SQL_ERR;
    }
}

int
icatGetTicketUserId( irods::plugin_property_map& _prop_map, const char *userName, char *userIdStr ) {
    char userZone[NAME_LEN];
    char zoneToUse[NAME_LEN];
    char userName2[NAME_LEN];

    std::string zone;
    irods::error ret = getLocalZone( _prop_map, &icss, zone );
    if ( !ret.ok() ) {
        return ret.code();
    }

    snprintf( zoneToUse, sizeof( zoneToUse ), "%s", zone.c_str() );
    int status = validateAndParseUserName( userName, userName2, userZone );
    if ( status ) {
        return status;
    }
    if ( userZone[0] != '\0' ) {
        rstrcpy( zoneToUse, userZone, NAME_LEN );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        auto opt_id = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName2 && col("zone_name") == zoneToUse && col("user_type_name") != "rodsgroup")
                .build());
        if (!opt_id) {
            return CAT_INVALID_USER;
        }
        rstrcpy( userIdStr, opt_id->c_str(), NAME_LEN );
        return 0;
    }
    catch (const std::exception& e) {
        log_db::error("{}: Exception caught: {}", __func__, e.what());
        return CAT_SQL_ERR;
    }
}

int
icatGetTicketGroupId( irods::plugin_property_map& _prop_map, const char *groupName, char *groupIdStr ) {
    char groupZone[NAME_LEN];
    char zoneToUse[NAME_LEN];
    char groupName2[NAME_LEN];

    std::string zone;
    irods::error ret = getLocalZone( _prop_map, &icss, zone );
    if ( !ret.ok() ) {
        return ret.code();
    }

    snprintf( zoneToUse, sizeof( zoneToUse ), "%s", zone.c_str() );
    int status = validateAndParseUserName( groupName, groupName2, groupZone );
    if ( status ) {
        return status;
    }
    if ( groupZone[0] != '\0' ) {
        rstrcpy( zoneToUse, groupZone, NAME_LEN );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        auto opt_id = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == groupName2 && col("zone_name") == zoneToUse && col("user_type_name") == "rodsgroup")
                .build());
        if (!opt_id) {
            return CAT_INVALID_GROUP;
        }
        rstrcpy( groupIdStr, opt_id->c_str(), NAME_LEN );
        return 0;
    }
    catch (const std::exception& e) {
        log_db::error("{}: Exception caught: {}", __func__, e.what());
        return CAT_SQL_ERR;
    }
}


static
int convert_hostname_to_dotted_decimal_ipv4_and_store_in_buffer(const char* _hostname, char* _buf) {
    struct addrinfo hint;
    memset(&hint, 0, sizeof(hint));
    hint.ai_family = AF_INET;
    struct addrinfo *p_addrinfo;
    const int ret_getaddrinfo_with_retry = getaddrinfo_with_retry(_hostname, 0, &hint, &p_addrinfo);
    if (ret_getaddrinfo_with_retry) {
        return ret_getaddrinfo_with_retry;
    }
    sprintf(_buf, "%s", inet_ntoa(reinterpret_cast<struct sockaddr_in*>(p_addrinfo->ai_addr)->sin_addr));
    freeaddrinfo(p_addrinfo);
    return 0;
}


char *
convertHostToIp( const char *inputName ) {
    static char ipAddr[50];
    const int status = convert_hostname_to_dotted_decimal_ipv4_and_store_in_buffer(inputName, ipAddr);
    if (status != 0) {
        log_db::error(
            "convertHostToIp convert_hostname_to_dotted_decimal_ipv4_and_store_in_buffer error. status [{}]", status);
        return NULL;
    }
    return ipAddr;
}

// XXXX HELPER FUNCTIONS ABOVE

// =-=-=-=-=-=-=-
// read a message body off of the socket
irods::error db_start_op(
    irods::plugin_context& _ctx ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    return ret;


} // db_start_op

// =-=-=-=-=-=-=-
// set debug behavior for plugin
irods::error db_debug_op(
    irods::plugin_context& _ctx,
    const char*            _mode ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check incoming param
    if ( !_mode ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "mode is null" );
    }

    // =-=-=-=-=-=-=-
    // run tolower on mode
    std::string mode( _mode );
    std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char _ch) { return std::tolower(_ch); });

    // =-=-=-=-=-=-=-
    // if mode contains 'sql' then turn SQL logging on
    if ( mode.find( "sql" ) != std::string::npos ) {
        logSQL = 1;
    }
    else {
        logSQL = 0;
    }

    return SUCCESS();

} // db_debug_op

// =-=-=-=-=-=-=-
// open a database connection
irods::error db_open_op(
    irods::plugin_context& _ctx ) {

    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check incoming param
//        if ( !_cfg ) {
//            return ERROR(
//                       CAT_INVALID_ARGUMENT,
//                       "config is null" );
//        }

    // =-=-=-=-=-=-=-
    // log as appropriate
    log_sql::debug("chlOpen");

    // =-=-=-=-=-=-=-
    // cache db creds
    try {
        const auto db_plugin_map = irods::get_server_property<nlohmann::json>(std::vector<std::string>{irods::KW_CFG_PLUGIN_CONFIGURATION, irods::KW_CFG_PLUGIN_TYPE_DATABASE});
        snprintf(icss.databaseUsername,
                 DB_USERNAME_LEN,
                 "%s",
                 db_plugin_map.at(irods::KW_CFG_DB_USERNAME).get_ref<const std::string&>().c_str());
        snprintf(icss.databasePassword,
                 DB_PASSWORD_LEN,
                 "%s",
                 db_plugin_map.at(irods::KW_CFG_DB_PASSWORD).get_ref<const std::string&>().c_str());
        snprintf(icss.database_plugin_type,
                 DB_TYPENAME_LEN,
                 "%s",
                 db_plugin_map.at(irods::KW_CFG_DB_TECHNOLOGY).get_ref<const std::string&>().c_str());
    } catch ( const irods::exception& e ) {
        return irods::error(e);
    } catch ( const boost::exception& e ) {
        return ERROR(INVALID_ANY_CAST, "Failed any_cast in the database configuration");
    }

    // =-=-=-=-=-=-=-
    // call open connection
    int status = db_open_connection( &icss );
    if ( 0 != status ) {
        return ERROR(
                   status,
                   "failed to open db connection" );
    }

    // =-=-=-=-=-=-=-
    // set success flag
    icss.status = 1;

    const auto& flavor = irods::experimental::catalog::get_db_flavor(icss.databaseType);
    if (flavor.supports_catalog_properties) {
        irods::catalog_properties::instance().capture_if_needed( &icss );
    }

    return CODE( status );

} // db_open_op

// =-=-=-=-=-=-=-
// close a database connection
irods::error db_close_op(
    irods::plugin_context& _ctx ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // call close connection
    int status = db_close_connection( &icss );
    if ( 0 != status ) {
        return ERROR(
                   status,
                   "failed to close db connection" );
    }

    // =-=-=-=-=-=-=-
    // set success flag
    icss.status = 0;
    irods::experimental::catalog::get_database_session().reset();

    return CODE( status );

} // db_close_op

// =-=-=-=-=-=-=-
// return the local zone
irods::error db_check_and_get_object_id_op(
    irods::plugin_context& _ctx,
    const char*            _type,
    const char*            _name,
    const char*            _access ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    rodsLong_t status = checkAndGetObjectId(
                            _ctx.comm(),
                            _ctx.prop_map(),
                            _type,
                            _name,
                            _access );
    if ( status < 0 ) {
        return ERROR( status, "checkAndGetObjectId failed" );
    }
    else {
        return SUCCESS();

    }

} // db_check_and_get_object_id_op

// =-=-=-=-=-=-=-
// return the local zone
irods::error db_get_local_zone_op(
    irods::plugin_context& _ctx,
    std::string*           _zone ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    ret = getLocalZone( _ctx.prop_map(), &icss, ( *_zone ) );
    if ( !ret.ok() ) {
        return PASS( ret );

    }
    else {
        return SUCCESS();

    }

} // db_get_local_zone_op

// =-=-=-=-=-=-=-
// update the data obj count of a resource
irods::error db_update_resc_obj_count_op(
    irods::plugin_context& _ctx,
    const std::string*     _resc,
    int                    _delta ) {

    return SUCCESS();

} // db_update_resc_obj_count_op

// =-=-=-=-=-=-=-
// update the data obj count of a resource
irods::error db_mod_data_obj_meta_op(
    irods::plugin_context& _ctx,
    dataObjInfo_t*         _data_obj_info,
    keyValPair_t*          _reg_param ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_data_obj_info ||
            !_reg_param ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    int status = 0, upCols = 0;
    rodsLong_t iVal = 0; // JMC cppcheck - uninit var

    char logicalFileName[MAX_NAME_LEN];
    char logicalDirName[MAX_NAME_LEN];
    char *theVal = 0;
    char replNum1[MAX_NAME_LEN];

    const char* whereColsAndConds[10];
    const char* whereValues[10];
    char idVal[MAX_NAME_LEN];
    int numConditions = 0;
    char replica_status_string[NAME_LEN]{};

    std::vector<const char *> updateCols;
    std::vector<const char *> updateVals;

    // clang-format off
    const std::vector<std::string_view> regParamNames = {
        COLL_ID_KW,
        DATA_CREATE_KW,
        CHKSUM_KW,
        DATA_EXPIRY_KW,
        //DATA_ID_KW,
        REPL_STATUS_KW,
        //DATA_MAP_ID_KW,
        DATA_MODE_KW,
        DATA_NAME_KW,
        DATA_OWNER_KW,
        DATA_OWNER_ZONE_KW,
        FILE_PATH_KW,
        REPL_NUM_KW,
        DATA_SIZE_KW,
        STATUS_STRING_KW,
        DATA_TYPE_KW,
        VERSION_KW,
        DATA_MODIFY_KW,
        DATA_COMMENTS_KW,
        // DATA_RESC_GROUP_NAME_KW,
        RESC_HIER_STR_KW,
        RESC_ID_KW,
        RESC_NAME_KW,
        DATA_ACCESS_TIME_KW
    };

    const std::vector<std::string_view> colNames = {
        "coll_id",
        "create_ts",
        "data_checksum",
        "data_expiry_ts",
        //"data_id",
        "data_is_dirty",
        //"data_map_id",
        "data_mode",
        "data_name",
        "data_owner_name",
        "data_owner_zone",
        "data_path",
        "data_repl_num",
        "data_size",
        "data_status",
        "data_type_name",
        "data_version",
        "modify_ts",
        "r_comment",
        //"resc_group_name",
        "resc_hier",
        "resc_id",
        "resc_name",
        "access_ts"
    };
    // clang-format on

    int doingDataSize = 0;
    char dataSizeString[NAME_LEN] = "";
    char objIdString[MAX_NAME_LEN];
    char *neededAccess = 0;

    log_sql::debug("chlModDataObjMeta");

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    bool adminMode{};
    if (getValByKey(_reg_param, ADMIN_KW)) {
        if (LOCAL_PRIV_USER_AUTH != _ctx.comm()->clientUser.authInfo.authFlag) {
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "failed with insufficient privilege");
        }
        adminMode = true;
    }

    std::string update_resc_id_str;

    bool update_resc_id = false;
    /* Set up the updateCols and updateVals arrays */
    std::size_t i = 0, j = 0;
    for (i = 0, j = 0; i < regParamNames.size(); i++) {
        theVal = getValByKey(_reg_param, regParamNames[i].data());
        if (theVal) {
            if(colNames[i] == "resc_name") {
                continue;
            }
            else if(colNames[i] == "resc_hier") {
                updateCols.push_back( "resc_id" );

                rodsLong_t resc_id;
                resc_mgr.hier_to_leaf_id(theVal,resc_id);

                update_resc_id_str = boost::lexical_cast<std::string>(resc_id);
                updateVals.push_back( update_resc_id_str.c_str() );
            }
            else {
                updateCols.push_back(colNames[i].data());
                updateVals.push_back(theVal);
            }

            if ( std::string( "resc_id" ) == colNames[i] || std::string( "resc_hier") == colNames[i]) {
                update_resc_id = true;
            }

            if(regParamNames[i] == DATA_EXPIRY_KW) {
                /* if data_expiry, make sure it's in the standard time-stamp format: "%011d" */
                if (colNames[i] == "data_expiry_ts") { /* double check*/
                    if ( strlen( theVal ) < 11 ) {
                        static char theVal2[20];
                        time_t myTimeValue;
                        myTimeValue = atoll( theVal );
                        snprintf( theVal2, sizeof theVal2, "%011d", ( int )myTimeValue );
                        updateVals[j] = theVal2;
                    }
                }
            }

            if(regParamNames[i] == DATA_MODIFY_KW) {
                /* if modify_ts, also make sure it's in the standard time-stamp format: "%011d" */
                if (colNames[i] == "modify_ts") { /* double check*/
                    if ( strlen( theVal ) < 11 ) {
                        static char theVal3[20];
                        time_t myTimeValue;
                        myTimeValue = atoll( theVal );
                        snprintf( theVal3, sizeof theVal3, "%011d", ( int )myTimeValue );
                        updateVals[j] = theVal3;
                    }
                }
            }

            if (regParamNames[i] == DATA_ACCESS_TIME_KW) {
                /* if access_ts, also make sure it's in the standard time-stamp format: "%011d" */
                if (colNames[i] == "access_ts") { /* double check*/
                    if (strlen(theVal) < 11) {
                        static char theVal4[20];
                        time_t myTimeValue;
                        myTimeValue = atoll(theVal);
                        snprintf(theVal4, sizeof theVal4, "%011d", (int) myTimeValue);
                        updateVals[j] = theVal4;
                    }
                }
            }

            if(regParamNames[i] == DATA_SIZE_KW) {
                doingDataSize = 1; /* flag to check size */
                snprintf( dataSizeString, sizeof( dataSizeString ), "%s", theVal );
            }

            j++;

            /* If the datatype is being updated, check that it is valid */
            if(regParamNames[i] == DATA_TYPE_KW) {
                status = irods::experimental::catalog::access_control::check_name_token(
                    executor, db_conn, "data_type", theVal );
                if ( status != 0 ) {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Invalid data type specified.";
                    addRErrorMsg( &_ctx.comm()->rError, 0, msg.str().c_str() );
                    return ERROR(
                               CAT_INVALID_DATA_TYPE,
                               msg.str() );
                }
            }
        }
    }
    upCols = j;

    /* If the only field is the chksum then the user only needs read
       access since we can trust that the server-side code is
       calculating it properly and checksum is a system-managed field.
       For example, when doing an irsync the server may calculate a
       checksum and want to set it in the source copy.
    */
    neededAccess = ACCESS_MODIFY_METADATA;
    if ( upCols == 1 && strcmp( updateCols[0], "chksum" ) == 0 ) {
        neededAccess = ACCESS_READ_OBJECT;
    }

    /* If dataExpiry is being updated, user needs to have
       a greater access permission */
    theVal = getValByKey( _reg_param, DATA_EXPIRY_KW );
    if ( theVal != NULL ) {
        neededAccess = ACCESS_DELETE_OBJECT;
    }

    if ( _data_obj_info->dataId <= 0 ) {
        if (const auto ec = splitPathByKey(_data_obj_info->objPath, logicalDirName, MAX_NAME_LEN, logicalFileName, MAX_NAME_LEN, '/'); ec < 0) {
            return ERROR(ec, fmt::format(
                         "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                         __func__, __LINE__, _data_obj_info->objPath, ec));
        }

        log_sql::debug("chlModDataObjMeta SQL 1 ");
        auto opt_coll_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == logicalDirName)
                .build());
        if (!opt_coll_id) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "collection '{}' is unknown", logicalDirName ).c_str() );
            _rollback( "chlModDataObjMeta" );
            return ERROR(
                       CAT_UNKNOWN_COLLECTION,
                       "failed with unknown collection" );
        }
        iVal = *opt_coll_id;
        const auto collIdString = std::to_string( iVal );

        log_sql::debug("chlModDataObjMeta SQL 2");
        auto opt_data_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"data_id"})
                .from("DATA_OBJECT")
                .where(col("coll_id") == collIdString && col("data_name") == logicalFileName)
                .build());
        if (!opt_data_id) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to find file in database by its logical path.";
            addRErrorMsg( &_ctx.comm()->rError, 0, msg.str().c_str() );
            _rollback( "chlModDataObjMeta" );
            return ERROR(
                       CAT_UNKNOWN_FILE,
                       "failed with unknown file" );
        }
        iVal = *opt_data_id;

        _data_obj_info->dataId = iVal;  /* return it for possible use next time, */
        /* and for use below */
    }

    snprintf( objIdString, MAX_NAME_LEN, "%lld", _data_obj_info->dataId );

    if (!adminMode) {
        if ( doingDataSize == 1 && strlen( mySessionTicket ) > 0 ) {
            status = irods::experimental::catalog::access_control::ticket_update_write_bytes(
                executor, db_conn, mySessionTicket, dataSizeString, objIdString );
            if ( status != 0 ) {
                return ERROR(status, "ticket_update_write_bytes failed");
            }
        }

        status = irods::experimental::catalog::access_control::check_data_object_id(
                     executor, db_conn,
                     objIdString,
                     _ctx.comm()->clientUser.userName,
                     _ctx.comm()->clientUser.rodsZone,
                     neededAccess,
                     mySessionTicket,
                     mySessionClientAddr );

        if ( status != 0 ) {
            theVal = getValByKey( _reg_param, ACL_COLLECTION_KW );
            if ( theVal != NULL && upCols == 1 &&
                    strcmp( updateCols[0], "data_path" ) == 0 ) {
                int len;
                int64_t iVal_check = 0;
                /*
                 In this case, the user is doing a 'imv' of a collection but one of
                 the sub-files is not owned by them.  We decided this should be
                 allowed and so we support it via this new ACL_COLLECTION_KW, checking
                 that the ACL_COLLECTION matches the beginning path of the object and
                 that the user has the appropriate access to that collection.
                 */
                len = strlen( theVal );
                if ( strncmp( theVal, _data_obj_info->objPath, len ) == 0 ) {

                    iVal_check = irods::experimental::catalog::access_control::check_collection_access(
                        executor, db_conn,
                        theVal,
                        _ctx.comm()->clientUser.userName,
                        _ctx.comm()->clientUser.rodsZone,
                        ACCESS_OWN );
                }
                if ( iVal_check > 0 ) {
                    status = 0;
                } /* Collection was found (id
                                   * returned) & user has access */
            }
            if ( status ) {
                _rollback( "chlModDataObjMeta" );
                return ERROR(
                           CAT_NO_ACCESS_PERMISSION,
                           "failed with no permission" );
            }
        }
    }

    whereColsAndConds[0] = "data_id=";
    snprintf( idVal, MAX_NAME_LEN, "%lld", _data_obj_info->dataId );
    whereValues[0] = idVal;
    numConditions = 1;

    /* This is up here since this is usually called to modify the
     * metadata of a single repl.  If ALL_KW is included, then apply
     * this change to all replicas (by not restricting the update to
     * only one).
     */
    std::string where_resc_id_str;
    if ( getValByKey( _reg_param, ALL_KW ) == NULL ) {
        // use resc_id instead of replNum as it is
        // always set, unless resc_id is to be
        // updated.  replNum is sometimes 0 in various
        // error cases
        if ( update_resc_id || strlen( _data_obj_info->rescHier ) <= 0 ) {
            j = numConditions;
            whereColsAndConds[j] = "data_repl_num=";
            snprintf( replNum1, MAX_NAME_LEN, "%d", _data_obj_info->replNum );
            whereValues[j] = replNum1;
            numConditions++;

        }
        else {
            rodsLong_t id = 0;
            resc_mgr.hier_to_leaf_id( _data_obj_info->rescHier, id );
            where_resc_id_str = boost::lexical_cast<std::string>(id);
            j = numConditions;
            whereColsAndConds[j] = "resc_id=";
            whereValues[j] = where_resc_id_str.c_str();
            numConditions++;
        }
    }

    std::string zone;
    ret = getLocalZone(
              _ctx.prop_map(),
              &icss,
              zone );
    if ( !ret.ok() ) {
        log_db::error("chlModObjMeta - failed in getLocalZone with status [{}]", status);
        return PASS( ret );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        std::unique_ptr<nanodbc::transaction> trans;
        if (!(_data_obj_info->flags & NO_COMMIT_FLAG)) {
            trans = std::make_unique<nanodbc::transaction>(db_conn);
        }

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto build_base_condition = [&]() -> gq2::builder::condition_builder {
            auto cond = (col("data_id") == std::string_view{idVal});
            if (getValByKey(_reg_param, ALL_KW) == NULL) {
                if (update_resc_id || strlen(_data_obj_info->rescHier) <= 0) {
                    cond = cond && (col("data_repl_num") == std::string_view{replNum1});
                }
                else {
                    cond = cond && (col("resc_id") == std::string_view{where_resc_id_str});
                }
            }
            return cond;
        };

        if (!getValByKey(_reg_param, ALL_REPL_STATUS_KW)) {
            log_sql::debug("chlModDataObjMeta SQL 4");
            if (!updateCols.empty()) {
                auto update_builder = gq2::builder::update("DATA_OBJECT");
                for (std::size_t k = 0; k < updateCols.size(); ++k) {
                    update_builder.set(updateCols[k], updateVals[k]);
                }
                auto update_stmt = update_builder.where(build_base_condition()).build();
                const auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, update_stmt);
                if (res.affected_rows == 0) {
                    return ERROR(CAT_SUCCESS_BUT_WITH_NO_INFO, "no rows updated");
                }
            }

            // Stale intermediate replicas
            if (getValByKey(_reg_param, STALE_ALL_INTERMEDIATE_REPLICAS_KW)) {
                snprintf(replNum1, MAX_NAME_LEN, "%d", _data_obj_info->replNum);

                auto stale_builder = gq2::builder::update("DATA_OBJECT")
                    .set("data_is_dirty", std::to_string(STALE_REPLICA));

                auto stale_cond = (col("data_id") == std::string_view{idVal}) &&
                    (col("data_repl_num") != std::string_view{replNum1}) &&
                    (col("data_is_dirty") == std::string_view{intermediate_replica_status_str});

                auto stale_stmt = stale_builder.where(stale_cond).build();
                log_sql::debug("chlModDataObjMeta SQL 6");
                irods::experimental::catalog::execute_catalog(executor, db_conn, stale_stmt);
            }
        }
        else {
            /* mark this one as GOOD_REPLICA and others as STALE_REPLICA */
            updateCols.push_back("data_is_dirty");
            snprintf(replica_status_string, NAME_LEN, "%d", GOOD_REPLICA);
            updateVals.push_back(replica_status_string);

            log_sql::debug("chlModDataObjMeta SQL 5");
            auto update_builder = gq2::builder::update("DATA_OBJECT");
            for (std::size_t k = 0; k < updateCols.size(); ++k) {
                update_builder.set(updateCols[k], updateVals[k]);
            }
            auto update_stmt = update_builder.where(build_base_condition()).build();
            const auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, update_stmt);
            if (res.affected_rows == 0) {
                return ERROR(CAT_SUCCESS_BUT_WITH_NO_INFO, "no rows updated");
            }

            snprintf(replNum1, MAX_NAME_LEN, "%d", _data_obj_info->replNum);

            auto stale_builder = gq2::builder::update("DATA_OBJECT")
                .set("data_is_dirty", std::to_string(STALE_REPLICA));

            auto stale_cond = (col("data_id") == std::string_view{idVal}) &&
                (col("data_repl_num") != std::string_view{replNum1}) &&
                (col("data_is_dirty") != std::string_view{intermediate_replica_status_str});

            auto stale_stmt = stale_builder.where(stale_cond).build();
            log_sql::debug("chlModDataObjMeta SQL 6");
            irods::experimental::catalog::execute_catalog(executor, db_conn, stale_stmt);
        }

        if (trans) {
            trans->commit();
        }

        return CODE(status);
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }

} // db_mod_data_obj_meta_op


// =-=-=-=-=-=-=-
// update the data obj count of a resource
irods::error db_reg_data_obj_op(
    irods::plugin_context& _ctx,
    dataObjInfo_t*         _data_obj_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_data_obj_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    // =-=-=-=-=-=-=-
    //
    char myTime[50];
    char logicalFileName[MAX_NAME_LEN];
    char logicalDirName[MAX_NAME_LEN];
    rodsLong_t seqNum;
    rodsLong_t iVal;
    char dataIdNum[MAX_NAME_LEN];
    char collIdNum[MAX_NAME_LEN];
    char dataReplNum[MAX_NAME_LEN];
    char dataSizeNum[MAX_NAME_LEN];
    char dataStatusNum[MAX_NAME_LEN];
    int status;
    int inheritFlag;

    log_sql::debug("chlRegDataObj");
    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    log_sql::debug("chlRegDataObj SQL 1 ");
    seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
    if ( seqNum < 0 ) {
        log_db::info("chlRegDataObj get_next_sequence_value failure {}", seqNum);
        _rollback( "chlRegDataObj" );
        return ERROR( seqNum, "chlRegDataObj get_next_sequence_value failure" );
    }
    snprintf( dataIdNum, MAX_NAME_LEN, "%lld", seqNum );
    _data_obj_info->dataId = seqNum; /* store as output parameter */

    if (const auto ec = splitPathByKey(_data_obj_info->objPath, logicalDirName, MAX_NAME_LEN, logicalFileName, MAX_NAME_LEN, '/'); ec < 0) {
        return ERROR(ec, fmt::format(
                     "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                     __func__, __LINE__, _data_obj_info->objPath, ec));
    }

    /* Check that collection exists and user has write permission.
       At the same time, also get the inherit flag */
    iVal = irods::experimental::catalog::access_control::check_collection_access_and_inherit(
        executor, db_conn,
        logicalDirName,
        _ctx.comm()->clientUser.userName,
        _ctx.comm()->clientUser.rodsZone,
        ACCESS_MODIFY_OBJECT,
        &inheritFlag,
        mySessionTicket,
        mySessionClientAddr );
    if ( iVal < 0 ) {
        if ( iVal == CAT_UNKNOWN_COLLECTION ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "collection '{}' is unknown", logicalDirName ).c_str() );
        }
        else if ( iVal == CAT_NO_ACCESS_PERMISSION ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "no permission to update collection '{}'", logicalDirName ).c_str() );
        }
        //_rollback("chlRegDataObj");
        return ERROR( iVal, "" );
    }
    snprintf( collIdNum, MAX_NAME_LEN, "%lld", iVal );
    _data_obj_info->collId = iVal;

    /* Make sure no collection already exists by this name */
    log_sql::debug("chlRegDataObj SQL 4");
    auto opt_coll = irods::experimental::catalog::query_catalog_integer(
        executor,
        db_conn,
        gq2::builder::select({"coll_id"})
            .from("COLLECTION")
            .where(col("coll_name") == _data_obj_info->objPath)
            .build());
    if ( opt_coll.has_value() ) {
        return ERROR( CAT_NAME_EXISTS_AS_COLLECTION, "collection exists" );
    }

    log_sql::debug("chlRegDataObj SQL 5");
    status = irods::experimental::catalog::access_control::check_name_token(
        executor, db_conn, "data_type", _data_obj_info->dataType );
    if ( status != 0 ) {
        return ERROR( CAT_INVALID_DATA_TYPE, "invalid data type" );
    }

    snprintf( dataReplNum, MAX_NAME_LEN, "%d", _data_obj_info->replNum );
    snprintf( dataStatusNum, MAX_NAME_LEN, "%d", _data_obj_info->replStatus );
    snprintf( dataSizeNum, MAX_NAME_LEN, "%lld", _data_obj_info->dataSize );

    getNowStr( myTime );
    if (0 == strcmp(_data_obj_info->dataModify, "")) {
        strcpy(_data_obj_info->dataModify, myTime);
    }
    if (0 == strcmp(_data_obj_info->dataCreate, "")) {
        strcpy(_data_obj_info->dataCreate, myTime);
    }
    if (0 == strcmp(_data_obj_info->dataAccessTime, "")) {
        strcpy(_data_obj_info->dataAccessTime, myTime);
    }
    strcpy(_data_obj_info->dataExpiry, "00000000000");

    std::snprintf(_data_obj_info->dataOwnerName, sizeof(_data_obj_info->dataOwnerName), "%s", _ctx.comm()->clientUser.userName);
    std::snprintf(_data_obj_info->dataOwnerZone, sizeof(_data_obj_info->dataOwnerZone), "%s", _ctx.comm()->clientUser.rodsZone);

    std::string resc_id_str = boost::lexical_cast<std::string>(_data_obj_info->rescId);

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        std::unique_ptr<nanodbc::transaction> trans;
        if (!(_data_obj_info->flags & NO_COMMIT_FLAG)) {
            trans = std::make_unique<nanodbc::transaction>(db_conn);
        }

        log_sql::debug("chlRegDataObj SQL 6");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto ins_data = gq2::builder::insert_into("DATA_OBJECT")
            .set("data_id", dataIdNum)
            .set("coll_id", collIdNum)
            .set("data_name", logicalFileName)
            .set("data_repl_num", dataReplNum)
            .set("data_version", _data_obj_info->version)
            .set("data_type_name", _data_obj_info->dataType)
            .set("data_size", dataSizeNum)
            .set("resc_id", resc_id_str)
            .set("data_path", _data_obj_info->filePath)
            .set("data_owner_name", _data_obj_info->dataOwnerName)
            .set("data_owner_zone", _data_obj_info->dataOwnerZone)
            .set("data_is_dirty", dataStatusNum)
            .set("data_checksum", _data_obj_info->chksum)
            .set("data_mode", _data_obj_info->dataMode)
            .set("create_ts", _data_obj_info->dataCreate)
            .set("modify_ts", _data_obj_info->dataModify)
            .set("data_expiry_ts", _data_obj_info->dataExpiry)
            .set("resc_name", "EMPTY_RESC_NAME")
            .set("resc_hier", "EMPTY_RESC_HIER")
            .set("resc_group_name", "EMPTY_RESC_GROUP_NAME")
            .set("access_ts", _data_obj_info->dataAccessTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_data);

        std::string zone;
        ret = getLocalZone(_ctx.prop_map(), &icss, zone);
        if (!ret.ok()) {
            log_db::error("chlRegDataInfo - failed in getLocalZone with status [{}]", ret.code());
            return PASS(ret);
        }

        if (inheritFlag) {
            log_sql::debug("chlRegDataObj SQL 7");
            auto q_res = irods::experimental::catalog::execute_catalog(
                executor, db_conn,
                gq2::builder::select({"user_id", "access_type_id"})
                    .from("ACCESS")
                    .where(col("object_id") == collIdNum)
                    .build());
            if (q_res.query_result) {
                while (q_res.query_result->next()) {
                    const auto uid = q_res.query_result->get<std::string>(0);
                    const auto atid = q_res.query_result->get<std::string>(1);
                    auto ins_acc = gq2::builder::insert_into("ACCESS")
                        .set("object_id", dataIdNum)
                        .set("user_id", uid)
                        .set("access_type_id", atid)
                        .set("create_ts", myTime)
                        .set("modify_ts", myTime)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins_acc);
                }
            }
        }
        else {
            log_sql::debug("chlRegDataObj SQL 8");
            const auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                    .build());
            if (!opt_user_id) {
                return ERROR(CAT_INVALID_USER, "user not found");
            }
            const auto opt_token_id = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"token_id"})
                    .from("TOKEN")
                    .where(col("token_namespace") == "access_type" && col("token_name") == ACCESS_OWN)
                    .build());
            if (!opt_token_id) {
                return ERROR(CAT_INVALID_ARGUMENT, "access token not found");
            }
            auto ins_acc = gq2::builder::insert_into("ACCESS")
                .set("object_id", dataIdNum)
                .set("user_id", std::to_string(*opt_user_id))
                .set("access_type_id", std::to_string(*opt_token_id))
                .set("create_ts", myTime)
                .set("modify_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_acc);
        }

        if (trans) {
            trans->commit();
        }

        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }

} // db_reg_data_obj_op


// =-=-=-=-=-=-=-
// register a data object into the catalog
irods::error db_reg_replica_op(
    irods::plugin_context& _ctx,
    dataObjInfo_t*         _src_data_obj_info,
    dataObjInfo_t*         _dst_data_obj_info,
    keyValPair_t*          _cond_input ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_src_data_obj_info ||
        !_dst_data_obj_info ||
        !_cond_input ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    if( _dst_data_obj_info->rescId <= 0 ) {
        std::stringstream msg;
        msg << "invalid resource id "
            << _dst_data_obj_info->rescId
            << " for ["
            << _dst_data_obj_info->objPath
            << "]";
        return ERROR(
                SYS_INVALID_INPUT_PARAM,
                msg.str() );
    }

    char myTime[50];
    char logicalFileName[MAX_NAME_LEN];
    char logicalDirName[MAX_NAME_LEN];
    rodsLong_t iVal;
    rodsLong_t status;
    int nextReplNum;
    char nextRepl[30];
    const int IX_DATA_REPL_NUM = 3; /* index of data_repl_num in theColls */
//        int IX_RESC_GROUP_NAME = 7; /* index into theColls */
    const int IX_RESC_ID = 10;
    const int IX_DATA_PATH = 11;    /* index into theColls */
    const int IX_REPLICA_STATUS = 14;
    const int IX_DATA_STATUS = 15;

    const int IX_DATA_MODE = 19;
    const int IX_CREATE_TS = 21;
    const int IX_MODIFY_TS = 22;
    const int IX_ACCESS_TS = 23;

    char objIdString[MAX_NAME_LEN];
    char replNumString[MAX_NAME_LEN];
    int adminMode;
    char *theVal;

    log_sql::debug("chlRegReplica");

    adminMode = 0;
    if ( _cond_input != NULL ) {
        theVal = getValByKey( _cond_input, ADMIN_KW );
        if ( theVal != NULL ) {
            adminMode = 1;
        }
    }

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    if (const auto ec = splitPathByKey(_src_data_obj_info->objPath, logicalDirName, MAX_NAME_LEN, logicalFileName, MAX_NAME_LEN, '/'); ec < 0) {
        return ERROR(ec, fmt::format(
                     "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                     __func__, __LINE__, _src_data_obj_info->objPath, ec));
    }

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    if ( adminMode ) {
        if ( _ctx.comm()->clientUser.authInfo.authFlag != LOCAL_PRIV_USER_AUTH ) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }
    }
    else {
        /* Check the access to the dataObj */
        log_sql::debug("chlRegReplica SQL 1 ");
        status = irods::experimental::catalog::access_control::check_data_object_only(
            executor, db_conn,
            logicalDirName, logicalFileName,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_READ_OBJECT );
        if ( status < 0 ) {
            _rollback( "chlRegReplica" );
            return ERROR( status, "check_data_object_only failed" );
        }
    }

    /* Get the next replica number */
    snprintf( objIdString, MAX_NAME_LEN, "%lld", _src_data_obj_info->dataId );
    log_sql::debug("chlRegReplica SQL 2");
    auto opt_max_repl = irods::experimental::catalog::query_catalog_integer(
        executor,
        db_conn,
        gq2::builder::select({gq2::builder::max("data_repl_num")})
            .from("DATA_OBJECT")
            .where(col("data_id") == objIdString)
            .build());
    if (!opt_max_repl) {
        _rollback( "chlRegReplica" );
        return ERROR( CAT_SQL_ERR, "query max data_repl_num failed" );
    }
    iVal = *opt_max_repl;

    nextReplNum = iVal + 1;
    snprintf( nextRepl, sizeof nextRepl, "%d", nextReplNum );
    _dst_data_obj_info->replNum = nextReplNum; /* return new replica number */
    snprintf( replNumString, MAX_NAME_LEN, "%d", _src_data_obj_info->replNum );
    log_sql::debug("chlRegReplica SQL 3");

    const std::vector<std::string> replica_cols = {
        "data_id", "coll_id", "data_name", "data_repl_num", "data_version", "data_type_name",
        "data_size", "resc_group_name", "resc_name", "resc_hier", "resc_id", "data_path",
        "data_owner_name", "data_owner_zone", "data_is_dirty", "data_status", "data_checksum",
        "data_expiry_ts", "data_map_id", "data_mode", "r_comment", "create_ts", "modify_ts", "access_ts"
    };

    std::vector<std::string> row_values;
    try {
        auto sel_replica = gq2::builder::select(replica_cols)
            .from("DATA_OBJECT")
            .where(col("data_id") == objIdString && col("data_repl_num") == replNumString)
            .build();
        auto row_res = irods::experimental::catalog::execute_catalog(executor, db_conn, sel_replica);
        if (!row_res.query_result || !row_res.query_result->next()) {
            _rollback("chlRegReplica");
            return ERROR(CAT_NO_ROWS_FOUND, "failed to find source replica");
        }
        for (int col = 0; col < 24; ++col) {
            row_values.push_back(row_res.query_result->get<std::string>(col, ""));
        }
    }
    catch (const std::exception& e) {
        log_db::error("chlRegReplica select source replica failure {}", e.what());
        _rollback("chlRegReplica");
        return ERROR(CAT_SQL_ERR, e.what());
    }

    std::string resc_id_str = boost::lexical_cast<std::string>(_dst_data_obj_info->rescId);

    row_values[IX_DATA_REPL_NUM] = nextRepl;
    row_values[IX_RESC_ID] = resc_id_str;
    row_values[IX_DATA_PATH] = _dst_data_obj_info->filePath;
    row_values[IX_DATA_MODE] = _dst_data_obj_info->dataMode;

    if (getValByKey(_cond_input, REGISTER_AS_INTERMEDIATE_KW)) {
        row_values[IX_REPLICA_STATUS] = std::string(intermediate_replica_status_str);
    }

    std::snprintf(_dst_data_obj_info->statusString, NAME_LEN, "");
    row_values[IX_DATA_STATUS] = _dst_data_obj_info->statusString;

    getNowStr( myTime );
    row_values[IX_MODIFY_TS] = myTime;
    row_values[IX_CREATE_TS] = myTime;
    row_values[IX_ACCESS_TS] = myTime;

    try {
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlRegReplica SQL 4");
        const auto existing = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"data_id"})
                .from("DATA_OBJECT")
                .where(col("resc_id") == resc_id_str && col("data_path") == _dst_data_obj_info->filePath && col("data_id") == objIdString)
                .build());
        if (!existing) {
            namespace gq2 = irods::experimental::genquery2;
            auto ins = gq2::builder::insert_into("DATA_OBJECT")
                .set("data_id", row_values[0])
                .set("coll_id", row_values[1])
                .set("data_name", row_values[2])
                .set("data_repl_num", row_values[3])
                .set("data_version", row_values[4])
                .set("data_type_name", row_values[5])
                .set("data_size", row_values[6])
                .set("resc_group_name", row_values[7])
                .set("resc_name", row_values[8])
                .set("resc_hier", row_values[9])
                .set("resc_id", row_values[10])
                .set("data_path", row_values[11])
                .set("data_owner_name", row_values[12])
                .set("data_owner_zone", row_values[13])
                .set("data_is_dirty", row_values[14])
                .set("data_status", row_values[15])
                .set("data_checksum", row_values[16])
                .set("data_expiry_ts", row_values[17])
                .set("data_map_id", row_values[18])
                .set("data_mode", row_values[19])
                .set("r_comment", row_values[20])
                .set("create_ts", row_values[21])
                .set("modify_ts", row_values[22])
                .set("access_ts", row_values[23])
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
        }

        std::string zone;
        ret = getLocalZone(_ctx.prop_map(), &icss, zone);
        if (!ret.ok()) {
            log_db::error("chlRegReplica - failed in getLocalZone with status [{}]", ret.code());
            return PASS(ret);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }

} // db_reg_replica_op

// =-=-=-=-=-=-=-
// unregister a data object
irods::error db_unreg_replica_op(
    irods::plugin_context& _ctx,
    dataObjInfo_t*         _data_obj_info,
    keyValPair_t*          _cond_input ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_data_obj_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    // =-=-=-=-=-=-=-
    //
    char logicalFileName[MAX_NAME_LEN];
    char logicalDirName[MAX_NAME_LEN];
    rodsLong_t status;
    char dataObjNumber[30];
    int adminMode;
    int trashMode;
    char *theVal;
    char checkPath[MAX_NAME_LEN];

    dataObjNumber[0] = '\0';
    log_sql::debug("chlUnregDataObj");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    adminMode = 0;
    trashMode = 0;
    if ( _cond_input != NULL ) {
        theVal = getValByKey( _cond_input, ADMIN_KW );
        if ( theVal != NULL ) {
            adminMode = 1;
        }
        theVal = getValByKey( _cond_input, ADMIN_RMTRASH_KW );
        if ( theVal != NULL ) {
            adminMode = 1;
            trashMode = 1;
        }
    }

    if (const auto ec = splitPathByKey(_data_obj_info->objPath, logicalDirName, MAX_NAME_LEN, logicalFileName, MAX_NAME_LEN, '/'); ec < 0) {
        return ERROR(ec, fmt::format(
                     "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                     __func__, __LINE__, _data_obj_info->objPath, ec));
    }

    if ( adminMode == 0 ) {
        /* Check the access to the dataObj */
        log_sql::debug("chlUnregDataObj SQL 1 ");
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        status = irods::experimental::catalog::access_control::check_data_object_only(
            executor, db_conn,
            logicalDirName, logicalFileName,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_DELETE_OBJECT );
        if ( status < 0 ) {
            _rollback( "chlUnregDataObj" );
            return ERROR( status, "check_data_object_only failed" );
        }
        snprintf( dataObjNumber, sizeof dataObjNumber, "%lld", (rodsLong_t)status );
    }
    else {
        if ( _ctx.comm()->clientUser.authInfo.authFlag != LOCAL_PRIV_USER_AUTH ) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }
        if ( trashMode ) {
            int len;
            std::string zone;
            ret = getLocalZone( _ctx.prop_map(), &icss, zone );
            if ( !ret.ok() ) {
                return PASS( ret );
            }
            snprintf( checkPath, MAX_NAME_LEN, "/%s/trash", zone.c_str() );
            len = strlen( checkPath );
            if ( strncmp( checkPath, logicalDirName, len ) != 0 ) {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              "TRASH_KW but not zone/trash path" );
                return ERROR( CAT_INVALID_ARGUMENT, "TRASH_KW but not zone/trash path" );
            }
            if ( _data_obj_info->dataId > 0 ) {
                snprintf( dataObjNumber, sizeof dataObjNumber, "%lld",
                          _data_obj_info->dataId );
            }
        }
        else {
            if ( _data_obj_info->replNum >= 0 && _data_obj_info->dataId >= 0 ) {
                snprintf( dataObjNumber, sizeof dataObjNumber, "%lld",
                          _data_obj_info->dataId );
            }
            else {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              "dataId and replNum required" );
                _rollback( "chlUnregDataObj" );
                return ERROR( CAT_INVALID_ARGUMENT, "dataId and replNum required" );
            }
        }
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        const auto coll_id_opt = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == logicalDirName)
                .build());
        if (!coll_id_opt) {
            addRErrorMsg(&_ctx.comm()->rError, 0, fmt::format("data object '{}' is unknown", logicalFileName).c_str());
            return ERROR(CAT_UNKNOWN_FILE, "data object unknown");
        }
        const auto coll_id_str = std::to_string(*coll_id_opt);

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        std::size_t affected = 0;
        if (_data_obj_info->replNum >= 0) {
            log_sql::debug("chlUnregDataObj SQL 4");
            auto rem_stmt = gq2::builder::remove_from("DATA_OBJECT")
                .where(col("coll_id") == coll_id_str &&
                       col("data_name") == logicalFileName &&
                       col("data_repl_num") == std::to_string(_data_obj_info->replNum))
                .build();
            affected = irods::experimental::catalog::execute_catalog(executor, db_conn, rem_stmt).affected_rows;
        }
        else {
            log_sql::debug("chlUnregDataObj SQL 5");
            auto rem_stmt = gq2::builder::remove_from("DATA_OBJECT")
                .where(col("coll_id") == coll_id_str &&
                       col("data_name") == logicalFileName)
                .build();
            affected = irods::experimental::catalog::execute_catalog(executor, db_conn, rem_stmt).affected_rows;
        }

        if (affected == 0) {
            addRErrorMsg(&_ctx.comm()->rError, 0, fmt::format("data object '{}' is unknown", logicalFileName).c_str());
            return ERROR(CAT_UNKNOWN_FILE, "data object unknown");
        }

        std::string zone;
        ret = getLocalZone(_ctx.prop_map(), &icss, zone);
        if (!ret.ok()) {
            log_db::error("chlUnRegDataObj - failed in getLocalZone with status [{}]", ret.code());
            return PASS(ret);
        }

        /* delete the access rows, if we just deleted the last replica */
        if (dataObjNumber[0] != '\0') {
            log_sql::debug("chlUnregDataObj SQL 3");
            const auto remaining_replicas = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({gq2::builder::count("data_id")})
                    .from("DATA_OBJECT")
                    .where(col("data_id") == dataObjNumber)
                    .build());
            if (remaining_replicas.value_or(0) == 0) {
                auto rem_access = gq2::builder::remove_from("ACCESS")
                    .where(col("object_id") == dataObjNumber)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, rem_access);

                if (const auto ec = removeMetaMapAndAVU(dataObjNumber); ec < 0) {
                    log_db::warn("[{}:{}] - failed to remove associated AVUs [ec=[{}]]", __func__, __LINE__, ec);
                }
            }
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }

} // db_unreg_replica_op

// =-=-=-=-=-=-=-
//
irods::error db_reg_rule_exec_op(
    irods::plugin_context& _ctx,
    ruleExecSubmitInp_t*   _re_sub_inp ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_re_sub_inp ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    char myTime[50];
    rodsLong_t seqNum;

    log_sql::debug("chlRegRuleExec");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlRegRuleExec SQL 1 ");

        seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
        if ( seqNum < 0 ) {
            log_db::info("chlRegRuleExec get_next_sequence_value failure {}", seqNum);
            return ERROR( seqNum, "get_next_sequence_value failure" );
        }
        const auto ruleExecIdNum = std::to_string( seqNum );

        /* store as output parameter */
        rstrcpy( _re_sub_inp->ruleExecId, ruleExecIdNum.c_str(), NAME_LEN );

        getNowStr( myTime );

        const irods::experimental::key_value_proxy kvp{_re_sub_inp->condInput};
        const std::string exe_context_str = kvp.contains(RULE_EXECUTION_CONTEXT_KW)
            ? std::string{kvp[RULE_EXECUTION_CONTEXT_KW].value()}
            : std::string{};

        log_sql::debug("chlRegRuleExec SQL 2");
        namespace gq2 = irods::experimental::genquery2;
        auto insert_stmt = gq2::builder::insert_into("RULE_EXEC")
            .set("rule_exec_id", ruleExecIdNum)
            .set("rule_name", _re_sub_inp->ruleName)
            .set("rei_file_path", _re_sub_inp->reiFilePath)
            .set("user_name", _re_sub_inp->userName)
            .set("exe_address", _re_sub_inp->exeAddress)
            .set("exe_time", _re_sub_inp->exeTime)
            .set("exe_frequency", _re_sub_inp->exeFrequency)
            .set("priority", _re_sub_inp->priority)
            .set("estimated_exe_time", _re_sub_inp->estimateExeTime)
            .set("notification_addr", _re_sub_inp->notificationAddr)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .set("exe_context", exe_context_str)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, insert_stmt);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_reg_rule_exec_op

// =-=-=-=-=-=-=-
// Modify an existing rule in the catalog.
irods::error db_mod_rule_exec_op(
    irods::plugin_context& _ctx,
    const char*            _re_id,
    keyValPair_t*          _reg_param )
{
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (!_re_id  || !_reg_param ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    // =-=-=-=-=-=-=-
    //
    int i;
    char *theVal = 0;

    /* regParamNames has the argument names (in regParam) that this
       routine understands and colNames has the corresponding column
       names; one for one. */
    char* regParamNames[] = {RULE_NAME_KW,
                             RULE_REI_FILE_PATH_KW,
                             RULE_USER_NAME_KW,
                             RULE_EXE_ADDRESS_KW,
                             RULE_EXE_TIME_KW,
                             RULE_EXE_FREQUENCY_KW,
                             RULE_PRIORITY_KW,
                             RULE_ESTIMATE_EXE_TIME_KW,
                             RULE_NOTIFICATION_ADDR_KW,
                             RULE_LAST_EXE_TIME_KW,
                             RULE_EXE_STATUS_KW,
                             RULE_EXECUTION_CONTEXT_KW,
                             RULE_LOCK_HOST_KW,
                             RULE_LOCK_HOST_PID_KW,
                             RULE_LOCK_TIME_KW,
                             "END"};

    char* colNames[] = {"rule_name",
                        "rei_file_path",
                        "user_name",
                        "exe_address",
                        "exe_time",
                        "exe_frequency",
                        "priority",
                        "estimated_exe_time",
                        "notification_addr",
                        "last_exe_time",
                        "exe_status",
                        "exe_context",
                        "lock_host",
                        "lock_host_pid",
                        "lock_ts",

                        // The following columns are handled automatically.
                        // ** New columns MUST be added before these lines! **
                        "create_ts",
                        "modify_ts"};

    log_sql::debug("chlModRuleExec");

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    auto update_builder = gq2::builder::update("RULE_EXEC");
    bool has_updates = false;

    for ( i = 0; strcmp( regParamNames[i], "END" ); i++ ) {
        theVal = getValByKey( _reg_param, regParamNames[i] );

        if (theVal) {
            if (std::string_view{regParamNames[i]} == RULE_PRIORITY_KW) {
                if (std::strlen(theVal) == 0) {
                    return ERROR(SYS_INVALID_INPUT_PARAM,
                                 "Delay rule priority cannot be empty. Delay rule priority must "
                                 "satisfy the following requirement: 1 <= P <= 9.");
                }

                try {
                    if (const auto p = std::stoi(theVal); p < 1 || p > 9) {
                        return ERROR(SYS_INVALID_INPUT_PARAM,
                                     "Delay rule priority must satisfy the following requirement: 1 <= P <= 9.");
                    }
                }
                catch (...) {
                    return ERROR(SYS_INVALID_INPUT_PARAM, "Delay rule priority is not an integer.");
                }
            }

            update_builder.set(colNames[i], theVal);
            has_updates = true;
        }
    }

    if ( !has_updates ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid argument" );
    }

    auto stmt = update_builder.where(col("rule_exec_id") == _re_id).build();

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlModRuleExec SQL 1 ");
        irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_mod_rule_exec_op

irods::error db_del_rule_exec_op(irods::plugin_context& _ctx, const char* _re_id)
{
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if (!_re_id) {
        return ERROR(CAT_INVALID_ARGUMENT, "null parameter");
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        log_sql::debug("chlDelRuleExec SQL 2 ");
        auto del_re = gq2::builder::remove_from("RULE_EXEC")
            .where(col("rule_exec_id") == _re_id)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_re);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_del_rule_exec_op

static irods::error extract_resource_properties_for_operations(
    const std::string& _resc_name,
    std::string& _resc_id,
    std::string& _resc_parent ) {

    irods::resource_ptr resc;
    irods::error ret = resc_mgr.resolve(
                           _resc_name,
                           resc);
    if(!ret.ok()) {
        return PASS(ret);
    }

    ret = resc->get_property<std::string>(
                    irods::RESOURCE_PARENT,
                    _resc_parent);
    if(!ret.ok()) {
        return PASS(ret);
    }

    rodsLong_t resc_id;
    ret = resc->get_property<rodsLong_t>(
                    irods::RESOURCE_ID,
                    resc_id);
    if(!ret.ok()) {
        return PASS(ret);
    }

    try {
        _resc_id = boost::lexical_cast<std::string>(resc_id);
    } catch( boost::bad_lexical_cast& ) {
        std::stringstream msg;
        msg << "failed to cast " << resc_id;
        return ERROR(
                INVALID_LEXICAL_CAST,
                msg.str());
    }

    return SUCCESS();

} // extract_resource_properties_for_operations

irods::error db_add_child_resc_op(
    irods::plugin_context& _ctx,
    std::map<std::string, std::string> *_resc_input ) {
    // =-=-=-=-=-=-=-
    // for readability
    std::map<std::string, std::string>& resc_input = *_resc_input;

    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    std::string new_child_string( resc_input[irods::RESOURCE_CHILDREN] );

    irods::children_parser child_parser;
    child_parser.set_string( new_child_string );

    irods::children_parser::children_map_t c_map;
    child_parser.list( c_map );

    if(c_map.empty()) {
       return ERROR(
                  SYS_INVALID_INPUT_PARAM,
                  "child map is empty" );
    }

    std::string child_name    = c_map.begin()->first;
    std::string child_context = c_map.begin()->second;

    std::string child_resource_id;
    std::string child_parent_name;
    ret = extract_resource_properties_for_operations(
              child_name,
              child_resource_id,
              child_parent_name);

    if(!ret.ok()) {
	if( SYS_RESC_DOES_NOT_EXIST == ret.code() ) {
	    return ERROR(
                       CHILD_NOT_FOUND,
                       child_parent_name.c_str());
	}
        return PASS(ret);
    }

    std::string& parent_name = resc_input[irods::RESOURCE_NAME];
    std::string parent_resource_id;
    std::string parent_parent_name;
    ret = extract_resource_properties_for_operations(
              parent_name,
              parent_resource_id,
              parent_parent_name);
    if(!ret.ok()) {
        if( SYS_RESC_DOES_NOT_EXIST == ret.code() ) {
            return ERROR(
                       CAT_INVALID_RESOURCE,
                       child_parent_name.c_str());
        }
        return PASS(ret);
    }

    int status = _canConnectToCatalog( _ctx.comm() );
    if(0 != status) {
        return ERROR(
                   status,
                   "_canConnectToCatalog failed");
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if(!ret.ok()) {
        return PASS(ret);
    }

    if(resc_input[irods::RESOURCE_ZONE].length() > 0 &&
       resc_input[irods::RESOURCE_ZONE] != zone ) {
        addRErrorMsg(
            &_ctx.comm()->rError, 0,
            "Currently, resources must be in the local zone" );

        return ERROR(
                   CAT_INVALID_ZONE,
                   "resources must be in the local zone");
    }

    ret = update_child_parent(child_resource_id, parent_resource_id, child_context);
    if(!ret.ok()) {
        return PASS(ret);
    }

    return SUCCESS();
} // db_add_child_resc_op

// =-=-=-=-=-=-=-
//
irods::error db_reg_resc_op(
    irods::plugin_context& _ctx,
    std::map<std::string, std::string> *_resc_input ) {

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_resc_input ) {
        return ERROR( SYS_INTERNAL_NULL_INPUT_ERR, "NULL parameter" );
    }

    // =-=-=-=-=-=-=-
    // for readability
    std::map<std::string, std::string>& resc_input = *_resc_input;

    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    rodsLong_t seqNum;

    log_sql::debug("chlRegResc");

    // =-=-=-=-=-=-=-
    // error trap empty resc name
    if ( resc_input[irods::RESOURCE_NAME].length() < 1 ) {
        addRErrorMsg( &_ctx.comm()->rError, 0, "resource name is empty" );
        return ERROR( CAT_INVALID_RESOURCE_NAME, "resource name is empty" );
    }

    // =-=-=-=-=-=-=-
    // error trap empty resc type
    if ( resc_input[irods::RESOURCE_TYPE].length() < 1 ) {
        addRErrorMsg( &_ctx.comm()->rError, 0, "resource type is empty" );
        return ERROR( CAT_INVALID_RESOURCE_TYPE, "resource type is empty" );
    }

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    // =-=-=-=-=-=-=-
    // Validate resource name format
    ret = validate_resource_name( resc_input[irods::RESOURCE_NAME] );
    if ( !ret.ok() ) {
        log_db::error(ret.result());
        return PASS( ret );
    }
    // =-=-=-=-=-=-=-


    log_sql::debug("chlRegResc SQL 1 ");
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
    }
    catch (const std::exception& e) {
        log_db::info("chlRegResc get_next_sequence_value failure {}", e.what());
        _rollback( "chlRegResc" );
        return ERROR( CAT_SQL_ERR, "get_next_sequence_value failure" );
    }
    if ( seqNum < 0 ) {
        log_db::info("chlRegResc get_next_sequence_value failure {}", seqNum);
        _rollback( "chlRegResc" );
        return ERROR( seqNum, "get_next_sequence_value failure" );
    }
    const auto idNum = std::to_string( seqNum );

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );

    }

    if ( resc_input[irods::RESOURCE_ZONE].length() > 0 ) {
        if ( resc_input[irods::RESOURCE_ZONE] != zone ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          "Currently, resources must be in the local zone" );
            return ERROR( CAT_INVALID_ZONE, "resources must be in the local zone" );
        }
    }
    // =-=-=-=-=-=-=-
    // JMC :: resources may now have an empty location if they
    //     :: are coordinating nodes
    //    if (strlen(_resc_info->rescLoc)<1) {
    //        return(CAT_INVALID_RESOURCE_NET_ADDR);
    //    }
    // =-=-=-=-=-=-=-
    // if the resource is not the 'empty resource' test it
    if ( resc_input[irods::RESOURCE_LOCATION] != irods::EMPTY_RESC_HOST ) {
        // =-=-=-=-=-=-=-

        _resolveHostName( _ctx.comm(), resc_input[irods::RESOURCE_LOCATION].c_str());
    }

    // Root dir is not a valid vault path
    ret = verify_non_root_vault_path(_ctx, resc_input[irods::RESOURCE_PATH]);
    if (!ret.ok()) {
        return PASS(ret);
    }

    const auto [current_time_secs, current_time_msecs] = get_current_time();

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlRegResc SQL 4");
        namespace gq2 = irods::experimental::genquery2;
        auto insert_stmt = gq2::builder::insert_into("RESOURCE")
            .set("resc_id", idNum)
            .set("resc_name", resc_input[irods::RESOURCE_NAME])
            .set("zone_name", zone)
            .set("resc_type_name", resc_input[irods::RESOURCE_TYPE])
            .set("resc_class_name", resc_input[irods::RESOURCE_CLASS])
            .set("resc_net", resc_input[irods::RESOURCE_LOCATION])
            .set("resc_def_path", resc_input[irods::RESOURCE_PATH])
            .set("create_ts", current_time_secs)
            .set("modify_ts", current_time_secs)
            .set("modify_ts_millis", current_time_msecs)
            .set("resc_children", resc_input[irods::RESOURCE_CHILDREN])
            .set("resc_context", resc_input[irods::RESOURCE_CONTEXT])
            .set("resc_parent", resc_input[irods::RESOURCE_PARENT])
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, insert_stmt);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_reg_resc_op

// =-=-=-=-=-=-=-
//
irods::error db_del_child_resc_op(
    irods::plugin_context& _ctx,
    std::map<std::string, std::string> *_resc_input ) {

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_resc_input ) {
        return ERROR( SYS_INTERNAL_NULL_INPUT_ERR, "NULL parameter" );
    }

    // =-=-=-=-=-=-=-
    // for readability
    std::map<std::string, std::string>& resc_input = *_resc_input;

    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    std::string child_string( resc_input[irods::RESOURCE_CHILDREN] );

    std::string& parent_name = resc_input[irods::RESOURCE_NAME];
    std::string parent_resource_id;
    std::string parent_parent_resource_id;
    ret = extract_resource_properties_for_operations(
              parent_name,
              parent_resource_id,
              parent_parent_resource_id);
    if(!ret.ok()) {
           return PASS(ret);
    }

    irods::children_parser parser;
    parser.set_string( child_string );

    std::string child_name;
    parser.first_child( child_name );

    std::string child_resource_id;
    std::string child_parent_resource_id;
    ret = extract_resource_properties_for_operations(
              child_name,
              child_resource_id,
              child_parent_resource_id);
    if(!ret.ok()) {
           return PASS(ret);
    }

    if (child_parent_resource_id != parent_resource_id) {
        return ERROR(CAT_INVALID_CHILD, "invalid parent/child relationship");
    }

    int status = _canConnectToCatalog( _ctx.comm() );
    if(0 != status) {
        return ERROR(
                   status,
                   "_canConnectToCatalog failed");
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if(!ret.ok()) {
        return PASS(ret);
    }

    ret = update_child_parent(child_resource_id, std::string(""), std::string(""));
    if(!ret.ok()) {
        return PASS(ret);
    }

    return SUCCESS();
} // db_del_child_resc_op

// =-=-=-=-=-=-=-
// delete a resource
irods::error db_del_resc_op(
    irods::plugin_context& _ctx,
    const char *_resc_name,
    int                    _dry_run ) {

    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_resc_name ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    int status;

    log_sql::debug("chlDelResc");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    // =-=-=-=-=-=-=-

    if ( strncmp( _resc_name, BUNDLE_RESC, strlen( BUNDLE_RESC ) ) == 0 ) {
        addRErrorMsg( &_ctx.comm()->rError, 0,
            fmt::format("{} is a built-in resource needed for bundle operations.", BUNDLE_RESC).c_str() );
        return ERROR( CAT_PSEUDO_RESC_MODIFY_DISALLOWED, "cannot delete bundle resc" );
    }
    // =-=-=-=-=-=-=-

    bool has_data = true; // default to error case
    status = _rescHasData( &icss, _resc_name, has_data );
    if( status < 0 ) {
        log_db::error("{} - _rescHasData failed for [{}] {}", __FUNCTION__, _resc_name, status);
        return ERROR(
                  status,
                  "failed to get object count for resource" );
    }

    if( has_data   ) {
        addRErrorMsg( &_ctx.comm()->rError, 0,
            fmt::format("resource '{}' contains one or more dataObjects", _resc_name).c_str() );
        return ERROR( CAT_RESOURCE_NOT_EMPTY, "resc not empty" );
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlDelResc SQL 2 ");
    std::string rescId;
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto opt_id = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"resc_id"})
                .from("RESOURCE")
                .where(col("resc_name") == _resc_name)
                .build());
        if (!opt_id) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                fmt::format("resource '{}' does not exist", _resc_name).c_str() );
            return ERROR( CAT_SUCCESS_BUT_WITH_NO_INFO, "resource does not exist" );
        }
        rescId = *opt_id;
    }
    catch (const std::exception& e) {
        log_db::error("chlDelResc select resc_id failure {}", e.what());
        _rollback( "chlDelResc" );
        return ERROR( CAT_SQL_ERR, "resource does not exist" );
    }

    if ( _rescHasParentOrChild( rescId.c_str() ) ) {
        addRErrorMsg( &_ctx.comm()->rError, 0,
            fmt::format("resource '{}' has a parent or child", _resc_name).c_str() );
        return ERROR( CHILD_EXISTS, "resource has a parent or child" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        log_sql::debug("chlDelResc SQL 3");
        auto del_resc = gq2::builder::remove_from("RESOURCE")
            .where(col("resc_name") == _resc_name)
            .build();
        const auto affected = irods::experimental::catalog::execute_catalog(executor, db_conn, del_resc).affected_rows;
        if ( affected == 0 ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                fmt::format("resource '{}' does not exist", _resc_name).c_str() );
            return ERROR( CAT_SUCCESS_BUT_WITH_NO_INFO, "resource does not exist" );
        }

        /* Remove associated AVUs, if any */
        if (const auto ec = removeMetaMapAndAVU(rescId.c_str()); ec < 0) {
            log_db::warn("[{}:{}] - failed to remove associated AVUs [ec=[{}]]", __func__, __LINE__, ec);
        }

        if ( _dry_run ) {
            trans.rollback();
            return SUCCESS();
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_del_resc_op

// =-=-=-=-=-=-=-
// rollback the db
irods::error db_rollback_op(
    irods::plugin_context& _ctx ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlRollback - SQL 1 ");

    int status = db_execute_no_answer_sql( "rollback", &icss );
    if ( status != 0 ) {
        log_db::info("chlRollback rollback failure {}", status);
        return ERROR( status, "chlRollback rollback failure" );
    }

    return CODE( status );

} // db_rollback_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_commit_op(
    irods::plugin_context& _ctx ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlCommit - SQL 1 ");
    int status = db_execute_no_answer_sql( "commit", &icss );
    if ( status != 0 ) {
        log_db::info("chlCommit commit failure {}", status);
        return ERROR( status, "chlCommit commit failure" );
    }

    return CODE( status );

} // db_commit_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_del_user_re_op(
    irods::plugin_context& _ctx,
    userInfo_t*            _user_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_user_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    int status;
    char iValStr[200];
    char zoneToUse[MAX_NAME_LEN];
    char userName2[NAME_LEN];
    char zoneName[NAME_LEN];

    log_sql::debug("chlDelUserRE");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    snprintf( zoneToUse, sizeof( zoneToUse ), "%s", zone.c_str() );
    if ( strlen( _user_info->rodsZone ) > 0 ) {
        snprintf( zoneToUse, sizeof( zoneToUse ), "%s", _user_info->rodsZone );
    }

    status = validateAndParseUserName( _user_info->userName, userName2, zoneName );
    if ( status ) {
        return ERROR( status, "Invalid username format" );
    }
    if ( zoneName[0] != '\0' ) {
        rstrcpy( zoneToUse, zoneName, NAME_LEN );
    }

    if ( strncmp( _ctx.comm()->clientUser.userName, userName2, sizeof( userName2 ) ) == 0 &&
            strncmp( _ctx.comm()->clientUser.rodsZone, zoneToUse, sizeof( zoneToUse ) ) == 0 ) {
        addRErrorMsg( &_ctx.comm()->rError, 0, "Cannot remove your own admin account, probably unintended" );
        return ERROR( CAT_INVALID_USER, "invalid user" );
    }


    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlDelUserRE SQL 1 ");
        auto opt_user_id = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName2 && col("zone_name") == zoneToUse)
                .build());
        if (!opt_user_id) {
            addRErrorMsg( &_ctx.comm()->rError, 0, "Invalid user" );
            return ERROR( CAT_INVALID_USER, "invalid user" );
        }
        rstrcpy(iValStr, opt_user_id->c_str(), sizeof(iValStr));

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        log_sql::debug("chlDelUserRE SQL 2");
        auto del_user = gq2::builder::remove_from("USER")
            .where(col("user_name") == userName2 && col("zone_name") == zoneToUse)
            .build();
        const auto affected = irods::experimental::catalog::execute_catalog(executor, db_conn, del_user).affected_rows;
        if ( affected == 0 ) {
            return ERROR( CAT_INVALID_USER, "invalid user" );
        }

        log_sql::debug("chlDelUserRE SQL 3");
        auto del_pw = gq2::builder::remove_from("USER_PASSWORD")
            .where(col("user_id") == iValStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_pw);

        auto del_access = gq2::builder::remove_from("ACCESS")
            .where(col("user_id") == iValStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_access);

        log_sql::debug("chlDelUserRE SQL 4");
        auto del_ug = gq2::builder::remove_from("USER_GROUP")
            .where(col("user_id") == iValStr || col("group_user_id") == iValStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_ug);

        log_sql::debug("chlDelUserRE SQL 5");
        auto del_auth = gq2::builder::remove_from("USER_AUTH")
            .where(col("user_id") == iValStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_auth);

        /* Remove associated AVUs, if any */
        if (const auto ec = removeMetaMapAndAVU(iValStr); ec < 0) {
            log_db::warn("[{}:{}] - failed to remove associated AVUs [ec=[{}]]", __func__, __LINE__, ec);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_del_user_re_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_reg_coll_by_admin_op(
    irods::plugin_context& _ctx,
    collInfo_t*            _coll_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_coll_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    char myTime[50];
    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];
    int status;
    char userName[NAME_LEN];
    char zoneName[NAME_LEN];

    log_sql::debug("chlRegCollByAdmin");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    // =-=-=-=-=-=-=-

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ||
            _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        int status2 = irods::experimental::catalog::access_control::check_group_admin_access(
            executor, db_conn,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            "" );
        if ( status2 != 0 ) {
            return ERROR( status2, "no group admin access" );
        }
        if ( creatingUserByGroupAdmin == 0 ) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }
        // =-=-=-=-=-=-=-
    }

    if (const auto ec = splitPathByKey(_coll_info->collName, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
        return ERROR(ec, fmt::format(
                     "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                     __func__, __LINE__, _coll_info->collName, ec));
    }

    if ( strlen( logicalParentDirName ) == 0 ) {
        snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
        snprintf( logicalEndName, sizeof( logicalEndName ), "%s", _coll_info->collName + 1 );
    }

    /* Check that the parent collection exists */
    log_sql::debug("chlRegCollByAdmin SQL 1 ");
    auto opt_parent_id = irods::experimental::catalog::query_catalog_integer(
        executor,
        db_conn,
        gq2::builder::select({"coll_id"})
            .from("COLLECTION")
            .where(col("coll_name") == logicalParentDirName)
            .build());
    if (!opt_parent_id) {
        addRErrorMsg( &_ctx.comm()->rError, 0,
            fmt::format("collection '{}' is unknown, cannot create {} under it",
                        logicalParentDirName, logicalEndName).c_str() );
        return ERROR( CAT_NO_ROWS_FOUND, "collection is unknown" );
    }

    /* Get next sequence item for objects */
    const auto seq_val = executor.get_next_sequence_value(db_conn, "R_ObjectID");
    if (seq_val < 0) {
        log_db::info("chlRegCollByAdmin get_next_sequence_value failure {}", seq_val);
        return ERROR(seq_val, "get_next_sequence_value failure");
    }
    const std::string new_coll_id_str = std::to_string(seq_val);

    getNowStr( myTime );

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    /* Parse input name into user and zone */
    status = validateAndParseUserName( _coll_info->collOwnerName, userName, zoneName );
    if ( status ) {
        return ERROR( status, "Invalid username format" );
    }
    if ( zoneName[0] == '\0' ) {
        rstrcpy( zoneName, zone.c_str(), NAME_LEN );
    }

    const std::string owner_zone = strlen(_coll_info->collOwnerZone) > 0 ? _coll_info->collOwnerZone : zoneName;

    try {
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlRegCollByAdmin SQL 2");
        namespace gq2 = irods::experimental::genquery2;
        auto insert_coll = gq2::builder::insert_into("COLLECTION")
            .set("coll_id", new_coll_id_str)
            .set("parent_coll_name", logicalParentDirName)
            .set("coll_name", _coll_info->collName)
            .set("coll_owner_name", userName)
            .set("coll_owner_zone", owner_zone)
            .set("coll_type", _coll_info->collType)
            .set("coll_info1", _coll_info->collInfo1)
            .set("coll_info2", _coll_info->collInfo2)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, insert_coll);

        log_sql::debug("chlRegCollByAdmin SQL 4");
        const auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName && col("zone_name") == zoneName)
                .build());
        if (!opt_user_id) {
            return ERROR(CAT_INVALID_USER, "user not found");
        }
        const auto opt_token_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"token_id"})
                .from("TOKEN")
                .where(col("token_namespace") == "access_type" && col("token_name") == ACCESS_OWN)
                .build());
        if (!opt_token_id) {
            return ERROR(CAT_INVALID_ARGUMENT, "access token not found");
        }
        auto ins_acc = gq2::builder::insert_into("ACCESS")
            .set("object_id", new_coll_id_str)
            .set("user_id", std::to_string(*opt_user_id))
            .set("access_type_id", std::to_string(*opt_token_id))
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_acc);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __func__, e.what());
        return ERROR(CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME");
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __func__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }

} // db_reg_coll_by_admin_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_reg_coll_op(
    irods::plugin_context& _ctx,
    collInfo_t*            _coll_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_coll_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    char myTime[50];
    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];
    rodsLong_t status;
    int inheritFlag;

    log_sql::debug("chlRegColl");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    if (const auto ec = splitPathByKey(_coll_info->collName, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
        return ERROR(ec, fmt::format(
                     "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                     __func__, __LINE__, _coll_info->collName, ec));
    }

    if ( strlen( logicalParentDirName ) == 0 ) {
        snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
        snprintf( logicalEndName, sizeof( logicalEndName ), "%s", _coll_info->collName + 1 );
    }

    /* Check that the parent collection exists and user has write permission,
       and get the collectionID.  Also get the inherit flag */
    log_sql::debug("chlRegColl SQL 1 ");

    {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        status = irods::experimental::catalog::access_control::check_collection_access_and_inherit(
            executor, db_conn,
            logicalParentDirName,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_MODIFY_OBJECT, &inheritFlag,
            mySessionTicket, mySessionClientAddr );
    }
    if ( status < 0 ) {
        if ( status == CAT_UNKNOWN_COLLECTION ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "collection '{}' is unknown", logicalParentDirName ).c_str() );
            return ERROR( status, "collection is unknown" );
        }
        _rollback( "chlRegColl" );
        return ERROR( status, "check_collection_access_and_inherit failed" );
    }
    const auto collIdNum = std::to_string( status );

    /* Check that the path is not already a dataObj */
    log_sql::debug("chlRegColl SQL 2");
    {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        auto opt_data_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"data_id"})
                .from("DATA_OBJECT")
                .where(col("data_name") == logicalEndName && col("coll_id") == collIdNum)
                .build());
        if ( opt_data_id ) {
            return ERROR( CAT_NAME_EXISTS_AS_DATAOBJ, "data obj already exists" );
        }

        const auto seq_val = executor.get_next_sequence_value(db_conn, "R_ObjectID");
        if ( seq_val < 0 ) {
            log_db::info("chlRegColl get_next_sequence_value failure {}", seq_val);
            return ERROR( seq_val, "get_next_sequence_value failure" );
        }
        status = seq_val;
    }

    const rodsLong_t new_coll_id = status;
    const std::string new_coll_id_str = std::to_string( new_coll_id );

    getNowStr( myTime );

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlRegColl SQL 3");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto insert_coll = gq2::builder::insert_into("COLLECTION")
            .set("coll_id", new_coll_id_str)
            .set("parent_coll_name", logicalParentDirName)
            .set("coll_name", _coll_info->collName)
            .set("coll_owner_name", _ctx.comm()->clientUser.userName)
            .set("coll_owner_zone", _ctx.comm()->clientUser.rodsZone)
            .set("coll_type", _coll_info->collType)
            .set("coll_info1", _coll_info->collInfo1)
            .set("coll_info2", _coll_info->collInfo2)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, insert_coll);

        if ( inheritFlag ) {
            log_sql::debug("chlRegColl SQL 4");
            auto q_res = irods::experimental::catalog::execute_catalog(
                executor, db_conn,
                gq2::builder::select({"user_id", "access_type_id"})
                    .from("ACCESS")
                    .where(col("object_id") == collIdNum)
                    .build());
            if (q_res.query_result) {
                while (q_res.query_result->next()) {
                    const auto uid = q_res.query_result->get<std::string>(0);
                    const auto atid = q_res.query_result->get<std::string>(1);
                    auto ins_acc = gq2::builder::insert_into("ACCESS")
                        .set("object_id", new_coll_id_str)
                        .set("user_id", uid)
                        .set("access_type_id", atid)
                        .set("create_ts", myTime)
                        .set("modify_ts", myTime)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins_acc);
                }
            }

            log_sql::debug("chlRegColl SQL 5");
            auto update_inherit = gq2::builder::update("COLLECTION")
                .set("coll_inheritance", "1")
                .set("modify_ts", myTime)
                .where(col("coll_id") == new_coll_id_str)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, update_inherit);
        }
        else {
            log_sql::debug("chlRegColl SQL 6");
            const auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                    .build());
            if (!opt_user_id) {
                return ERROR(CAT_INVALID_USER, "user not found");
            }
            const auto opt_token_id = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"token_id"})
                    .from("TOKEN")
                    .where(col("token_namespace") == "access_type" && col("token_name") == ACCESS_OWN)
                    .build());
            if (!opt_token_id) {
                return ERROR(CAT_INVALID_ARGUMENT, "access token not found");
            }
            auto ins_acc = gq2::builder::insert_into("ACCESS")
                .set("object_id", new_coll_id_str)
                .set("user_id", std::to_string(*opt_user_id))
                .set("access_type_id", std::to_string(*opt_token_id))
                .set("create_ts", myTime)
                .set("modify_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_acc);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_reg_coll_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_mod_coll_op(
    irods::plugin_context& _ctx,
    collInfo_t*            _coll_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_coll_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    char myTime[50];
    int count;
    rodsLong_t iVal;

    log_sql::debug("chlModColl");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    /* Check that collection exists and user has write permission */
    {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        iVal = irods::experimental::catalog::access_control::check_collection_access(
            executor, db_conn,
            _coll_info->collName,  _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_MODIFY_OBJECT );
    }

    if ( iVal < 0 ) {
        if ( iVal == CAT_UNKNOWN_COLLECTION ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "collection '{}' is unknown", _coll_info->collName ).c_str() );
            return ERROR( CAT_UNKNOWN_COLLECTION, "unknown collection" );
        }

        if ( iVal == CAT_NO_ACCESS_PERMISSION ) {
            // Allows elevation of privileges (e.g. irods_rule_engine_plugin-update_collection_mtime).
            if (irods::is_privileged_client(*_ctx.comm())) {
                iVal = 0;
            }
            else {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              fmt::format( "no permission to update collection '{}'", _coll_info->collName ).c_str() );
                return ERROR( CAT_NO_ACCESS_PERMISSION, "no permission" );
            }
        }

        // If client privileges are elevated, then iVal must be checked again because
        // it could have been modified (e.g. irods_rule_engine_plugin-update_collection_mtime).
        if (iVal < 0) {
            return ERROR( iVal, "check_collection_access failed" );
        }
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    auto upd = gq2::builder::update("COLLECTION");
    count = 0;

    if ( strlen( _coll_info->collType ) > 0 ) {
        if ( strcmp( _coll_info->collType, "NULL_SPECIAL_VALUE" ) == 0 ) {
            upd.set("coll_type", "");
        }
        else {
            upd.set("coll_type", _coll_info->collType);
        }
        count++;
    }

    if ( strlen( _coll_info->collInfo1 ) > 0 ) {
        if ( strcmp( _coll_info->collInfo1, "NULL_SPECIAL_VALUE" ) == 0 ) {
            upd.set("coll_info1", "");
        }
        else {
            upd.set("coll_info1", _coll_info->collInfo1);
        }
        count++;
    }

    if ( strlen( _coll_info->collInfo2 ) > 0 ) {
        if ( strcmp( _coll_info->collInfo2, "NULL_SPECIAL_VALUE" ) == 0 ) {
            upd.set("coll_info2", "");
        }
        else {
            upd.set("coll_info2", _coll_info->collInfo2);
        }
        count++;
    }

    if (strlen(_coll_info->collModify) > 0) {
        upd.set("modify_ts", _coll_info->collModify);
        ++count;
    }
    else {
        getNowStr( myTime );
        upd.set("modify_ts", myTime);
    }

    if ( count == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "count is 0" );
    }

    upd.where(col("coll_name") == _coll_info->collName);
    auto stmt = upd.build();

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlModColl SQL 1");
        irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_mod_coll_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_reg_zone_op(
    irods::plugin_context& _ctx,
    const char*            _zone_name,
    const char*            _zone_type,
    const char*            _zone_conn_info,
    const char*            _zone_comment ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_zone_name ||
        !_zone_type ||
        !_zone_conn_info ||
        !_zone_comment ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    char myTime[50];

    log_sql::debug("chlRegZone");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    if ( strncmp( _zone_type, "remote", 6 ) != 0 ) {
        addRErrorMsg( &_ctx.comm()->rError, 0,
                      "Currently, only zones of type 'remote' are allowed" );
        return ERROR( CAT_INVALID_ARGUMENT, "Currently, only zones of type 'remote' are allowed" );
    }

    if (!irods::is_zone_name_valid(_zone_name)) {
        log_db::error("{}: Zone name [{}] does not satisfy requirements.", __func__, _zone_name);
        return ERROR(SYS_INVALID_INPUT_PARAM, "Zone name contains invalid characters");
    }

    // =-=-=-=-=-=-=-
    // validate the connection string is well formed
    ret = validate_zone_connection_string(_zone_conn_info, _ctx);
    if (!ret.ok()) {
        return ret;
    }

    getNowStr( myTime );

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        const auto seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
        if (seqNum < 0) {
            return ERROR(seqNum, "get_next_sequence_value failed");
        }
        const auto zone_id_str = std::to_string(seqNum);

        log_sql::debug("chlRegZone SQL 1 ");
        namespace gq2 = irods::experimental::genquery2;
        auto ins = gq2::builder::insert_into("ZONE")
            .set("zone_id", zone_id_str)
            .set("zone_name", _zone_name)
            .set("zone_type_name", "remote")
            .set("zone_conn_string", _zone_conn_info)
            .set("r_comment", _zone_comment)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_reg_zone_op

// =-=-=-=-=-=-=-
// modify the zone
irods::error db_mod_zone_op(
    irods::plugin_context& _ctx,
    const char*            _zone_name,
    const char*            _option,
    const char*            _option_value ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_zone_name ||
        !_option ||
        !_option_value ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    log_sql::debug("chlModZone");

    if ( *_zone_name == '\0' || *_option == '\0' || *_option_value == '\0' ) {
        return  ERROR( CAT_INVALID_ARGUMENT, "invalid argument value" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        log_sql::debug("chlModZone SQL 1 ");
        auto opt_zone_id = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"zone_id"})
                .from("ZONE")
                .where(col("zone_name") == _zone_name)
                .build());
        if (!opt_zone_id) {
            return ERROR( CAT_INVALID_ZONE, "invalid zone name" );
        }
        const auto& zoneId = *opt_zone_id;

        char myTime[50]{};
        getNowStr( myTime );
        int OK = 0;

        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        if ( strcmp( _option, "comment" ) == 0 ) {
            log_sql::debug("chlModZone SQL 3");
            auto upd = gq2::builder::update("ZONE")
                .set("r_comment", _option_value)
                .set("modify_ts", myTime)
                .where(col("zone_id") == zoneId)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
            OK = 1;
        }
        if (strcmp(_option, "conn") == 0) {
            ret = validate_zone_connection_string(_option_value, _ctx);
            if (!ret.ok()) {
                return ret;
            }

            log_sql::debug("chlModZone SQL 5");
            auto upd = gq2::builder::update("ZONE")
                .set("zone_conn_string", _option_value)
                .set("modify_ts", myTime)
                .where(col("zone_id") == zoneId)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
            OK = 1;
        }
        if ( strcmp( _option, "name" ) == 0 ) {
            if ( strcmp( _zone_name, zone.c_str() ) == 0 ) {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              "It is not valid to rename the local zone via chlModZone; iadmin should use acRenameLocalZone" );
                return ERROR( CAT_INVALID_ARGUMENT, "cannot rename localzone" );
            }

            if (!irods::is_zone_name_valid(_option_value)) {
                log_db::error("{}: Zone name [{}] does not satisfy requirements.", __func__, _option_value);
                addRErrorMsg(&_ctx.comm()->rError, 0, fmt::format("zone name is invalid [{}]", _option_value).c_str());
                return ERROR(SYS_INVALID_INPUT_PARAM, "Zone name contains invalid characters");
            }

            log_sql::debug("chlModZone SQL 5");
            auto upd = gq2::builder::update("ZONE")
                .set("zone_name", _option_value)
                .set("modify_ts", myTime)
                .where(col("zone_id") == zoneId)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
            OK = 1;
        }
        if ( OK == 0 ) {
            return ERROR( CAT_INVALID_ARGUMENT, "invalid option" );
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __func__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
} // db_mod_zone_op

// =-=-=-=-=-=-=-
// modify the zone
irods::error db_rename_coll_op(
    irods::plugin_context& _ctx,
    const char*            _old_coll,
    const char*            _new_coll ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_old_coll ||
        !_new_coll ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    /* See if the input path is a collection and the user owns it,
       and, if so, get the collectionID */
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        const auto coll_id = irods::experimental::catalog::access_control::check_collection_access(
            executor, db_conn, _old_coll,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_OWN);

        if ( coll_id < 0 ) {
            return ERROR( coll_id, "check_collection_access failed" );
        }

        /* call chlRenameObject to rename */
        const auto status = chlRenameObject( _ctx.comm(), coll_id, _new_coll );
        if ( status != 0 ) {
            return ERROR( status, "chlRenameObject failed" );
        }

        return CODE( status );
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_rename_coll_op

// =-=-=-=-=-=-=-
// modify the zone
irods::error db_mod_zone_coll_acl_op(
    irods::plugin_context& _ctx,
    const char*            _access_level,
    const char*            _user_name,
    const char*            _path_name ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_access_level ||
        !_user_name ||
        !_path_name ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    int status;
    if ( *_path_name != '/' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid path name" );
    }
    const char* cp = _path_name + 1;
    if ( strstr( cp, PATH_SEPARATOR ) != NULL ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid path name" );
    }
    status =  chlModAccessControl( _ctx.comm(), 0,
                                   _access_level,
                                   _user_name,
                                   _ctx.comm()->clientUser.rodsZone,
                                   _path_name );
    if ( !status ) {
        return ERROR( status, "chlModAccessControl failed" );
    }

    return CODE( status );

} // db_mod_zone_coll_acl_op

// =-=-=-=-=-=-=-
// modify the zone
irods::error db_rename_local_zone_op(
    irods::plugin_context& _ctx,
    const char*            _old_zone,
    const char*            _new_zone ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_old_zone ||
        !_new_zone ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    log_sql::debug("chlRenameLocalZone");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    log_sql::debug("chlRenameLocalZone SQL 1 ");

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( strcmp( zone.c_str(), _old_zone ) != 0 ) { /* not the local zone */
        return ERROR( CAT_INVALID_ARGUMENT, "not the local zone" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        /* check that the new zone does not exist */
        log_sql::debug("chlRenameLocalZone SQL 2 ");
        auto opt_zone_id = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"zone_id"})
                .from("ZONE")
                .where(col("zone_name") == _new_zone)
                .build());
        if ( opt_zone_id ) {
            return ERROR( CAT_INVALID_ZONE, "zone not found" );
        }

        char myTime[50]{};
        getNowStr( myTime );

        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        /* update coll_owner_zone in R_COLL_MAIN */
        log_sql::debug("chlRenameLocalZone SQL 3 ");
        auto upd_coll = gq2::builder::update("COLLECTION")
            .set("coll_owner_zone", _new_zone)
            .set("modify_ts", myTime)
            .where(col("coll_owner_zone") == _old_zone)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_coll);

        /* update data_owner_zone in R_DATA_MAIN */
        log_sql::debug("chlRenameLocalZone SQL 4 ");
        auto upd_data = gq2::builder::update("DATA_OBJECT")
            .set("data_owner_zone", _new_zone)
            .set("modify_ts", myTime)
            .where(col("data_owner_zone") == _old_zone)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_data);

        /* update zone_name in R_RESC_MAIN */
        log_sql::debug("chlRenameLocalZone SQL 5 ");
        auto upd_resc = gq2::builder::update("RESOURCE")
            .set("zone_name", _new_zone)
            .set("modify_ts", myTime)
            .where(col("zone_name") == _old_zone)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_resc);

        /* update rule_owner_zone in R_RULE_MAIN */
        log_sql::debug("chlRenameLocalZone SQL 6 ");
        auto upd_rule = gq2::builder::update("RULE")
            .set("rule_owner_zone", _new_zone)
            .set("modify_ts", myTime)
            .where(col("rule_owner_zone") == _old_zone)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_rule);

        /* update zone_name in R_USER_MAIN */
        log_sql::debug("chlRenameLocalZone SQL 7 ");
        auto upd_user = gq2::builder::update("USER")
            .set("zone_name", _new_zone)
            .set("modify_ts", myTime)
            .where(col("zone_name") == _old_zone)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_user);

        /* update zone_name in R_ZONE_MAIN */
        log_sql::debug("chlRenameLocalZone SQL 8 ");
        auto upd_zone = gq2::builder::update("ZONE")
            .set("zone_name", _new_zone)
            .set("modify_ts", myTime)
            .where(col("zone_name") == _old_zone)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_zone);

        trans.commit();
        return SUCCESS();
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __func__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
} // db_rename_local_zone_op

// =-=-=-=-=-=-=-
// modify the zone
irods::error db_del_zone_op(
    irods::plugin_context& _ctx,
    const char*            _zone_name ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_zone_name ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    log_sql::debug("chlDelZone");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        log_sql::debug("chlDelZone SQL 1 ");
        auto opt_zone_type = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"zone_type_name"})
                .from("ZONE")
                .where(col("zone_name") == _zone_name)
                .build());
        if ( !opt_zone_type ) {
            return ERROR( CAT_INVALID_ZONE, "invalid zone name" );
        }

        if ( *opt_zone_type != "remote" ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          "It is not permitted to remove the local zone" );
            return ERROR( CAT_INVALID_ARGUMENT, "cannot remove local zone" );
        }

        nanodbc::transaction trans{db_conn};
        log_sql::debug("chlDelZone 2");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto del_zone = gq2::builder::remove_from("ZONE")
            .where(col("zone_name") == _zone_name)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_zone);

        trans.commit();
        return SUCCESS();
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __func__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
} // db_del_zone_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_del_coll_by_admin_op(
    irods::plugin_context& _ctx,
    collInfo_t*            _coll_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_coll_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];

    if (const auto ec = splitPathByKey(_coll_info->collName, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
        return ERROR(ec, fmt::format(
                     "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                     __func__, __LINE__, _coll_info->collName, ec));
    }

    if ( strlen( logicalParentDirName ) == 0 ) {
        snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
        snprintf( logicalEndName, sizeof( logicalEndName ), "%s", _coll_info->collName + 1 );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        // get coll_id
        const auto coll_id_opt = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == _coll_info->collName)
                .build());
        if ( !coll_id_opt ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "collection '{}' is unknown", _coll_info->collName ).c_str() );
            return ERROR( CAT_UNKNOWN_COLLECTION, "unknown collection" );
        }

        const auto collIdNum = std::to_string( *coll_id_opt );

        // check that the collection is empty (subcollections and data objects)
        const auto subcoll = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("parent_coll_name") == _coll_info->collName)
                .build());
        if ( subcoll.has_value() ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "collection '{}' is not empty", _coll_info->collName ).c_str() );
            return ERROR( CAT_COLLECTION_NOT_EMPTY, "collection not empty" );
        }

        const auto data_obj = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"data_id"})
                .from("DATA_OBJECT")
                .where(col("coll_id") == collIdNum)
                .build());
        if ( data_obj.has_value() ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "collection '{}' is not empty", _coll_info->collName ).c_str() );
            return ERROR( CAT_COLLECTION_NOT_EMPTY, "collection not empty" );
        }

        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto del_access = gq2::builder::remove_from("ACCESS")
            .where(col("object_id") == collIdNum)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_access);

        if (const auto ec = removeMetaMapAndAVU(collIdNum.c_str()); ec < 0) {
            log_db::warn("[{}:{}] - failed to remove associated AVUs [ec=[{}]]", __func__, __LINE__, ec);
        }

        auto del_coll = gq2::builder::remove_from("COLLECTION")
            .where(col("coll_name") == _coll_info->collName)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_coll);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_del_coll_by_admin_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_del_coll_op(
    irods::plugin_context& _ctx,
    collInfo_t*            _coll_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_coll_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    int status;

    log_sql::debug("chlDelColl");

    status = _delColl( _ctx.comm(), _coll_info );
    if ( status != 0 ) {
        return ERROR( status, "_delColl failed" );
    }

    return SUCCESS();
} // db_del_coll_op

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_check_auth_op(
    irods::plugin_context& _ctx,
    const char*            _scheme,
    const char*            _challenge,
    const char*            _response,
    const char*            _user_name,
    int*                   _user_priv_level,
    int*                   _client_priv_level ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_challenge || !_response || !_user_name || !_user_priv_level || !_client_priv_level ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    static int prevFailure = 0;
    if ( prevFailure > 1 ) {
        if ( prevFailure > 5 ) {
            sleep( 20 );
        }
        sleep( 2 );
    }
    *_user_priv_level = NO_USER_AUTH;
    *_client_priv_level = NO_USER_AUTH;

    int hashType = HASH_TYPE_MD5;
    std::string user_name( _user_name );
    std::string::size_type pos = user_name.find( SHA1_FLAG_STRING );
    if ( std::string::npos != pos ) {
        user_name = user_name.substr( pos );
        hashType = HASH_TYPE_SHA1;
    }

    char md5Buf[CHALLENGE_LEN + MAX_PASSWORD_LEN + 2]{};
    strncpy( md5Buf, _challenge, CHALLENGE_LEN );
    snprintf( prevChalSig, sizeof prevChalSig,
              "%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x",
              ( unsigned char )md5Buf[0], ( unsigned char )md5Buf[1],
              ( unsigned char )md5Buf[2], ( unsigned char )md5Buf[3],
              ( unsigned char )md5Buf[4], ( unsigned char )md5Buf[5],
              ( unsigned char )md5Buf[6], ( unsigned char )md5Buf[7],
              ( unsigned char )md5Buf[8], ( unsigned char )md5Buf[9],
              ( unsigned char )md5Buf[10], ( unsigned char )md5Buf[11],
              ( unsigned char )md5Buf[12], ( unsigned char )md5Buf[13],
              ( unsigned char )md5Buf[14], ( unsigned char )md5Buf[15] );

    char userName2[NAME_LEN + 2]{};
    char userZone[NAME_LEN + 2]{};
    int status = validateAndParseUserName( user_name.c_str(), userName2, userZone );
    if ( status ) {
        return ERROR( status, "Invalid username format" );
    }

    char myUserZone[MAX_NAME_LEN]{};
    if ( userZone[0] == '\0' ) {
        std::string zone;
        ret = getLocalZone( _ctx.prop_map(), &icss, zone );
        if ( !ret.ok() ) {
            return PASS( ret );
        }
        snprintf( myUserZone, sizeof( myUserZone ), "%s", zone.c_str() );
    }
    else {
        snprintf( myUserZone, sizeof( myUserZone ), "%s", userZone );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        struct PasswordEntry {
            std::string rcat_password;
            std::string pass_expiry_ts;
            std::string create_ts;
            std::string modify_ts;
        };
        std::vector<PasswordEntry> passwords;

        const auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName2 && col("zone_name") == myUserZone)
                .build());
        if (opt_user_id) {
            auto q = gq2::builder::select({"rcat_password", "pass_expiry_ts", "create_ts", "modify_ts"})
                .from("USER_PASSWORD")
                .where(col("user_id") == std::to_string(*opt_user_id))
                .build();
            auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, q);
            if (res.query_result) {
                while (res.query_result->next()) {
                    passwords.push_back({
                        res.query_result->get<std::string>(0, ""),
                        res.query_result->get<std::string>(1, ""),
                        res.query_result->get<std::string>(2, ""),
                        res.query_result->get<std::string>(3, "")
                    });
                }
            }
        }

        bool isAnonymous = (strncmp(ANONYMOUS_USER, userName2, NAME_LEN) == 0);

        if (passwords.empty()) {
            if (!isAnonymous) {
                return ERROR(CAT_INVALID_USER, "select rcat_password failed");
            }
            // anonymous user, skip password check and proceed to privilege check
        }
        else {
            int OK = 0;
            std::string goodPw;
            std::string lastPw;
            std::string goodPwExpiry;
            std::string goodPwTs;
            std::string goodPwModTs;

            for (const auto& entry : passwords) {
                char cpw[MAX_PASSWORD_LEN + 10]{};
                rstrcpy(cpw, entry.rcat_password.c_str(), sizeof(cpw));
                lastPw = entry.rcat_password;

                memset(md5Buf, 0, sizeof(md5Buf));
                strncpy(md5Buf, _challenge, CHALLENGE_LEN);
                icatDescramble(cpw);
                strncpy(md5Buf + CHALLENGE_LEN, cpw, MAX_PASSWORD_LEN);

                char digest[RESPONSE_LEN + 2]{};
                obfMakeOneWayHash(hashType,
                                  reinterpret_cast<unsigned char*>(md5Buf),
                                  CHALLENGE_LEN + MAX_PASSWORD_LEN,
                                  reinterpret_cast<unsigned char*>(digest));

                for (int i = 0; i < RESPONSE_LEN; i++) {
                    if (digest[i] == '\0') {
                        digest[i]++;
                    }
                }

                const char* cp = _response;
                OK = 1;
                for (int i = 0; i < RESPONSE_LEN; i++) {
                    if (*cp++ != digest[i]) {
                        OK = 0;
                        break;
                    }
                }

                if (OK == 1) {
                    goodPw = cpw;
                    goodPwExpiry = entry.pass_expiry_ts;
                    goodPwTs = entry.create_ts;
                    goodPwModTs = entry.modify_ts;
                    break;
                }
            }

            if (OK == 0) {
                prevFailure++;
                return ERROR(CAT_INVALID_AUTHENTICATION, "invalid argument");
            }

            rodsLong_t expireTime = atoll(goodPwExpiry.c_str());
            auth_config ac{};
            if (const auto err = get_auth_config("authentication", ac); !err.ok()) {
                log_db::error("Failed to get auth configuration. [{}]", err.result());
                return err;
            }

            char myTime[50]{};
            getNowStr(myTime);
            time_t nowTime = atoll(myTime);

            if ((strncmp(goodPwExpiry.c_str(), "9999", 4) != 0) &&
                expireTime >= ac.password_min_time &&
                expireTime <= ac.password_max_time) {
                time_t modTime = atoll(goodPwModTs.c_str());
                if (modTime + expireTime < nowTime) {
                    // Expired PAM password
                    nanodbc::transaction trans{db_conn};
                    if (opt_user_id) {
                        auto del_pw = gq2::builder::remove_from("USER_PASSWORD")
                            .where(col("rcat_password") == lastPw && col("create_ts") == goodPwTs && col("user_id") == std::to_string(*opt_user_id))
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, del_pw);
                    }
                    trans.commit();
                    return ERROR(CAT_PASSWORD_EXPIRED, "password expired");
                }
            }

            int temp_password_max_time = irods::get_advanced_setting<const int>(irods::KW_CFG_MAX_TEMP_PASSWORD_LIFETIME);
            if (!std::string_view{lastPw}.starts_with(PASSWORD_SCRAMBLE_PREFIX) && expireTime < temp_password_max_time) {
                int temp_password_time = irods::get_advanced_setting<const int>(irods::KW_CFG_DEF_TEMP_PASSWORD_LIFETIME);

                time_t createTime = atoll(goodPwTs.c_str());
                bool returnExpired = false;
                if (createTime == 0 || nowTime == 0 || createTime + expireTime < nowTime) {
                    returnExpired = true;
                }

                nanodbc::transaction trans{db_conn};

                auto del_good_pw = gq2::builder::remove_from("USER_PASSWORD")
                    .where(col("rcat_password") == goodPw)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_good_pw);

                time_t pwExpireMaxCreateTime = nowTime - temp_password_time;
                char expireStrCreate[50]{};
                snprintf(expireStrCreate, sizeof(expireStrCreate), "%011d", static_cast<int>(pwExpireMaxCreateTime));

                auto del_expired = gq2::builder::remove_from("USER_PASSWORD")
                    .where(col("pass_expiry_ts") == std::to_string(temp_password_time) && col("create_ts") < expireStrCreate)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_expired);

                trans.commit();

                if (returnExpired) {
                    return ERROR(CAT_PASSWORD_EXPIRED, "password expired");
                }
            }
        }

        // Determine user type and privilege level
        const auto user_type_opt = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"user_type_name"})
                .from("USER")
                .where(col("user_name") == userName2 && col("zone_name") == myUserZone)
                .build());
        if (!user_type_opt) {
            return ERROR(CAT_INVALID_USER, "select user_type_name failed");
        }

        *_user_priv_level = LOCAL_USER_AUTH;
        if (*user_type_opt == "rodsadmin") {
            *_user_priv_level = LOCAL_PRIV_USER_AUTH;

            if (strcmp(_ctx.comm()->clientUser.userName, userName2) == 0 &&
                strcmp(_ctx.comm()->clientUser.rodsZone, userZone) == 0) {
                *_client_priv_level = LOCAL_PRIV_USER_AUTH;
            }
            else if (_ctx.comm()->clientUser.userName[0] == '\0') {
                *_client_priv_level = REMOTE_USER_AUTH;
                prevFailure = 0;
                return SUCCESS();
            }
            else {
                const auto client_type_opt = irods::experimental::catalog::query_catalog_string(
                    executor,
                    db_conn,
                    gq2::builder::select({"user_type_name"})
                        .from("USER")
                        .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                        .build());
                if (!client_type_opt) {
                    return ERROR(CAT_INVALID_CLIENT_USER, "select user_type_name failed");
                }
                *_client_priv_level = LOCAL_USER_AUTH;
                if (*client_type_opt == "rodsadmin") {
                    *_client_priv_level = LOCAL_PRIV_USER_AUTH;
                }
            }
        }

        prevFailure = 0;
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const irods::exception& e) {
        return irods::error(e);
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }

} // db_check_auth_op

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_make_temp_pw_op(
    irods::plugin_context& _ctx,
    char*                  _pw_value_to_hash,
    const char*            _other_user ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    if ( !_pw_value_to_hash || !_other_user ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    int temp_password_time;
    try {
        temp_password_time = irods::get_advanced_setting<const int>(irods::KW_CFG_DEF_TEMP_PASSWORD_LIFETIME);
    } catch ( const irods::exception& e ) {
        return irods::error(e);
    }

    bool useOtherUser = false;
    if ( strlen( _other_user ) > 0 ) {
        if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ||
             _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }
        useOtherUser = true;
    }

    char password[MAX_PASSWORD_LEN + 10]{};
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        const auto pwd_uid_opt = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                .build());
        if (!pwd_uid_opt) {
            return ERROR(CAT_INVALID_USER, "failed to get password");
        }
        const auto pwd_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"rcat_password"})
                .from("USER_PASSWORD")
                .where(col("user_id") == std::to_string(*pwd_uid_opt) && col("pass_expiry_ts") != std::to_string(temp_password_time))
                .build());
        if (!pwd_opt) {
            return ERROR(CAT_INVALID_USER, "failed to get password");
        }
        rstrcpy(password, pwd_opt->c_str(), sizeof(password));

        icatDescramble(password);

        char rBuf[200]{};
        get64RandomBytes(rBuf);
        char hashValue[50]{};
        int j = 0;
        for (int i = 0; i < 50 && j < MAX_PASSWORD_LEN - 1; i++) {
            char c = rBuf[i] & 0x7f;
            if (c < '0') {
                c += '0';
            }
            if ((c > 'a' && c < 'z') || (c > 'A' && c < 'Z') || (c > '0' && c < '9')) {
                hashValue[j++] = c;
            }
        }
        hashValue[j] = '\0';

        char md5Buf[100]{};
        snprintf(md5Buf, sizeof(md5Buf), "%s%s", hashValue, password);
        unsigned char digest[RESPONSE_LEN + 2]{};
        obfMakeOneWayHash(HASH_TYPE_DEFAULT, reinterpret_cast<unsigned char*>(md5Buf), 100, digest);

        char newPw[MAX_PASSWORD_LEN + 10]{};
        hashToStr(digest, newPw);

        snprintf(_pw_value_to_hash, MAX_PASSWORD_LEN, "%s", hashValue);

        char myTime[50]{};
        getNowStr(myTime);
        std::string myTimeExp = std::to_string(temp_password_time);

        std::string targetUser = useOtherUser ? _other_user : _ctx.comm()->clientUser.userName;

        nanodbc::transaction trans{db_conn};
        const auto target_user_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == targetUser && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                .build());
        if (!target_user_id) {
            return ERROR(CAT_INVALID_USER, "user not found");
        }

        auto ins_pw = gq2::builder::insert_into("USER_PASSWORD")
            .set("user_id", std::to_string(*target_user_id))
            .set("rcat_password", newPw)
            .set("pass_expiry_ts", myTimeExp)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_pw);
        trans.commit();

        memset(newPw, 0, MAX_PASSWORD_LEN);
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
} // db_make_temp_pw_op

namespace
{
    void delete_expired_passwords(
        irods::experimental::catalog::nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        const auth_config& _ac,
        const char* _now_time_str)
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto pw_res = irods::experimental::catalog::execute_catalog(
            _exec, _conn,
            gq2::builder::select({"user_id", "rcat_password", "pass_expiry_ts", "modify_ts"})
                .from("USER_PASSWORD")
                .build());
        if (pw_res.query_result) {
            time_t now_ts = std::atoll(_now_time_str);
            while (pw_res.query_result->next()) {
                const auto uid = pw_res.query_result->get<std::string>(0);
                const auto pw = pw_res.query_result->get<std::string>(1);
                const auto exp_str = pw_res.query_result->get<std::string>(2);
                const auto mod_str = pw_res.query_result->get<std::string>(3);
                if (!exp_str.starts_with("9999")) {
                    time_t exp_ts = std::atoll(exp_str.c_str());
                    time_t mod_ts = std::atoll(mod_str.c_str());
                    if (exp_ts >= _ac.password_min_time && exp_ts <= _ac.password_max_time && (exp_ts + mod_ts < now_ts)) {
                        auto del_stmt = gq2::builder::remove_from("USER_PASSWORD")
                            .where(col("user_id") == uid && col("rcat_password") == pw)
                            .build();
                        irods::experimental::catalog::execute_catalog(_exec, _conn, del_stmt);
                    }
                }
            }
        }
    }
} // anonymous namespace

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_make_limited_pw_op(
    irods::plugin_context& _ctx,
    int                    _ttl,
    char*                  _pw_value_to_hash ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    if ( !_pw_value_to_hash ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    int temp_password_time;
    try {
        temp_password_time = irods::get_advanced_setting<const int>(irods::KW_CFG_DEF_TEMP_PASSWORD_LIFETIME);
    } catch ( const irods::exception& e ) {
        return irods::error(e);
    }

    char password[MAX_PASSWORD_LEN + 10]{};
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        const auto pwd_uid_opt = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                .build());
        if (!pwd_uid_opt) {
            return ERROR(CAT_INVALID_USER, "get password failed");
        }
        const auto pwd_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"rcat_password"})
                .from("USER_PASSWORD")
                .where(col("user_id") == std::to_string(*pwd_uid_opt) && col("pass_expiry_ts") != std::to_string(temp_password_time))
                .build());
        if (!pwd_opt) {
            return ERROR(CAT_INVALID_USER, "get password failed");
        }
        rstrcpy(password, pwd_opt->c_str(), sizeof(password));

        icatDescramble(password);

        char rBuf[200]{};
        get64RandomBytes(rBuf);
        char hashValue[50]{};
        int j = 0;
        for (int i = 0; i < 50 && j < MAX_PASSWORD_LEN - 1; i++) {
            char c = rBuf[i] & 0x7f;
            if (c < '0') {
                c += '0';
            }
            if ((c > 'a' && c < 'z') || (c > 'A' && c < 'Z') || (c > '0' && c < '9')) {
                hashValue[j++] = c;
            }
        }
        hashValue[j] = '\0';

        char md5Buf[100]{};
        snprintf(md5Buf, sizeof(md5Buf), "%s%s", hashValue, password);
        unsigned char digest[RESPONSE_LEN + 2]{};
        obfMakeOneWayHash(HASH_TYPE_DEFAULT, reinterpret_cast<unsigned char*>(md5Buf), 100, digest);

        char newPw[MAX_PASSWORD_LEN + 10]{};
        hashToStr(digest, newPw);
        icatScramble(newPw);

        snprintf(_pw_value_to_hash, MAX_PASSWORD_LEN, "%s", hashValue);

        auth_config ac{};
        if (const auto err = get_auth_config("authentication", ac); !err.ok()) {
            log_db::error("Failed to get auth configuration. [{}]", err.result());
            return err;
        }

        int timeToLive = _ttl * 3600;
        if (timeToLive < ac.password_min_time || timeToLive > ac.password_max_time) {
            log_db::error("Invalid TTL - min time: [{}] max time:[{}] ttl: [{}]",
                          ac.password_min_time, ac.password_max_time, timeToLive);
            return ERROR(PAM_AUTH_PASSWORD_INVALID_TTL, "invalid ttl");
        }

        char myTime[50]{};
        getNowStr(myTime);

        nanodbc::transaction trans{db_conn};

        const auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                .build());
        if (!opt_user_id) {
            return ERROR(CAT_INVALID_USER, "user not found");
        }

        auto ins_pw = gq2::builder::insert_into("USER_PASSWORD")
            .set("user_id", std::to_string(*opt_user_id))
            .set("rcat_password", newPw)
            .set("pass_expiry_ts", std::to_string(timeToLive))
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_pw);

        delete_expired_passwords(executor, db_conn, ac, myTime);

        trans.commit();

        memset(newPw, 0, MAX_PASSWORD_LEN);
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
} // db_make_limited_pw_op

// =-=-=-=-=-=-=-
// authenticate user
auto db_update_pam_password_op(irods::plugin_context& _ctx,
                                const char* _user_name,
                                int _ttl,
                                const char* _test_time,
                                char** _password_buffer,
                                std::size_t _password_buffer_size) -> irods::error
{
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    if (!_user_name || !_password_buffer) {
        return ERROR(CAT_INVALID_ARGUMENT, "null parameter");
    }

    std::array<char, MAX_PASSWORD_LEN + 1> password_in_database_buffer{};
    if (password_in_database_buffer.size() > _password_buffer_size) {
        return ERROR(
            SYS_INVALID_INPUT_PARAM,
            fmt::format("{}: Buffer not large enough to hold password. Requires [{}] bytes, received [{}] bytes.",
                        __func__,
                        password_in_database_buffer.size(),
                        _password_buffer_size));
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    char myTime[50]{};
    getNowStr( myTime );
    if ( _test_time != NULL && strlen( _test_time ) > 0 ) {
        snprintf( myTime, sizeof( myTime ), "%s", _test_time );
    }

    auth_config ac{};
    if (const auto err = get_auth_config("authentication", ac); !err.ok()) {
        log_db::error("Failed to get auth configuration. [{}]", err.result());
        return err;
    }

    std::string expTime;
    if ( _ttl == 0 ) {
        expTime = std::to_string(ac.password_min_time);
    }
    else {
        _ttl = _ttl * 3600;
        if (_ttl < ac.password_min_time || _ttl > ac.password_max_time) {
            return ERROR( PAM_AUTH_PASSWORD_INVALID_TTL, "pam ttl invalid" );
        }
        expTime = std::to_string(_ttl);
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        // get user id
        const auto user_id_opt = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _user_name && col("zone_name") == zone && col("user_type_name") != "rodsgroup")
                .build());
        if (!user_id_opt) {
            return ERROR(CAT_INVALID_USER, "invalid user");
        }
        const std::string selUserId = *user_id_opt;

        // first delete any that are expired
        nanodbc::transaction trans{db_conn};

        delete_expired_passwords(executor, db_conn, ac, myTime);

        // check if existing valid PAM password exists
        auto sel_res = irods::experimental::catalog::execute_catalog(
            executor, db_conn,
            gq2::builder::select({"rcat_password", "pass_expiry_ts"})
                .from("USER_PASSWORD")
                .where(col("user_id") == selUserId)
                .build());

        std::string existing_pw;
        bool has_valid_pam = false;
        if (sel_res.query_result) {
            while (sel_res.query_result->next()) {
                auto pw = sel_res.query_result->get<std::string>(0);
                auto exp_str = sel_res.query_result->get<std::string>(1);
                if (!exp_str.starts_with("9999")) {
                    auto exp_val = std::atoll(exp_str.c_str());
                    if (exp_val >= ac.password_min_time && exp_val <= ac.password_max_time) {
                        existing_pw = pw;
                        has_valid_pam = true;
                        break;
                    }
                }
            }
        }

        if (has_valid_pam) {
            if (ac.password_extend_lifetime) {
                auto upd_pw = gq2::builder::update("USER_PASSWORD")
                    .set("modify_ts", myTime)
                    .set("pass_expiry_ts", expTime)
                    .where(col("user_id") == selUserId && col("rcat_password") == existing_pw)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_pw);
            }
            trans.commit();

            rstrcpy(password_in_database_buffer.data(), existing_pw.c_str(), password_in_database_buffer.size());
            icatDescramble(password_in_database_buffer.data());
            std::strncpy(*_password_buffer, password_in_database_buffer.data(), _password_buffer_size);
            return SUCCESS();
        }

        constexpr auto random_password_len = MAX_PASSWORD_LEN - 8;
        if (random_password_len + 1 > _password_buffer_size) {
            return ERROR(
                SYS_INVALID_INPUT_PARAM,
                fmt::format("{}: Buffer not large enough to hold password. Requires [{}] bytes, received [{}] bytes.",
                            __func__,
                            random_password_len + 1,
                            _password_buffer_size));
        }

        std::array<char, MAX_PASSWORD_LEN + 1> scrambled_random_password{};
        const auto random_password = irods::generate_random_alphanumeric_string(random_password_len);
        std::strncpy(scrambled_random_password.data(), random_password.c_str(), random_password_len + 1);
        icatScramble(scrambled_random_password.data());

        auto ins_pw = gq2::builder::insert_into("USER_PASSWORD")
            .set("user_id", selUserId)
            .set("rcat_password", scrambled_random_password.data())
            .set("pass_expiry_ts", expTime)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_pw);

        trans.commit();

        std::strncpy(*_password_buffer, random_password.c_str(), _password_buffer_size);
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
} // db_update_pam_password_op

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_mod_user_op(
    irods::plugin_context& _ctx,
    const char*            _user_name,
    const char*            _option,
    const char*            _new_value ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( !_user_name || !_option || !_new_value ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    if ( *_user_name == '\0' || *_option == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "parameter is empty" );
    }

    if( *_new_value == '\0' && (
        strcmp( _option, "type"   ) == 0 ||
        strcmp( _option, "zone"   ) == 0 ||
        strcmp( _option, "addAuth") == 0 ||
        strcmp( _option, "rmAuth" ) == 0 ) ) {
        return ERROR( CAT_INVALID_ARGUMENT, "new value is empty" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        int groupAdminSettingPassword = 0;
        if ( _ctx.comm()->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH &&
             _ctx.comm()->proxyUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH ) {
            // user is OK
        }
        else {
            if ( strcmp( _option, "password" ) != 0 ) {
                return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
            }
            if ( 0 != strcmp( _user_name, _ctx.comm()->clientUser.userName ) ) {
                int status2 = irods::experimental::catalog::access_control::check_group_admin_access(
                    executor, db_conn, _ctx.comm()->clientUser.userName, _ctx.comm()->clientUser.rodsZone, "" );
                if ( status2 != 0 ) {
                    return ERROR( status2, "check_group_admin_access failed" );
                }
                groupAdminSettingPassword = 1;
            }
        }

        std::string zone;
        ret = getLocalZone( _ctx.prop_map(), &icss, zone );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        char userName2[NAME_LEN]{};
        char zoneName[NAME_LEN]{};
        int status = validateAndParseUserName( _user_name, userName2, zoneName );
        if ( status ) {
            return ERROR( status, "Invalid username format" );
        }
        if ( zoneName[0] == '\0' ) {
            rstrcpy( zoneName, zone.c_str(), NAME_LEN );
        }

        char myTime[50]{};
        getNowStr( myTime );

        nanodbc::transaction trans{db_conn};

        if ( strcmp( _option, "type" ) == 0 || strcmp( _option, "user_type_name" ) == 0 ) {
            int tokStatus = irods::experimental::catalog::access_control::check_name_token(
                executor, db_conn, "user_type", _new_value );
            if ( tokStatus != 0 ) {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              fmt::format( "user_type '{}' is not valid", _new_value ).c_str() );
                return ERROR( CAT_INVALID_USER_TYPE, "invalid user type" );
            }

            auto upd_stmt = gq2::builder::update("USER")
                .set("user_type_name", _new_value)
                .set("modify_ts", myTime)
                .where(col("user_name") == userName2 && col("zone_name") == zoneName)
                .build();
            auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
            if ( res.affected_rows == 0 ) {
                return ERROR( CAT_INVALID_USER, "invalid user" );
            }
        }
        else if ( strcmp( _option, "addAuth" ) == 0 ) {
            if ( _userInRUserAuth( userName2, zoneName, _new_value ) ) {
                return SUCCESS();
            }

            const auto uid_opt = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == userName2 && col("zone_name") == zoneName)
                    .build());
            if (!uid_opt) {
                return ERROR( CAT_INVALID_USER, "invalid user" );
            }

            auto ins_stmt = gq2::builder::insert_into("USER_AUTH")
                .set("user_id", std::to_string(*uid_opt))
                .set("user_auth_name", _new_value)
                .set("create_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
        }
        else if ( strcmp( _option, "rmAuth" ) == 0 ) {
            const auto uid_opt = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == userName2 && col("zone_name") == zoneName)
                    .build());
            if (uid_opt) {
                auto del_stmt = gq2::builder::remove_from("USER_AUTH")
                    .where(col("user_id") == std::to_string(*uid_opt) && col("user_auth_name") == _new_value)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);
            }
        }
        else if ( strncmp( _option, "rmPamPw", 9 ) == 0 ) {
            auth_config ac{};
            if (const auto err = get_auth_config("authentication", ac); !err.ok()) {
                log_db::error("Failed to get auth configuration. [{}]", err.result());
                return err;
            }
            const auto password_min_time_str = std::to_string(ac.password_min_time);
            const auto password_max_time_str = std::to_string(ac.password_max_time);

            const auto uid_opt = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == userName2 && col("zone_name") == zoneName)
                    .build());
            if (uid_opt) {
                auto pw_res = irods::experimental::catalog::execute_catalog(
                    executor, db_conn,
                    gq2::builder::select({"rcat_password", "pass_expiry_ts"})
                        .from("USER_PASSWORD")
                        .where(col("user_id") == std::to_string(*uid_opt))
                        .build());
                if (pw_res.query_result) {
                    while (pw_res.query_result->next()) {
                        const auto pw = pw_res.query_result->get<std::string>(0);
                        const auto expiry_str = pw_res.query_result->get<std::string>(1);
                        if (!expiry_str.starts_with("9999")) {
                            const auto expiry = std::atoll(expiry_str.c_str());
                            if (expiry >= ac.password_min_time && expiry <= ac.password_max_time) {
                                auto del_pw = gq2::builder::remove_from("USER_PASSWORD")
                                    .where(col("user_id") == std::to_string(*uid_opt) && col("rcat_password") == pw)
                                    .build();
                                irods::experimental::catalog::execute_catalog(executor, db_conn, del_pw);
                            }
                        }
                    }
                }
            }
        }
        else if ( strcmp( _option, "info" ) == 0 || strcmp( _option, "user_info" ) == 0 ) {
            auto upd_stmt = gq2::builder::update("USER")
                .set("user_info", _new_value)
                .set("modify_ts", myTime)
                .where(col("user_name") == userName2 && col("zone_name") == zoneName)
                .build();
            auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
            if ( res.affected_rows == 0 ) {
                return ERROR( CAT_INVALID_USER, "invalid user" );
            }
        }
        else if ( strcmp( _option, "comment" ) == 0 || strcmp( _option, "r_comment" ) == 0 ) {
            auto upd_stmt = gq2::builder::update("USER")
                .set("r_comment", _new_value)
                .set("modify_ts", myTime)
                .where(col("user_name") == userName2 && col("zone_name") == zoneName)
                .build();
            auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
            if ( res.affected_rows == 0 ) {
                return ERROR( CAT_INVALID_USER, "invalid user" );
            }
        }
        else if ( strcmp( _option, "password" ) == 0 ) {
            char decoded[MAX_PASSWORD_LEN + 20]{};
            int decStatus = decodePw( _ctx.comm(), _new_value, decoded );
            if ( decStatus == CAT_PASSWORD_ENCODING_ERROR || strlen(decoded) > MAX_PASSWORD_LEN - 8 ) {
                irods::pop_error_message(_ctx.comm()->rError);
                return ERROR( PASSWORD_EXCEEDS_MAX_SIZE, "Password must be between 3 and 42 characters" );
            }
            int ruleStatus = icatApplyRule( _ctx.comm(), ( char* )"acCheckPasswordStrength", decoded );
            if ( ruleStatus == NO_RULE_OR_MSI_FUNCTION_FOUND_ERR ) {
                addRErrorMsg( &_ctx.comm()->rError, 0, "acCheckPasswordStrength rule not found" );
            }
            if ( ruleStatus ) {
                return ERROR( ruleStatus, "icatApplyRule failed" );
            }

            icatScramble( decoded );
            if ( decStatus ) {
                return ERROR( decStatus, "password scramble failed" );
            }

            const auto uid_opt = irods::experimental::catalog::query_catalog_string(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == userName2 && col("zone_name") == zoneName)
                    .build());
            if (!uid_opt) {
                return ERROR( CAT_INVALID_USER, "invalid user" );
            }

            const auto pwd_uid_opt = irods::experimental::catalog::query_catalog_string(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER_PASSWORD")
                    .where(col("user_id") == *uid_opt)
                    .build());

            if ( pwd_uid_opt.has_value() ) {
                if ( groupAdminSettingPassword == 1 ) {
                    return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
                }
                auto upd_stmt = gq2::builder::update("USER_PASSWORD")
                    .set("rcat_password", decoded)
                    .set("modify_ts", myTime)
                    .where(col("user_id") == *pwd_uid_opt)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
            }
            else {
                auto ins_stmt = gq2::builder::insert_into("USER_PASSWORD")
                    .set("user_id", *uid_opt)
                    .set("rcat_password", decoded)
                    .set("pass_expiry_ts", "9999-12-31-23.59.01")
                    .set("create_ts", myTime)
                    .set("modify_ts", myTime)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
            }

            memset( decoded, 0, MAX_PASSWORD_LEN );
        }
        else {
            return ERROR( CAT_INVALID_ARGUMENT, "invalid argument" );
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_mod_user_op

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_mod_group_op(
    irods::plugin_context& _ctx,
    const char*            _group_name,
    const char*            _option,
    const char*            _user_name,
    const char*            _user_zone ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( !_group_name || !_option || !_user_name ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    if ( *_group_name == '\0' || *_option == '\0' || *_user_name == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "argument is empty" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ||
             _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
            int status2 = irods::experimental::catalog::access_control::check_group_admin_access(
                executor, db_conn, _ctx.comm()->clientUser.userName, _ctx.comm()->clientUser.rodsZone, _group_name );
            if ( status2 != 0 ) {
                if ( strcmp( _option, "add" ) == 0 ) {
                    int status3 = irods::experimental::catalog::access_control::check_group_admin_access(
                        executor, db_conn, _ctx.comm()->clientUser.userName, _ctx.comm()->clientUser.rodsZone, "" );
                    if ( status3 == 0 ) {
                        int status4 = irods::experimental::catalog::access_control::get_group_member_count(
                            executor, db_conn, _group_name );
                        if ( status4 == 0 ) {
                            status2 = 0;
                        }
                    }
                }
            }
            if ( status2 != 0 ) {
                return ERROR( status2, "group admin access invalid" );
            }
        }

        std::string zone;
        ret = getLocalZone( _ctx.prop_map(), &icss, zone );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        char zoneToUse[MAX_NAME_LEN]{};
        if ( _user_zone != NULL && *_user_zone != '\0' ) {
            snprintf( zoneToUse, MAX_NAME_LEN, "%s", _user_zone );
        }
        else {
            snprintf( zoneToUse, MAX_NAME_LEN, "%s", zone.c_str() );
        }

        char userName2[NAME_LEN]{};
        char zoneName[NAME_LEN]{};
        int status = validateAndParseUserName( _user_name, userName2, zoneName );
        if ( status ) {
            return ERROR( status, "Invalid username format" );
        }
        if ( zoneName[0] != '\0' ) {
            rstrcpy( zoneToUse, zoneName, NAME_LEN );
        }

        const auto userIdOpt = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName2 && col("zone_name") == zoneToUse && col("user_type_name") != "rodsgroup")
                .build());
        if (!userIdOpt) {
            return ERROR( CAT_INVALID_USER, "user not found" );
        }
        const std::string userId = *userIdOpt;

        const auto groupIdOpt = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _group_name && col("zone_name") == zone && col("user_type_name") == "rodsgroup")
                .build());
        if (!groupIdOpt) {
            return ERROR( CAT_INVALID_GROUP, "invalid group" );
        }
        const std::string groupId = *groupIdOpt;

        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        if ( strcmp( _option, "remove" ) == 0 ) {
            auto del_stmt = gq2::builder::remove_from("USER_GROUP")
                .where(col("group_user_id") == groupId && col("user_id") == userId)
                .build();
            const auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);
            if ( res.affected_rows == 0 ) {
                return ERROR( USER_NOT_IN_GROUP, "user not in group" );
            }
        }
        else if ( strcmp( _option, "add" ) == 0 ) {
            char myTime[50]{};
            getNowStr( myTime );
            auto ins_stmt = gq2::builder::insert_into("USER_GROUP")
                .set("group_user_id", groupId)
                .set("user_id", userId)
                .set("create_ts", myTime)
                .set("modify_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
        }
        else {
            return ERROR( CAT_INVALID_ARGUMENT, "invalid option" );
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_mod_group_op

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_mod_resc_op(
    irods::plugin_context& _ctx,
    const char*            _resc_name,
    const char*            _option,
    const char*            _option_value ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_resc_name  ||
        !_option     ||
        !_option_value ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    if ( *_resc_name == '\0' || *_option == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "argument is empty" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    // =-=-=-=-=-=-=-

    if ( strncmp( _resc_name, BUNDLE_RESC, strlen( BUNDLE_RESC ) ) == 0 ) {
        addRErrorMsg( &_ctx.comm()->rError, 0,
                      fmt::format( "{} is a built-in resource needed for bundle operations.", BUNDLE_RESC ).c_str() );
        return ERROR( CAT_PSEUDO_RESC_MODIFY_DISALLOWED, "cannot mod bundle resc" );
    }
    // =-=-=-=-=-=-=-

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto resc_id_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"resc_id"})
                .from("RESOURCE")
                .where(col("resc_name") == _resc_name && col("zone_name") == zone)
                .build());
        if ( !resc_id_opt.has_value() ) {
            return ERROR( CAT_INVALID_RESOURCE, "invalid resource" );
        }
        const std::string resc_id = *resc_id_opt;

        const auto [current_time_secs, current_time_msecs] = get_current_time();
        int OK = 0;
        std::string previous_resc_path;

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        if ( strcmp( _option, "comment" ) == 0 ) {
            auto stmt = gq2::builder::update("RESOURCE")
                .set("r_comment", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }
        else if ( *_option_value == '\0' ) {
            return ERROR( CAT_INVALID_ARGUMENT, "argument is empty" );
        }

        if ( strcmp( _option, "info" ) == 0 ) {
            auto stmt = gq2::builder::update("RESOURCE")
                .set("resc_info", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }

        if ( strcmp( _option, "freespace" ) == 0 || strcmp( _option, "free_space" ) == 0 ) {
            int inType = 0;
            const char* opt_val = _option_value;
            if ( *opt_val == '+' ) {
                inType = 1;
                opt_val++;
            }
            else if ( *opt_val == '-' ) {
                inType = 2;
                opt_val++;
            }

            std::string final_free_space;
            if ( inType == 0 ) {
                final_free_space = opt_val;
            }
            else {
                auto current_fs_opt = irods::experimental::catalog::query_catalog_string(
                    executor, db_conn,
                    gq2::builder::select({"free_space"})
                        .from("RESOURCE")
                        .where(col("resc_id") == resc_id)
                        .build());
                int64_t current_fs = 0;
                if (current_fs_opt && !current_fs_opt->empty()) {
                    try {
                        current_fs = std::stoll(*current_fs_opt);
                    } catch (...) {
                        current_fs = 0;
                    }
                }
                int64_t delta = 0;
                try {
                    delta = std::stoll(opt_val);
                } catch (...) {
                    delta = 0;
                }
                int64_t new_fs = (inType == 1) ? (current_fs + delta) : (current_fs - delta);
                final_free_space = std::to_string(new_fs);
            }

            auto stmt = gq2::builder::update("RESOURCE")
                .set("free_space", final_free_space)
                .set("free_space_ts", current_time_secs)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }

        if ( strcmp( _option, "host" ) == 0 ) {
            _resolveHostName( _ctx.comm(), _option_value );
            auto stmt = gq2::builder::update("RESOURCE")
                .set("resc_net", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }

        if ( strcmp( _option, "type" ) == 0 ) {
            auto stmt = gq2::builder::update("RESOURCE")
                .set("resc_type_name", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }

        if ( strcmp( _option, "path" ) == 0 ) {
            ret = verify_non_root_vault_path( _ctx, std::string( _option_value ) );
            if ( !ret.ok() ) {
                return PASS( ret );
            }

            auto path_opt = irods::experimental::catalog::query_catalog_string(
                executor, db_conn,
                gq2::builder::select({"resc_def_path"}).from("RESOURCE").where(col("resc_id") == resc_id).build());
            if ( !path_opt.has_value() ) {
                return ERROR( CAT_INVALID_RESOURCE, "failed to get path" );
            }
            previous_resc_path = *path_opt;

            auto stmt = gq2::builder::update("RESOURCE")
                .set("resc_def_path", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }

        if ( strcmp( _option, "status" ) == 0 ) {
            auto stmt = gq2::builder::update("RESOURCE")
                .set("resc_status", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }

        if ( strcmp( _option, "name" ) == 0 ) {
            auto stmt = gq2::builder::update("RESOURCE")
                .set("resc_name", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);

            auto load_stmt = gq2::builder::update("R_SERVER_LOAD")
                .set("resc_name", _option_value)
                .where(col("resc_name") == _resc_name)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, load_stmt);

            auto digest_stmt = gq2::builder::update("R_SERVER_LOAD_DIGEST")
                .set("resc_name", _option_value)
                .where(col("resc_name") == _resc_name)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, digest_stmt);

            OK = 1;
        }

        if ( strcmp( _option, "context" ) == 0 ) {
            auto stmt = gq2::builder::update("RESOURCE")
                .set("resc_context", _option_value)
                .set("modify_ts", current_time_secs)
                .set("modify_ts_millis", current_time_msecs)
                .where(col("resc_id") == resc_id)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
            OK = 1;
        }

        if ( OK == 0 ) {
            return ERROR( CAT_INVALID_ARGUMENT, "invalid option" );
        }

        trans.commit();

        if ( !previous_resc_path.empty() ) {
            char rescPathMsg[MAX_NAME_LEN + 100];
            snprintf( rescPathMsg, sizeof( rescPathMsg ), "Previous resource path: %s",
                      previous_resc_path.c_str() );
            addRErrorMsg( &_ctx.comm()->rError, 0, rescPathMsg );
        }

        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_mod_resc_op

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_mod_resc_data_paths_op(
    irods::plugin_context& _ctx,
    const char*            _resc_name,
    const char*            _old_path,
    const char*            _new_path,
    const char*            _user_name ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_resc_name ||
        !_old_path  ||
        !_new_path ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    if ( *_resc_name == '\0' || *_old_path == '\0' || *_new_path == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "argument is empty" );
    }

    /* the paths must begin and end with / */
    if ( *_old_path != '/' || *_new_path != '/' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid path" );
    }
    const auto old_len = strlen( _old_path );
    if ( _old_path[old_len - 1] != '/' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid old path" );
    }
    const auto new_len = strlen( _new_path );
    if ( _new_path[new_len - 1] != '/' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid new path" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto resc_id_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"resc_id"})
                .from("RESOURCE")
                .where(col("resc_name") == _resc_name && col("zone_name") == zone)
                .build());
        if ( !resc_id_opt.has_value() ) {
            return ERROR( CAT_INVALID_RESOURCE, "invalid resource" );
        }
        const std::string resc_id = *resc_id_opt;

        const std::string old_path_like = std::string(_old_path) + "%";
        int rows = 0;

        std::vector<std::tuple<std::string, std::string, std::string>> updates;
        if ( _user_name != nullptr && *_user_name != '\0' ) {
            char user_name2[NAME_LEN]{};
            char user_zone[NAME_LEN]{};
            int status = validateAndParseUserName( _user_name, user_name2, user_zone );
            if ( status != 0 ) {
                return ERROR( status, "Invalid username format" );
            }
            const std::string zone_to_use = user_zone[0] != '\0' ? user_zone : zone;

            auto sel = gq2::builder::select({"data_id", "data_repl_num", "data_path"})
                .from("DATA_OBJECT")
                .where(col("resc_id") == resc_id && col("data_path").like(old_path_like) &&
                       col("data_owner_name") == user_name2 && col("data_owner_zone") == zone_to_use)
                .build();
            auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, sel);
            if (res.query_result) {
                while (res.query_result->next()) {
                    updates.emplace_back(
                        res.query_result->get<std::string>(0),
                        res.query_result->get<std::string>(1),
                        res.query_result->get<std::string>(2));
                }
            }
        }
        else {
            auto sel = gq2::builder::select({"data_id", "data_repl_num", "data_path"})
                .from("DATA_OBJECT")
                .where(col("resc_id") == resc_id && col("data_path").like(old_path_like))
                .build();
            auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, sel);
            if (res.query_result) {
                while (res.query_result->next()) {
                    updates.emplace_back(
                        res.query_result->get<std::string>(0),
                        res.query_result->get<std::string>(1),
                        res.query_result->get<std::string>(2));
                }
            }
        }

        for (const auto& [did, repl, p] : updates) {
            std::string new_p = p;
            boost::replace_all(new_p, _old_path, _new_path);
            auto upd = gq2::builder::update("DATA_OBJECT")
                .set("data_path", new_p)
                .where(col("data_id") == did && col("data_repl_num") == repl)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
            rows++;
        }

        trans.commit();

        if ( rows > 0 ) {
            char rowsMsg[100];
            snprintf( rowsMsg, sizeof(rowsMsg), "%d rows updated", rows );
            addRErrorMsg( &_ctx.comm()->rError, 0, rowsMsg );
        }

        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_mod_resc_data_paths_op

// =-=-=-=-=-=-=-
// authenticate user
irods::error db_mod_resc_freespace_op(
    irods::plugin_context& _ctx,
    const char*            _resc_name,
    int                    _update_value ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_resc_name ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    if ( *_resc_name == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "resc name is empty" );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }
    if ( _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        const auto [current_time_secs, current_time_msecs] = get_current_time();
        const std::string update_val_str = std::to_string( _update_value );

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto stmt = gq2::builder::update("RESOURCE")
            .set("free_space", update_val_str)
            .set("free_space_ts", current_time_secs)
            .set("modify_ts", current_time_secs)
            .set("modify_ts_millis", current_time_msecs)
            .where(col("resc_name") == _resc_name)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_mod_resc_freespace_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_reg_user_re_op(
    irods::plugin_context& _ctx,
    userInfo_t*            _user_info ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (
        !_user_info ) {
        return ERROR(
                   CAT_INVALID_ARGUMENT,
                   "null parameter" );
    }

    char myTime[50];
    int status;
    char userZone[MAX_NAME_LEN];
    char zoneId[MAX_NAME_LEN];

    int zoneForm;
    char userName2[NAME_LEN];
    char zoneName[NAME_LEN];

    static char lastValidUserType[MAX_NAME_LEN] = "";
    static char userTypeTokenName[MAX_NAME_LEN] = "";

    log_sql::debug("chlRegUserRE");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog is not connected" );
    }

    trimWS( _user_info->userName );
    trimWS( _user_info->userType );

    if ( !strlen( _user_info->userType ) || !strlen( _user_info->userName ) ) {
        return ERROR( CAT_INVALID_ARGUMENT, "user type or user name empty" );
    }

    // =-=-=-=-=-=-=-

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ||
            _ctx.comm()->proxyUser.authInfo.authFlag  < LOCAL_PRIV_USER_AUTH ) {
        int status2 = irods::experimental::catalog::access_control::check_group_admin_access(
            executor, db_conn,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            "" );
        if ( status2 != 0 ) {
            return ERROR( status2, "invalid group admin access" );
        }
        creatingUserByGroupAdmin = 1;
    }
    // =-=-=-=-=-=-=-
    /*
      Check if the user type is valid.
      This check is skipped if this process has already verified this type
      (iadmin doing a series of mkuser subcommands).
    */
    if ( *_user_info->userType == '\0' ||
            strcmp( _user_info->userType, lastValidUserType ) != 0 ) {
        log_sql::debug("chlRegUserRE SQL 1 ");
        auto opt_type = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"token_name"})
                .from("TOKEN")
                .where(col("token_namespace") == "user_type" && col("token_name") == _user_info->userType)
                .build());
        if ( opt_type ) {
            snprintf( lastValidUserType, sizeof( lastValidUserType ), "%s", _user_info->userType );
            rstrcpy( userTypeTokenName, opt_type->c_str(), sizeof( userTypeTokenName ) );
        }
        else {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "user_type '{}' is not valid", _user_info->userType ).c_str() );
            return ERROR( CAT_INVALID_USER_TYPE, "invalid user type" );
        }
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( strlen( _user_info->rodsZone ) > 0 ) {
        zoneForm = 1;
        snprintf( userZone, sizeof( userZone ), "%s", _user_info->rodsZone );
    }
    else {
        zoneForm = 0;
        snprintf( userZone, sizeof( userZone ), "%s", zone.c_str() );
    }

    status = validateAndParseUserName( _user_info->userName, userName2, zoneName );
    if ( status ) {
        return ERROR( status, "Invalid username format" );
    }
    if ( zoneName[0] != '\0' ) {
        snprintf( userZone, sizeof( userZone ), "%s", zoneName );
        zoneForm = 2;
    }

    if ( zoneForm ) {
        /* check that the zone exists (if not defaulting to local) */
        zoneId[0] = '\0';
        log_sql::debug("chlRegUserRE SQL 5 ");
        auto opt_zone = irods::experimental::catalog::query_catalog_string(
            executor,
            db_conn,
            gq2::builder::select({"zone_id"})
                .from("ZONE")
                .where(col("zone_name") == userZone)
                .build());
        if ( !opt_zone ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "zone '{}' does not exist", userZone ).c_str() );
            return ERROR( CAT_INVALID_ZONE, "invalid zone name" );
        }
        rstrcpy( zoneId, opt_zone->c_str(), sizeof( zoneId ) );
    }

    log_sql::debug("chlRegUserRE SQL 2");
    const auto seq_val = executor.get_next_sequence_value(db_conn, "R_ObjectID");
    if ( seq_val < 0 ) {
        log_db::info("chlRegUserRE get_next_sequence_value failure {}", seq_val);
        return ERROR( seq_val, "get_next_sequence_value failure" );
    }
    const auto seqStr = std::to_string( static_cast<long long>(seq_val) );

    getNowStr( myTime );

    try {
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;

        log_sql::debug("chlRegUserRE SQL 3");
        auto ins_user = gq2::builder::insert_into("USER")
            .set("user_id", seqStr)
            .set("user_name", userName2)
            .set("user_type_name", userTypeTokenName)
            .set("zone_name", userZone)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_user);

        log_sql::debug("chlRegUserRE SQL 4");
        auto ins_ug = gq2::builder::insert_into("USER_GROUP")
            .set("group_user_id", seqStr)
            .set("user_id", seqStr)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_ug);

        trans.commit();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( translate_nanodbc_error(e), e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }


    /*
      The case where the caller is specifying an authstring is used in
      some specialized cases.  Using the new table (Aug 12, 2009), this
      is now set via the chlModUser call below.  This is untested, though.
    */
    if ( strlen( _user_info->authInfo.authStr ) > 0 ) {
        status = chlModUser( _ctx.comm(), _user_info->userName, "addAuth",
                             _user_info->authInfo.authStr );
        if ( status != 0 ) {
            log_db::info("chlRegUserRE chlModUser insert auth failure {}", status);
            _rollback( "chlRegUserRE" );
            return ERROR( status, "insert auth failure" );
        }
    }

    return CODE( status );

} // db_reg_user_re_op

// =-=-=-=-=-=-=-
// commit the transaction
irods::error db_set_avu_metadata_op(
    irods::plugin_context& _ctx,
    const char*            _type,
    const char*            _name,
    const char*            _attribute,
    const char*            _new_value,
    const char*            _new_unit,
    const KeyValPair*      _cond_input)
{
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (!_type || !_name || !_attribute) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    int status;
    char myTime[50];
    rodsLong_t objId;
    char metaIdStr[MAX_NAME_LEN * 2]; /* twice as needed to query multiple */
    char objIdStr[MAX_NAME_LEN];

    memset( metaIdStr, 0, sizeof( metaIdStr ) );
    log_sql::debug("chlSetAVUMetadata");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    log_sql::debug("chlSetAVUMetadata SQL 1 ");
    objId = checkAndGetObjectId(_ctx.comm(), _ctx.prop_map(), _type, _name, ACCESS_CREATE_METADATA,
                                getValByKey(_cond_input, ADMIN_KW));
    if ( objId < 0 ) {
        return ERROR( objId, "checkAndGetObjectId failed" );
    }
    snprintf( objIdStr, MAX_NAME_LEN, "%lld", objId );

    log_sql::debug("chlSetAVUMetadata SQL 2");

    /* Treat unspecified unit as empty string */
    if ( _new_unit == NULL ) {
        _new_unit = "";
    }

    if (!is_valid_avu(_attribute, _new_value, _new_unit)) {
        return ERROR(CAT_INVALID_ARGUMENT, "invalid AVU");
    }

    /* Query to see if the attribute exists for this object
     *
     * If status == 0: then object has zero AVUs with matching A
     * If status == 1: then object has *exactly one* AVU with this A AND said AVU is not shared with any other object
     * If status >= 2: then at least one of:
     *                     object has multiple AVUs with this A
     *                     object has an AVU with this A and said AVU is shared with another object
     */

    int row_count = 0;
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto meta_ids = irods::experimental::catalog::query_catalog_strings(
            executor, db_conn,
            gq2::builder::select({"meta_id"})
                .from("METADATA_MAP")
                .where(col("object_id") == objIdStr)
                .build());

        std::vector<std::string> matching_meta_ids;
        for (const auto& mid : meta_ids) {
            auto match = irods::experimental::catalog::query_catalog_string(
                executor, db_conn,
                gq2::builder::select({"meta_id"})
                    .from("METADATA")
                    .where(col("meta_id") == mid && col("meta_attr_name") == _attribute)
                    .build());
            if (match) {
                matching_meta_ids.push_back(mid);
            }
        }

        if (matching_meta_ids.size() == 1) {
            auto ref_count = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({gq2::builder::count("object_id")})
                    .from("METADATA_MAP")
                    .where(col("meta_id") == matching_meta_ids[0])
                    .build());
            if (ref_count && *ref_count == 1) {
                row_count = 1;
                rstrcpy(metaIdStr, matching_meta_ids[0].c_str(), sizeof(metaIdStr));
            }
            else {
                row_count = 2; // shared with other objects
            }
        }
        else if (matching_meta_ids.size() > 1) {
            row_count = 2; // multiple AVUs with this attribute
        }
        else {
            row_count = 0; // zero AVUs with matching attribute
        }
    }
    catch (const std::exception& e) {
        log_db::info("chlSetAVUMetadata nanodbc query failure {}", e.what());
        return ERROR( CAT_SQL_ERR, "get avu failed" );
    }

    if ( row_count == 0 ) {
        // Need to add the metadata.
        status = chlAddAVUMetadata( _ctx.comm(), _type, _name, _attribute,
                                    _new_value, _new_unit, _cond_input );
        return ERROR( status, "get avu failed" );
    }

    if ( row_count > 1 ) {
        /* Cannot update AVU in-place, need to do a delete with wildcards then add */
        status = chlDeleteAVUMetadata( _ctx.comm(), 1, _type, _name, _attribute, "%",
                                       "%", 1, _cond_input );
        if ( status != 0 ) {
            /* Give it a second chance
             * as per r5350:
             *   "Improve the handling of an ICAT AVU metadata concurrency case.
             *
             *    When setting user-defined meta-data (AVUs), if two Agents try to
             *    modify the the same metadata (which exists with different values in
             *    R_META_MAIN), one of the chlSetAVUMetadata fails.  These changes allow
             *    the agent to retry a second time.
             *
             *    Since this is in the case of "delete then add", the
             *    chlDeleteAVUMetadata is called with noCommit=1, but the delete fails
             *    (since the AVU was changed by the other agent), so in this case
             *    (noCommit set) it should not do the rollback.  And then the caller,
             *    chlSetAVUMetadata, can try a second time to modify the AVU."
             *
             * Essentially, this is a MASSSIVE hack that attempts to bypass a race condition
             * by TRYING TWICE. This implies other major race conditions also exist related
             * to the AVUMetadata, as well as the fact that this race condition is not
             * actually fixed. Leaving it in for now, as it is marginally better than the
             * alternative, but this is a definite TODO: Implement AVUMetadata locks.
             */
            status = chlDeleteAVUMetadata( _ctx.comm(), 1, _type, _name, _attribute, "%",
                                           "%", 1, _cond_input );
        }

        if ( status != 0 ) {
            _rollback( "chlSetAVUMetadata" );
            return ERROR( status, "delete avu metadata failed" );
        }

        status = chlAddAVUMetadata( _ctx.comm(), _type, _name, _attribute,
                                    _new_value, _new_unit, _cond_input );

        return ERROR( status, "delete avu metadata failed" );
    }

    /* Only one metaId for this Attribute and Object has been found, and the metaID is not shared */
    log_db::debug("chlSetAVUMetadata found metaId {}", metaIdStr);

    log_sql::debug("chlSetAVUMetadata SQL 4");

    getNowStr( myTime );
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlSetAVUMetadata SQL 5");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto stmt = gq2::builder::update("METADATA")
            .set("meta_attr_value", _new_value ? _new_value : "")
            .set("meta_attr_unit", _new_unit ? _new_unit : "")
            .set("modify_ts", myTime)
            .where(col("meta_id") == metaIdStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( translate_nanodbc_error(e), e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_set_avu_metadata_op

irods::error db_add_avu_metadata_op(
    irods::plugin_context& _ctx,
    const char*            _type,
    const char*            _name,
    const char*            _attribute,
    const char*            _value,
    const char*            _units,
    const KeyValPair*      _cond_input)
{
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (!_type || !_name || !_attribute) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    int itype;
    char myTime[50];
    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];
    rodsLong_t seqNum;
    rodsLong_t objId, status;
    char userName[NAME_LEN];
    char userZone[NAME_LEN];

    log_sql::debug("chlAddAVUMetadata");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    if ( _type == NULL || *_type == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "type null or empty" );
    }

    if ( _name == NULL || *_name == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "name null or empty" );
    }

    if ( _attribute == NULL || *_attribute == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "attribute null or empty" );
    }

    if ( _value == NULL || *_value == '\0' ) {
        return  ERROR( CAT_INVALID_ARGUMENT, "value null or empty" );
    }

    const bool admin_mode = _cond_input && getValByKey(_cond_input, ADMIN_KW);

    if (admin_mode && !irods::is_privileged_client(*_ctx.comm())) {
        return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Insufficient privileges");
    }

    if ( _units == NULL ) {
        _units = "";
    }

    itype = convertTypeOption( _type );
    if ( itype == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid type argument" );
    }

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    // Data Objects
    if (itype == 1) {
        const auto ec = splitPathByKey(_name, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); 

        if (ec < 0) {
            return ERROR(ec, fmt::format("[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                                         __func__, __LINE__, _name, ec));
        }

        if (std::strlen(logicalParentDirName ) == 0) {
            snprintf(logicalParentDirName, sizeof(logicalParentDirName), "%s", PATH_SEPARATOR);
            snprintf(logicalEndName, sizeof(logicalEndName), "%s", _name);
        }

        log_sql::debug("chlAddAVUMetadata SQL 2");

        status = irods::experimental::catalog::access_control::check_data_object_only(
            executor, db_conn,
            logicalParentDirName, logicalEndName,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_CREATE_METADATA, admin_mode);

        if (status < 0) {
            _rollback("chlAddAVUMetadata");
            return ERROR(status, "select data_id failed");
        }

        objId = status;
    }

    // Collections
    if (itype == 2) {
        // Check that the collection exists and user has create_metadata
        // permission, and get the collectionID.
        log_sql::debug("chlAddAVUMetadata SQL 4");

        status = irods::experimental::catalog::access_control::check_collection_access(
            executor, db_conn,
            _name,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_CREATE_METADATA, admin_mode);

        if ( status < 0 ) {
            _rollback( "chlAddAVUMetadata" ); // TODO We've rolled back here, so why the extra call below?

            if ( status == CAT_UNKNOWN_COLLECTION ) {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              fmt::format( "collection '{}' is unknown", _name ).c_str() );
            }
            else {
                _rollback( "chlAddAVUMetadata" ); // TODO Why do we rollback again?
            }

            return ERROR( status, "check_collection_access failed" );
        }

        objId = status;
    }

    if ( itype == 3 ) {
        if (!irods::is_privileged_client(*_ctx.comm())) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }

        std::string zone;
        ret = getLocalZone( _ctx.prop_map(), &icss, zone );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        log_sql::debug("chlAddAVUMetadata SQL 5");
        auto opt_resc = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"resc_id"})
                .from("RESOURCE")
                .where(col("resc_name") == _name && col("zone_name") == zone)
                .build());
        if ( !opt_resc ) {
            _rollback( "chlAddAVUMetadata" );
            return ERROR( CAT_INVALID_RESOURCE, "invalid resource" );
        }
        objId = *opt_resc;
    }

    if ( itype == 4 ) {
        if (!irods::is_privileged_client(*_ctx.comm())) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }

        status = validateAndParseUserName( _name, userName, userZone );
        if ( status ) {
            return ERROR( status, "Invalid username format" );
        }
        if ( userZone[0] == '\0' ) {
            std::string zone;
            ret = getLocalZone( _ctx.prop_map(), &icss, zone );
            if ( !ret.ok() ) {
                return PASS( ret );
            }
            snprintf( userZone, NAME_LEN, "%s", zone.c_str() );
        }

        log_sql::debug("chlAddAVUMetadata SQL 6");
        auto opt_user = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName && col("zone_name") == userZone)
                .build());
        if ( !opt_user ) {
            _rollback( "chlAddAVUMetadata" );
            return ERROR( CAT_INVALID_USER, "invalid user" );
        }
        objId = *opt_user;
    }

    if ( itype == 5 ) {
        if (!irods::is_privileged_client(*_ctx.comm())) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL , "insufficient privilege" );
        }

        std::string zone;
        ret = getLocalZone( _ctx.prop_map(), &icss, zone );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        log_sql::debug("chlAddAVUMetadata SQL 7");
        auto opt_group = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"resc_group_id"})
                .distinct()
                .from("RESOURCE_GROUP")
                .where(col("resc_group_name") == _name)
                .build());
        if ( !opt_group ) {
            _rollback( "chlAddAVUMetadata" );
            return ERROR( CAT_INVALID_RESOURCE, "invalid resource" );
        }
        objId = *opt_group;
    }

    status = findOrInsertAVU( _attribute, _value, _units );
    if ( status < 0 ) {
        log_db::info("chlAddAVUMetadata findOrInsertAVU failure {}", status);
        _rollback( "chlAddAVUMetadata" );
        return ERROR( status, "findOrInsertAVU failure" );
    }
    seqNum = status;

    getNowStr( myTime );

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlAddAVUMetadata SQL 7");
        namespace gq2 = irods::experimental::genquery2;
        auto ins_stmt = gq2::builder::insert_into("METADATA_MAP")
            .set("object_id", std::to_string(objId))
            .set("meta_id", std::to_string(seqNum))
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( translate_nanodbc_error(e), e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_add_avu_metadata_op

irods::error db_mod_avu_metadata_op(
    irods::plugin_context& _ctx,
    const char*            _type,
    const char*            _name,
    const char*            _attribute,
    const char*            _value,
    const char*            _unitsOrArg0,
    const char*            _arg1,
    const char*            _arg2,
    const char*            _arg3,
    const KeyValPair*      _cond_input)
{
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (!_type || !_name || !_attribute) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    int status, atype;
    const char *dummy = NULL;
    const char *myUnits = "";
    const char *addAttr = "";
    const char *addValue = "";
    const char  *addUnits = NULL;

    if ( _unitsOrArg0 == NULL ) {
        return ERROR( CAT_INVALID_ARGUMENT, "unitsOrArg0 empty or null" );
    }

    atype = checkModArgType( _unitsOrArg0 );
    if ( atype == 0 ) {
        myUnits = _unitsOrArg0;
    }
    else {
        dummy = _unitsOrArg0;
    }

    status = chlDeleteAVUMetadata( _ctx.comm(), 0, _type, _name, _attribute, _value,
                                   myUnits, 1, _cond_input );
    if ( status != 0 ) {
        _rollback( "chlModAVUMetadata" );
        return ERROR( status, "delete avu metadata failed" );
    }

    bool new_attr_set = false;
    bool new_val_set = false;
    bool new_unit_set = false;

    for (auto arg : { dummy , _arg1, _arg2, _arg3 } )
    {
      if (arg == NULL) continue;
      atype = checkModArgType( arg );
      if ( atype == 1 ) {
        if (new_attr_set) {
            _rollback( "chlModAVUMetadata" );
            return ERROR( CAT_INVALID_ARGUMENT, "new attribute specified more than once" );
        } else {
            new_attr_set = true;
        }
        addAttr = arg + 2;
      }
      if ( atype == 2 ) {
        if (new_val_set) {
            _rollback( "chlModAVUMetadata" );
            return ERROR( CAT_INVALID_ARGUMENT, "new value specified more than once" );
        } else {
            new_val_set = true;
        }
        addValue = arg + 2;
      }
      if ( atype == 3 ) {
        if (new_unit_set) {
            _rollback( "chlModAVUMetadata" );
            return ERROR( CAT_INVALID_ARGUMENT, "new unit specified more than once" );
        } else {
            new_unit_set = true;
        }
        addUnits = arg + 2;
      }
    }

    if ( *addAttr  == '\0' &&
            *addValue == '\0' &&
            addUnits == NULL ) {
        _rollback( "chlModAVUMetadata" );
        return ERROR( CAT_INVALID_ARGUMENT, "arg check failed" );
    }

    if ( *addAttr == '\0' ) {
        addAttr = _attribute;
    }
    if ( *addValue == '\0' ) {
        addValue = _value;
    }
    if ( addUnits == NULL ) {
        addUnits = myUnits;
    }

    status = chlAddAVUMetadata( _ctx.comm(), _type, _name, addAttr, addValue,
                                addUnits, _cond_input );

    return CODE( status );
} // db_mod_avu_metadata_op

irods::error db_del_avu_metadata_op(
    irods::plugin_context& _ctx,
    int                    _option,
    const char*            _type,
    const char*            _name,
    const char*            _attribute,
    const char*            _value,
    const char*            _unit,
    int                    _nocommit,
    const KeyValPair*      _cond_input)
{
    const bool admin_mode = _cond_input && getValByKey(_cond_input, ADMIN_KW);

    if (admin_mode && !irods::is_privileged_client(*_ctx.comm())) {
        return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Insufficient privileges");
    }

    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (!_type || !_name || !_attribute) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    int itype;
    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];
    rodsLong_t status;
    rodsLong_t objId;
    std::string objIdStr;
    int allowNullUnits;
    char userName[NAME_LEN];
    char userZone[NAME_LEN];

    log_sql::debug("chlDeleteAVUMetadata");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    if ( _type == NULL || *_type == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid type" );
    }

    if ( _name == NULL || *_name == '\0' ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid name" );
    }
    if ( _option != 2 ) {
        if ( _attribute == NULL || *_attribute == '\0' ) {
            return ERROR( CAT_INVALID_ARGUMENT, "invalid attribute" );
        }

        if ( _value == NULL || *_value == '\0' ) {
            return ERROR( CAT_INVALID_ARGUMENT, "invalid value" );
        }
    }

    if ( _unit == NULL ) {
        _unit = "";
    }

    itype = convertTypeOption( _type );
    if ( itype == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "invalid type" );
    }

    if ( itype == 1 ) {
        if (const auto ec = splitPathByKey(_name, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
            return ERROR(ec, fmt::format(
                         "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                         __func__, __LINE__, _name, ec));
        }

        if ( strlen( logicalParentDirName ) == 0 ) {
            snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
            snprintf( logicalEndName, sizeof( logicalEndName ), "%s", _name );
        }

        log_sql::debug("chlDeleteAVUMetadata SQL 1 ");

        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        status = irods::experimental::catalog::access_control::check_data_object_only(
            executor, db_conn,
            logicalParentDirName, logicalEndName,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_DELETE_METADATA, admin_mode);
        if ( status < 0 ) {
            if ( _nocommit != 1 ) {
                _rollback( "chlDeleteAVUMetadata" );
            }

            return ERROR( status, "delete avu failed" );
        }

        objId = status;
    }

    if ( itype == 2 ) {
        // Check that the collection exists and user has delete_metadata permission,
        // and get the collectionID.
        log_sql::debug("chlDeleteAVUMetadata SQL 2");

        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        status = irods::experimental::catalog::access_control::check_collection_access(
            executor, db_conn,
            _name,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_DELETE_METADATA, admin_mode);

        if ( status < 0 ) {
            if ( status == CAT_UNKNOWN_COLLECTION ) {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              fmt::format( "collection '{}' is unknown", _name ).c_str() );
            }

            return ERROR( status, "check_collection_access failed" );
        }

        objId = status;
    }

    if ( itype == 3 ) {
        if (!irods::is_privileged_client(*_ctx.comm())) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }

        std::string zone;
        ret = getLocalZone( _ctx.prop_map(), &icss, zone );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        log_sql::debug("chlDeleteAVUMetadata SQL 3");
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        auto opt_resc = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"resc_id"})
                .from("RESOURCE")
                .where(col("resc_name") == _name && col("zone_name") == zone)
                .build());
        if ( !opt_resc ) {
            return ERROR( CAT_INVALID_RESOURCE, "invalid resource" );
        }
        objId = *opt_resc;
    }

    if ( itype == 4 ) {
        if (!irods::is_privileged_client(*_ctx.comm())) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }

        status = validateAndParseUserName( _name, userName, userZone );
        if ( status ) {
            return ERROR( status, "Invalid username format" );
        }
        if ( userZone[0] == '\0' ) {
            std::string zone;
            ret = getLocalZone( _ctx.prop_map(), &icss, zone );
            if ( !ret.ok() ) {
                return PASS( ret );
            }
            snprintf( userZone, sizeof( userZone ), "%s", zone.c_str() );
        }

        log_sql::debug("chlDeleteAVUMetadata SQL 4");
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        auto opt_user = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == userName && col("zone_name") == userZone)
                .build());
        if ( !opt_user ) {
            return ERROR( CAT_INVALID_USER, "invalid user" );
        }
        objId = *opt_user;
    }

    if ( itype == 5 ) {
        if (!irods::is_privileged_client(*_ctx.comm())) {
            return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
        }

        std::string zone;
        ret = getLocalZone( _ctx.prop_map(), &icss, zone );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        log_sql::debug("chlDeleteAVUMetadata SQL 5");
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        auto opt_group = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"resc_group_id"})
                .from("RESOURCE_GROUP")
                .where(col("resc_group_name") == _name)
                .build());
        if ( !opt_group ) {
            return ERROR( CAT_INVALID_RESOURCE, "invalid resource" );
        }
        objId = *opt_group;
    }


    objIdStr = std::to_string( objId );
    if ( _option == 2 ) {
        try {
            auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
            std::unique_ptr<nanodbc::transaction> trans;
            if (_nocommit != 1) {
                trans = std::make_unique<nanodbc::transaction>(db_conn);
            }


            log_sql::debug("chlDeleteAVUMetadata SQL 9");
            namespace gq2 = irods::experimental::genquery2;
            using gq2::builder::col;
            auto del_stmt = gq2::builder::remove_from("METADATA_MAP")
                .where(col("object_id") == objIdStr && col("meta_id") == (_attribute ? _attribute : ""))
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

            if (trans) {
                trans->commit();
            }
            return SUCCESS();
        }
        catch (const nanodbc::database_error& e) {
            log_db::error("{}: database error: {}", __FUNCTION__, e.what());
            return ERROR( CAT_SQL_ERR, e.what() );
        }
        catch (const std::exception& e) {
            log_db::error("{}: exception: {}", __FUNCTION__, e.what());
            return ERROR( SYS_INTERNAL_ERR, e.what() );
        }
    }

    allowNullUnits = 0;
    if ( *_unit == '\0' ) {
        allowNullUnits = 1; /* null or empty-string units */
    }
    if ( _option == 1 && *_unit == '%' && *( _unit + 1 ) == '\0' ) {
        allowNullUnits = 1; /* wildcard and just % */
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        std::unique_ptr<nanodbc::transaction> trans;
        if (_nocommit != 1) {
            trans = std::make_unique<nanodbc::transaction>(db_conn);
        }


        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const std::string attr = _attribute ? _attribute : "";
        const std::string val = _value ? _value : "";
        const std::string unit = _unit ? _unit : "";

        gq2::builder::condition_builder cond = (_option == 1)
            ? (col("meta_attr_name").like(attr) && col("meta_attr_value").like(val))
            : (col("meta_attr_name") == attr && col("meta_attr_value") == val);

        if ( allowNullUnits ) {
            if ( _option == 1 ) {
                cond = cond && (col("meta_attr_unit").like(unit) || col("meta_attr_unit").is_null());
            }
            else {
                cond = cond && (col("meta_attr_unit") == unit || col("meta_attr_unit").is_null());
            }
        }
        else {
            if ( _option == 1 ) {
                cond = cond && col("meta_attr_unit").like(unit);
            }
            else {
                cond = cond && col("meta_attr_unit") == unit;
            }
        }

        const auto obj_meta_ids = irods::experimental::catalog::query_catalog_strings(
            executor, db_conn,
            gq2::builder::select({"meta_id"})
                .from("METADATA_MAP")
                .where(col("object_id") == objIdStr)
                .build());

        if (obj_meta_ids.empty()) {
            if (trans) {
                trans->commit();
            }
            return SUCCESS();
        }

        auto matching_meta_ids = irods::experimental::catalog::query_catalog_strings(
            executor, db_conn,
            gq2::builder::select({"meta_id"})
                .from("METADATA")
                .where(std::move(cond) && col("meta_id").in(obj_meta_ids))
                .build());

        if (!matching_meta_ids.empty()) {
            auto del = gq2::builder::remove_from("METADATA_MAP")
                .where(col("object_id") == objIdStr && col("meta_id").in(std::move(matching_meta_ids)))
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, del);
        }

        if (trans) {
            trans->commit();
        }
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_del_avu_metadata_op

irods::error db_copy_avu_metadata_op(
    irods::plugin_context& _ctx,
    const char*            _type1,
    const char*            _type2,
    const char*            _name1,
    const char*            _name2,
    const KeyValPair*      _cond_input)
{
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if (!_type1 || !_type2 || !_name1 || !_name2) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    char myTime[50];
    rodsLong_t objId1, objId2;

    log_sql::debug("chlCopyAVUMetadata");

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    const bool admin_mode = _cond_input && getValByKey(_cond_input, ADMIN_KW);

    if (admin_mode && !irods::is_privileged_client(*_ctx.comm())) {
        return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Insufficient privileges");
    }

    log_sql::debug("chlCopyAVUMetadata SQL 1 ");
    objId1 = checkAndGetObjectId(_ctx.comm(), _ctx.prop_map(), _type1, _name1, ACCESS_READ_METADATA, admin_mode);
    if ( objId1 < 0 ) {
        return ERROR( objId1, "checkAndGetObjectId failure" );
    }

    log_sql::debug("chlCopyAVUMetadata SQL 2");
    objId2 = checkAndGetObjectId(_ctx.comm(), _ctx.prop_map(), _type2, _name2, ACCESS_CREATE_METADATA, admin_mode);
    if ( objId2 < 0 ) {
        return ERROR( objId2, "checkAndGetObjectId failure" );
    }

    getNowStr( myTime );
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlCopyAVUMetadata SQL 3");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const std::string obj1_str = std::to_string(objId1);
        const std::string obj2_str = std::to_string(objId2);

        auto meta_ids = irods::experimental::catalog::query_catalog_strings(
            executor, db_conn,
            gq2::builder::select({"meta_id"})
                .from("METADATA_MAP")
                .where(col("object_id") == obj1_str)
                .build());

        for (const auto& mid : meta_ids) {
            auto ins = gq2::builder::insert_into("METADATA_MAP")
                .set("object_id", obj2_str)
                .set("meta_id", mid)
                .set("create_ts", myTime)
                .set("modify_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        _rollback( "chlCopyAVUMetadata" );
        return ERROR( translate_nanodbc_error(e), e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        _rollback( "chlCopyAVUMetadata" );
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_copy_avu_metadata_op

irods::error db_mod_access_control_resc_op(
    irods::plugin_context& _ctx,
    const int                    _recursive_flag,
    const char*                  _access_level,
    const char*                  _user_name,
    const char*                  _zone,
    const char*                  _resc_name ) {
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( !_access_level || !_user_name || !_resc_name ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    const std::string myAccessStr = _access_level + strlen( MOD_RESC_PREFIX );

    const char* myAccessLev = nullptr;
    int rmFlag = 0;
    if ( myAccessStr == AP_NULL ) {
        myAccessLev = ACCESS_NULL;
        rmFlag = 1;
    }
    else if ( myAccessStr == AP_READ ) {
        myAccessLev = ACCESS_READ_OBJECT;
    }
    else if ( myAccessStr == AP_WRITE ) {
        myAccessLev = ACCESS_MODIFY_OBJECT;
    }
    else if ( myAccessStr == AP_OWN ) {
        myAccessLev = ACCESS_OWN;
    }
    else {
        addRErrorMsg( &_ctx.comm()->rError, 0,
                      fmt::format( "access level '{}' is invalid for a resource", myAccessStr ).c_str() );
        return ERROR( CAT_INVALID_ARGUMENT, "invalid argument" );
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    const std::string my_zone = (_zone == nullptr || strlen(_zone) == 0) ? zone : _zone;

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        int64_t resc_id = 0;
        if ( _ctx.comm()->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH ) {
            auto resc_id_opt = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"resc_id"}).from("RESOURCE").where(col("resc_name") == _resc_name).build());
            if ( !resc_id_opt.has_value() ) {
                return ERROR( CAT_UNKNOWN_RESOURCE, "unknown resource" );
            }
            resc_id = *resc_id_opt;
        }
        else {
            auto access_status = irods::experimental::catalog::access_control::check_resource_access(
                executor, db_conn, _resc_name,
                _ctx.comm()->clientUser.userName,
                _ctx.comm()->clientUser.rodsZone,
                ACCESS_OWN);
            if ( access_status < 0 ) {
                return ERROR( access_status, "check_resource_access error" );
            }
            resc_id = access_status;
        }

        const std::string resc_id_str = std::to_string( resc_id );

        auto user_id_opt = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _user_name && col("zone_name") == my_zone)
                .build());
        if ( !user_id_opt.has_value() ) {
            return ERROR( CAT_INVALID_USER, "invalid user" );
        }
        const std::string user_id_str = std::to_string( *user_id_opt );

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto del_stmt = gq2::builder::remove_from("ACCESS")
            .where(col("user_id") == user_id_str && col("object_id") == resc_id_str)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

        if ( rmFlag == 0 ) {
            char myTime[50];
            getNowStr( myTime );
            const auto opt_token_id = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"token_id"})
                    .from("TOKEN")
                    .where(col("token_namespace") == "access_type" && col("token_name") == myAccessLev)
                    .build());
            if (!opt_token_id) {
                return ERROR(CAT_INVALID_ARGUMENT, "access token not found");
            }
            auto ins_stmt = gq2::builder::insert_into("ACCESS")
                .set("object_id", resc_id_str)
                .set("user_id", user_id_str)
                .set("access_type_id", std::to_string(*opt_token_id))
                .set("create_ts", myTime)
                .set("modify_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_mod_access_control_resc_op

irods::error db_mod_access_control_op(
    irods::plugin_context& _ctx,
    const int                    _recursive_flag,
    const char*                  _access_level,
    const char*                  _user_name,
    const char*                  _zone,
    const char*                  _path_name ) {
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlModAccessControl");

    if ( strncmp( _access_level, MOD_RESC_PREFIX, strlen( MOD_RESC_PREFIX ) ) == 0 ) {
        ret = db_mod_access_control_resc_op(
                  _ctx,
                  _recursive_flag,
                  _access_level,
                  _user_name,
                  _zone,
                  _path_name );
        return PASS( ret );
    }

    int adminMode = 0;
    char myAccessStr[LONG_NAME_LEN];
    if ( strncmp( _access_level, MOD_ADMIN_MODE_PREFIX,
                  strlen( MOD_ADMIN_MODE_PREFIX ) ) == 0 ) {
        if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          "You must be the admin to use the -M admin mode" );
            return ERROR( CAT_NO_ACCESS_PERMISSION, "You must be the admin to use the -M admin mode" );
        }
        snprintf( myAccessStr, sizeof( myAccessStr ), "%s", _access_level + strlen( MOD_ADMIN_MODE_PREFIX ) );
        _access_level = myAccessStr;
        adminMode = 1;
    }

    static const std::vector<char const*> allowed_access_levels{ACCESS_EXECUTE,
                                                                ACCESS_READ_ANNOTATION,
                                                                ACCESS_READ_SYSTEM_METADATA,
                                                                ACCESS_READ_METADATA,
                                                                ACCESS_READ_OBJECT,
                                                                ACCESS_WRITE_ANNOTATION,
                                                                ACCESS_CREATE_METADATA,
                                                                ACCESS_MODIFY_METADATA,
                                                                ACCESS_DELETE_METADATA,
                                                                ACCESS_ADMINISTER_OBJECT,
                                                                ACCESS_CREATE_OBJECT,
                                                                ACCESS_MODIFY_OBJECT,
                                                                ACCESS_DELETE_OBJECT,
                                                                ACCESS_CREATE_TOKEN,
                                                                ACCESS_DELETE_TOKEN,
                                                                ACCESS_CURATE,
                                                                ACCESS_OWN};
    const char* myAccessLev = NULL;
    int rmFlag = 0;
    int inheritFlag = 0;
    if ( strcmp( _access_level, AP_NULL ) == 0 ) {
        myAccessLev = ACCESS_NULL;
        rmFlag = 1;
    }
    else if ( strcmp( _access_level, AP_READ ) == 0 ) {
        myAccessLev = ACCESS_READ_OBJECT;
    }
    else if ( strcmp( _access_level, AP_WRITE ) == 0 ) {
        myAccessLev = ACCESS_MODIFY_OBJECT;
    }
    else if ( strcmp( _access_level, ACCESS_INHERIT ) == 0 ) {
        inheritFlag = 1;
    }
    else if ( strcmp( _access_level, ACCESS_NO_INHERIT ) == 0 ) {
        inheritFlag = 2;
    }
    else {
        for (char const* level : allowed_access_levels) {
            if (!strcmp(_access_level, level)) {
                myAccessLev = level;
                break;
            }
        }
        if (myAccessLev == NULL) {
            const auto errMsg = fmt::format("access level '{}' is invalid", _access_level);
            addRErrorMsg(&_ctx.comm()->rError, 0, errMsg.c_str());
            return ERROR(CAT_INVALID_ARGUMENT, errMsg);
        }
    }

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    int status1;
    if ( adminMode ) {
        /* See if the input path is a collection
           and, if so, get the collectionID */
        log_sql::debug("chlModAccessControl SQL 14");
        auto opt_coll_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == _path_name)
                .build());
        if ( !opt_coll_id ) {
            status1 = CAT_UNKNOWN_COLLECTION;
        }
        else {
            status1 = *opt_coll_id;
        }
    }
    else {
        /* See if the input path is a collection and the user owns it,
           and, if so, get the collectionID */
        log_sql::debug("chlModAccessControl SQL 1 ");
        status1 = irods::experimental::catalog::access_control::check_collection_access(
            executor, db_conn,
            _path_name,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_OWN );
    }
    std::string collIdStr;
    if ( status1 >= 0 ) {
        collIdStr = std::to_string( status1 );
    }

    if ( status1 < 0 && inheritFlag != 0 ) {
        constexpr auto errMsg = "either the collection does not exist or you do not have sufficient access";
        addRErrorMsg( &_ctx.comm()->rError, 0, errMsg );
        return ERROR( CAT_NO_ACCESS_PERMISSION, errMsg );
    }

    rodsLong_t objId = 0;

    /* Not a collection (with access for non-Admin) */
    if ( status1 < 0 ) {
        char logicalEndName[MAX_NAME_LEN];
        char logicalParentDirName[MAX_NAME_LEN];
        if (const auto ec = splitPathByKey(_path_name, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
            return ERROR(ec, fmt::format(
                         "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                         __func__, __LINE__, _path_name, ec));
        }

        if ( strlen( logicalParentDirName ) == 0 ) {
            snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
            snprintf( logicalEndName, sizeof( logicalEndName ), "%s", _path_name + 1 );
        }

        int status2 = 0;
        if ( adminMode ) {
            log_sql::debug("chlModAccessControl SQL 15");
            const auto opt_parent_id = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"coll_id"})
                    .from("COLLECTION")
                    .where(col("coll_name") == logicalParentDirName)
                    .build());
            std::optional<int64_t> opt_data_id;
            if (opt_parent_id) {
                opt_data_id = irods::experimental::catalog::query_catalog_integer(
                    executor,
                    db_conn,
                    gq2::builder::select({"data_id"})
                        .from("DATA_OBJECT")
                        .where(col("data_name") == logicalEndName && col("coll_id") == std::to_string(*opt_parent_id))
                        .build());
            }
            if ( !opt_data_id ) {
                status2 = CAT_UNKNOWN_FILE;
            }
            else {
                status2 = *opt_data_id;
            }
        }
        else {
            /* Not a collection with access, so see if the input path dataObj
               exists and the user owns it, and, if so, get the objectID */
            log_sql::debug("chlModAccessControl SQL 2");
            status2 = irods::experimental::catalog::access_control::check_data_object_only(
                executor, db_conn,
                logicalParentDirName, logicalEndName,
                _ctx.comm()->clientUser.userName,
                _ctx.comm()->clientUser.rodsZone,
                ACCESS_OWN );
        }
        if ( status2 > 0 ) {
            objId = status2;
        }
        /* If both failed, it doesn't exist or there's no permission */
        else if ( status2 < 0 ) {
            if ( status1 == CAT_UNKNOWN_COLLECTION && status2 == CAT_UNKNOWN_FILE ) {
                addRErrorMsg( &_ctx.comm()->rError, 0,
                              fmt::format( "Input path is not a collection and not a dataObj: {}", _path_name ).c_str() );
                return ERROR( CAT_INVALID_ARGUMENT, "unknown collection or file" );
            }
            if ( status1 != CAT_UNKNOWN_COLLECTION ) {
                return ERROR(
                    status1,
                    fmt::format(
                        "User [{}] has insufficient permission to modifiy collection: [{}]",
                        _ctx.comm()->clientUser.userName,
                        _path_name));
            }
            else {
                if ( status2 == CAT_NO_ACCESS_PERMISSION ) {
                    return ERROR(
                        status2,
                        fmt::format(
                            "User [{}] has insufficient permission to modifiy data object: [{}]",
                            _ctx.comm()->clientUser.userName,
                            _path_name));
                }
                else {
                    return ERROR( status2, "check_data_object_only failed" );
                }
            }
        }
    }

    /* Doing inheritance */
    if ( inheritFlag != 0 ) {
        const int status = _modInheritance( inheritFlag, _recursive_flag, collIdStr.c_str(), _path_name );
        if ( status != 0 ) {
            return ERROR( status, "_modInheritance failed" );
        }
        return SUCCESS();
    }

    /* Check that the receiving user exists and if so get the userId */
    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    const char *myZone = ( _zone && strlen( _zone ) != 0 ) ? _zone : zone.c_str();

    rodsLong_t userId = 0;
    log_sql::debug("chlModAccessControl SQL 3");
    {
        auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _user_name && col("zone_name") == myZone)
                .build());
        if ( !opt_user_id ) {
            return ERROR( CAT_INVALID_USER, "invalid user" );
        }
        userId = *opt_user_id;
    }

    const auto userIdStr = std::to_string( userId );
    const auto objIdStr = std::to_string( objId );

    log_db::debug("recursiveFlag {}", _recursive_flag);

    /* non-Recursive mode */
    if ( _recursive_flag == 0 ) {
        try {
            nanodbc::transaction trans{db_conn};
            namespace gq2 = irods::experimental::genquery2;
            using gq2::builder::col;

            /* doing a dataObj */
            if ( objId ) {
                log_sql::debug("chlModAccessControl SQL 4");
                auto del_stmt = gq2::builder::remove_from("ACCESS")
                    .where(col("user_id") == userIdStr && col("object_id") == objIdStr)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

                if ( rmFlag == 0 ) { /* if not just removing: */
                    char myTime[50];
                    getNowStr( myTime );
                    log_sql::debug("chlModAccessControl SQL 5");
                    const auto opt_token_id = irods::experimental::catalog::query_catalog_integer(
                        executor, db_conn,
                        gq2::builder::select({"token_id"})
                            .from("TOKEN")
                            .where(col("token_namespace") == "access_type" && col("token_name") == myAccessLev)
                            .build());
                    if (!opt_token_id) {
                        return ERROR(CAT_INVALID_ARGUMENT, "access token not found");
                    }
                    auto ins_stmt = gq2::builder::insert_into("ACCESS")
                        .set("object_id", objIdStr)
                        .set("user_id", userIdStr)
                        .set("access_type_id", std::to_string(*opt_token_id))
                        .set("create_ts", myTime)
                        .set("modify_ts", myTime)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
                }

                trans.commit();
                return SUCCESS();
            }

            /* doing a collection, non-recursive */
            log_sql::debug("chlModAccessControl SQL 6");
            auto del_stmt = gq2::builder::remove_from("ACCESS")
                .where(col("user_id") == userIdStr && col("object_id") == collIdStr)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

            if ( rmFlag == 0 ) {
                char myTime[50];
                getNowStr( myTime );
                log_sql::debug("chlModAccessControl SQL 7");
                const auto opt_token_id = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"token_id"})
                        .from("TOKEN")
                        .where(col("token_namespace") == "access_type" && col("token_name") == myAccessLev)
                        .build());
                if (!opt_token_id) {
                    return ERROR(CAT_INVALID_ARGUMENT, "access token not found");
                }
                auto ins_stmt = gq2::builder::insert_into("ACCESS")
                    .set("object_id", collIdStr)
                    .set("user_id", userIdStr)
                    .set("access_type_id", std::to_string(*opt_token_id))
                    .set("create_ts", myTime)
                    .set("modify_ts", myTime)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
            }

            trans.commit();
            return SUCCESS();
        }
        catch (const nanodbc::database_error& e) {
            log_db::error("{}: database error: {}", __FUNCTION__, e.what());
            return ERROR( CAT_SQL_ERR, e.what() );
        }
        catch (const std::exception& e) {
            log_db::error("{}: exception: {}", __FUNCTION__, e.what());
            return ERROR( SYS_INTERNAL_ERR, e.what() );
        }
    }


    /* Recursive */
    if ( objId ) {
        const auto errMsg = fmt::format(
            "Input path is not a collection and recursion was requested: {}",
            _path_name );
        addRErrorMsg( &_ctx.comm()->rError, 0, errMsg.c_str() );
        return ERROR( CAT_INVALID_ARGUMENT, errMsg );
    }


    std::string pathStart = makeEscapedPath( _path_name ) + "/%";

    try {
        nanodbc::transaction trans{db_conn};
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        // Query all matching collection IDs (root + subcollections)
        auto matching_colls = irods::experimental::catalog::query_catalog_strings(
            executor, db_conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == _path_name || col("coll_name").like(pathStart))
                .build());

        // Resolve access token if adding/modifying permission
        std::string access_token_str;
        char myTime[50]{};
        if ( !rmFlag ) {
            getNowStr( myTime );
            const auto opt_token_id = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"token_id"})
                    .from("TOKEN")
                    .where(col("token_namespace") == "access_type" && col("token_name") == myAccessLev)
                    .build());
            if ( !opt_token_id ) {
                return ERROR(CAT_INVALID_ARGUMENT, "access token not found");
            }
            access_token_str = std::to_string(*opt_token_id);
        }

        constexpr std::size_t batch_size = 500;

        for ( std::size_t c_idx = 0; c_idx < matching_colls.size(); c_idx += batch_size ) {
            const auto chunk_end = std::min( c_idx + batch_size, matching_colls.size() );
            std::vector<std::string> coll_chunk(
                matching_colls.begin() + c_idx,
                matching_colls.begin() + chunk_end );

            // Batch delete collection access
            auto del_coll_access = gq2::builder::remove_from("ACCESS")
                .where(col("user_id") == userIdStr && col("object_id").in(coll_chunk))
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, del_coll_access);

            if ( !rmFlag && !access_token_str.empty() ) {
                for ( const auto& cid : coll_chunk ) {
                    auto ins_coll_access = gq2::builder::insert_into("ACCESS")
                        .set("object_id", cid)
                        .set("user_id", userIdStr)
                        .set("access_type_id", access_token_str)
                        .set("create_ts", myTime)
                        .set("modify_ts", myTime)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins_coll_access);
                }
            }

            // Batch query data objects belonging to this batch of collections
            auto coll_data_ids = irods::experimental::catalog::query_catalog_strings(
                executor, db_conn,
                gq2::builder::select({"data_id"})
                    .from("DATA_OBJECT")
                    .where(col("coll_id").in(coll_chunk))
                    .build());

            // Process data objects in batches
            for ( std::size_t d_idx = 0; d_idx < coll_data_ids.size(); d_idx += batch_size ) {
                const auto d_chunk_end = std::min( d_idx + batch_size, coll_data_ids.size() );
                std::vector<std::string> data_chunk(
                    coll_data_ids.begin() + d_idx,
                    coll_data_ids.begin() + d_chunk_end );

                auto del_data_access = gq2::builder::remove_from("ACCESS")
                    .where(col("user_id") == userIdStr && col("object_id").in(data_chunk))
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_data_access);

                if ( !rmFlag && !access_token_str.empty() ) {
                    for ( const auto& did : data_chunk ) {
                        auto ins_data_access = gq2::builder::insert_into("ACCESS")
                            .set("object_id", did)
                            .set("user_id", userIdStr)
                            .set("access_type_id", access_token_str)
                            .set("create_ts", myTime)
                            .set("modify_ts", myTime)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_data_access);
                    }
                }
            }
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_mod_access_control_op

irods::error db_rename_object_op(
    irods::plugin_context& _ctx,
    rodsLong_t             _obj_id,
    const char*            _new_name ) {
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( !_new_name ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    }

    if ( strstr( _new_name, PATH_SEPARATOR ) ) {
        return ERROR( CAT_INVALID_ARGUMENT, "new name invalid" );
    }

    const std::string obj_id_str = std::to_string( _obj_id );
    const std::string user_name = _ctx.comm()->clientUser.userName;
    const std::string user_zone = _ctx.comm()->clientUser.rodsZone;

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        // Try as data object
        int own_access = irods::experimental::catalog::access_control::check_data_object_id(
            executor, db_conn, obj_id_str, user_name, user_zone, ACCESS_OWN);
        if ( own_access == 0 ) {
            auto coll_id_opt = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"COLL_ID"})
                    .from("DATA_OBJECT")
                    .where(col("DATA_ID") == obj_id_str)
                    .build());
            if ( coll_id_opt.has_value() ) {
                const int64_t coll_id = *coll_id_opt;
                const std::string coll_id_str = std::to_string( coll_id );

                // Check that no other dataObj exists with this name in this collection
                auto other_data = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"DATA_ID"})
                        .from("DATA_OBJECT")
                        .where(col("DATA_NAME") == _new_name && col("COLL_ID") == coll_id_str)
                        .build());
                if ( other_data.has_value() ) {
                    return ERROR( CAT_NAME_EXISTS_AS_DATAOBJ, "select data_id failed" );
                }

                // Check that no subcoll exists in this collection with the _new_name
                auto current_coll_name = irods::experimental::catalog::query_catalog_string(
                    executor, db_conn,
                    gq2::builder::select({"COLL_NAME"})
                        .from("COLLECTION")
                        .where(col("COLL_ID") == coll_id_str)
                        .build());
                if ( current_coll_name.has_value() ) {
                    const std::string coll_name_tmp = *current_coll_name + "/" + _new_name;
                    auto other_coll = irods::experimental::catalog::query_catalog_integer(
                        executor, db_conn,
                        gq2::builder::select({"COLL_ID"})
                            .from("COLLECTION")
                            .where(col("COLL_NAME") == coll_name_tmp)
                            .build());
                    if ( other_coll.has_value() ) {
                        return ERROR( CAT_NAME_EXISTS_AS_COLLECTION, "select coll_id failed" );
                    }
                }

                char my_time[50];
                getNowStr( my_time );

                auto upd_data = gq2::builder::update("DATA_OBJECT")
                    .set("data_name", _new_name)
                    .where(col("data_id") == obj_id_str)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_data);

                auto upd_coll = gq2::builder::update("COLLECTION")
                    .set("modify_ts", my_time)
                    .where(col("coll_id") == coll_id_str)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_coll);

                trans.commit();
                return SUCCESS();
            }
        }

        // Try as collection
        int coll_access = irods::experimental::catalog::access_control::check_collection_id(
            executor, db_conn, obj_id_str, user_name, user_zone, ACCESS_OWN);
        if ( coll_access >= 0 ) {
            auto coll_res = irods::experimental::catalog::execute_catalog(
                executor, db_conn,
                gq2::builder::select({"parent_coll_name", "coll_name"})
                    .from("COLLECTION")
                    .where(col("coll_id") == obj_id_str)
                    .build());
            if ( coll_res.query_result && coll_res.query_result->next() ) {
                const std::string parent_coll_name = coll_res.query_result->get<std::string>(0, "");
                const std::string coll_name = coll_res.query_result->get<std::string>(1, "");

                // Check that no other dataObj exists with this name in parent collection
                auto parent_coll_id = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"COLL_ID"})
                        .from("COLLECTION")
                        .where(col("COLL_NAME") == parent_coll_name)
                        .build());
                if ( parent_coll_id.has_value() ) {
                    auto other_data = irods::experimental::catalog::query_catalog_integer(
                        executor, db_conn,
                        gq2::builder::select({"DATA_ID"})
                            .from("DATA_OBJECT")
                            .where(col("DATA_NAME") == _new_name && col("COLL_ID") == std::to_string(*parent_coll_id))
                            .build());
                    if ( other_data.has_value() ) {
                        return ERROR( CAT_NAME_EXISTS_AS_DATAOBJ, "select data_id failed" );
                    }
                }

                // Check that no subcoll exists in the parent collection with the _new_name
                const std::string coll_name_tmp = parent_coll_name + "/" + _new_name;
                auto other_coll = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"COLL_ID"})
                        .from("COLLECTION")
                        .where(col("COLL_NAME") == coll_name_tmp)
                        .build());
                if ( other_coll.has_value() ) {
                    return ERROR( CAT_NAME_EXISTS_AS_COLLECTION, "select coll_id failed" );
                }

                if ( parent_coll_name.empty() || coll_name.empty() ) {
                    return ERROR( CAT_INVALID_ARGUMENT, "coll name or parent is invalid" );
                }

                const bool is_root_dir = ( parent_coll_name == "/" );
                const std::string final_coll_name = is_root_dir ? (parent_coll_name + _new_name) : (parent_coll_name + "/" + _new_name);

                // Update subcollections under this collection
                const std::string coll_name_slash = coll_name + "/";
                auto subcolls = irods::experimental::catalog::execute_catalog(
                    executor, db_conn,
                    gq2::builder::select({"coll_id", "coll_name", "parent_coll_name"})
                        .from("COLLECTION")
                        .where(col("parent_coll_name") == coll_name || col("parent_coll_name").like(coll_name_slash + "%"))
                        .build());

                std::vector<std::tuple<std::string, std::string, std::string>> sub_updates;
                if (subcolls.query_result) {
                    while (subcolls.query_result->next()) {
                        auto cid = subcolls.query_result->get<std::string>(0);
                        auto cname = subcolls.query_result->get<std::string>(1);
                        auto pname = subcolls.query_result->get<std::string>(2);
                        if (cname == coll_name) {
                            cname = final_coll_name;
                        } else if (cname.starts_with(coll_name_slash)) {
                            cname = final_coll_name + cname.substr(coll_name.size());
                        }
                        if (pname == coll_name) {
                            pname = final_coll_name;
                        } else if (pname.starts_with(coll_name_slash)) {
                            pname = final_coll_name + pname.substr(coll_name.size());
                        }
                        sub_updates.emplace_back(std::move(cid), std::move(cname), std::move(pname));
                    }
                }

                for (const auto& [cid, new_cname, new_pname] : sub_updates) {
                    auto upd = gq2::builder::update("COLLECTION")
                        .set("coll_name", new_cname)
                        .set("parent_coll_name", new_pname)
                        .where(col("coll_id") == cid)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
                }

                char my_time[50];
                getNowStr( my_time );

                auto upd_coll = gq2::builder::update("COLLECTION")
                    .set("coll_name", final_coll_name)
                    .set("modify_ts", my_time)
                    .where(col("coll_id") == obj_id_str)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_coll);

                trans.commit();
                return SUCCESS();
            }
        }

        // Both collection and dataObj failed, check if object exists to return proper error
        auto data_check = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"COLL_ID"}).from("DATA_OBJECT").where(col("DATA_ID") == obj_id_str).build());
        if ( data_check.has_value() ) {
            return ERROR( CAT_NO_ACCESS_PERMISSION, "select coll_id failed" );
        }

        auto coll_check = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"COLL_ID"}).from("COLLECTION").where(col("COLL_ID") == obj_id_str).build());
        if ( coll_check.has_value() ) {
            return ERROR( CAT_NO_ACCESS_PERMISSION, "select coll_id failed" );
        }

        return ERROR( CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION, "not a collection" );
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_rename_object_op

irods::error db_move_object_op(
    irods::plugin_context& _ctx,
    rodsLong_t             _obj_id,
    rodsLong_t             _target_coll_id ) {
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    const std::string obj_id_str = std::to_string( _obj_id );
    const std::string target_coll_id_str = std::to_string( _target_coll_id );
    const std::string user_name = _ctx.comm()->clientUser.userName;
    const std::string user_zone = _ctx.comm()->clientUser.rodsZone;

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        // Check that target collection exists and user has write (modify_object) permission
        int target_access = irods::experimental::catalog::access_control::check_collection_id(
            executor, db_conn, target_coll_id_str, user_name, user_zone, ACCESS_MODIFY_OBJECT);
        if ( target_access < 0 ) {
            // Does target coll exist?
            auto exists = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"COLL_ID"}).from("COLLECTION").where(col("COLL_ID") == target_coll_id_str).build());
            if ( exists.has_value() ) {
                return ERROR( CAT_NO_ACCESS_PERMISSION, "permission error" );
            }
            return ERROR( CAT_UNKNOWN_COLLECTION, "target is not a collection" );
        }

        auto target_coll_name_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"COLL_NAME"}).from("COLLECTION").where(col("COLL_ID") == target_coll_id_str).build());
        const std::string target_coll_name = target_coll_name_opt.value_or("");

        // Try as data object
        int own_access = irods::experimental::catalog::access_control::check_data_object_id(
            executor, db_conn, obj_id_str, user_name, user_zone, ACCESS_OWN);
        if ( own_access == 0 ) {
            auto data_name_opt = irods::experimental::catalog::query_catalog_string(
                executor, db_conn,
                gq2::builder::select({"DATA_NAME"}).from("DATA_OBJECT").where(col("DATA_ID") == obj_id_str).build());
            if ( data_name_opt.has_value() ) {
                const std::string data_obj_name = *data_name_opt;

                // Check that no other dataObj exists with this name in target coll
                auto other_data = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"DATA_ID"})
                        .from("DATA_OBJECT")
                        .where(col("DATA_NAME") == data_obj_name && col("COLL_ID") == target_coll_id_str)
                        .build());
                if ( other_data.has_value() ) {
                    return ERROR( CAT_NAME_EXISTS_AS_DATAOBJ, "select data_id failed" );
                }

                // Check that no subcoll exists in the target collection with the name of the object
                const std::string subcoll_target_name = target_coll_name + "/" + data_obj_name;
                auto other_coll = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"COLL_ID"})
                        .from("COLLECTION")
                        .where(col("COLL_NAME") == subcoll_target_name)
                        .build());
                if ( other_coll.has_value() ) {
                    return ERROR( CAT_NAME_EXISTS_AS_COLLECTION, "select coll_id failed" );
                }

                char my_time[50];
                getNowStr( my_time );

                auto upd_data = gq2::builder::update("DATA_OBJECT")
                    .set("coll_id", target_coll_id_str)
                    .set("modify_ts", my_time)
                    .where(col("data_id") == obj_id_str)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_data);

                auto upd_coll = gq2::builder::update("COLLECTION")
                    .set("modify_ts", my_time)
                    .where(col("coll_id") == target_coll_id_str)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_coll);

                trans.commit();
                return SUCCESS();
            }
        }

        // Try as collection
        int coll_access = irods::experimental::catalog::access_control::check_collection_id(
            executor, db_conn, obj_id_str, user_name, user_zone, ACCESS_OWN);
        if ( coll_access >= 0 ) {
            auto coll_res = irods::experimental::catalog::execute_catalog(
                executor, db_conn,
                gq2::builder::select({"parent_coll_name", "coll_name"}).from("COLLECTION").where(col("coll_id") == obj_id_str).build());
            if ( coll_res.query_result && coll_res.query_result->next() ) {
                const std::string parent_coll_name = coll_res.query_result->get<std::string>(0, "");
                const std::string old_coll_name = coll_res.query_result->get<std::string>(1, "");

                if ( parent_coll_name.empty() || old_coll_name.empty() ) {
                    return ERROR( CAT_INVALID_ARGUMENT, "parent or coll name null" );
                }

                const auto last_slash = old_coll_name.rfind('/');
                if ( last_slash == std::string::npos ) {
                    return ERROR( CAT_INVALID_ARGUMENT, "OK == 0" );
                }
                const std::string end_coll_name = old_coll_name.substr(last_slash + 1);

                // Check write access to source collection
                const auto dir_access = irods::experimental::catalog::access_control::check_collection_access(
                    executor, db_conn, parent_coll_name, user_name, user_zone, ACCESS_MODIFY_OBJECT);
                if ( dir_access < 0 ) {
                    return ERROR( dir_access, "check_collection_access failed" );
                }

                // Check that no other dataObj exists with end_coll_name in target coll
                auto other_data = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"DATA_ID"})
                        .from("DATA_OBJECT")
                        .where(col("DATA_NAME") == end_coll_name && col("COLL_ID") == target_coll_id_str)
                        .build());
                if ( other_data.has_value() ) {
                    return ERROR( CAT_NAME_EXISTS_AS_DATAOBJ, "select data_id failed" );
                }

                // Check that no subcoll exists in target collection with end_coll_name
                const std::string new_coll_name = target_coll_name + "/" + end_coll_name;
                auto other_coll = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"COLL_ID"})
                        .from("COLLECTION")
                        .where(col("COLL_NAME") == new_coll_name)
                        .build());
                if ( other_coll.has_value() ) {
                    return ERROR( CAT_NAME_EXISTS_AS_COLLECTION, "select coll_id failed" );
                }

                // Check that we're not moving the coll down into its own subtree
                if ( target_coll_name.rfind(old_coll_name, 0) == 0 &&
                     (target_coll_name.size() == old_coll_name.size() || target_coll_name[old_coll_name.size()] == '/') ) {
                    return ERROR( CAT_RECURSIVE_MOVE, "moving coll into own subtree" );
                }

                // Update this collection's row
                char my_time[50];
                getNowStr( my_time );

                auto upd_coll_main = gq2::builder::update("COLLECTION")
                    .set("coll_name", new_coll_name)
                    .set("parent_coll_name", target_coll_name)
                    .set("modify_ts", my_time)
                    .where(col("coll_id") == obj_id_str)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, upd_coll_main);

                // Update any collections under this collection
                const std::string coll_name_slash = old_coll_name + "/";
                auto subcolls = irods::experimental::catalog::execute_catalog(
                    executor, db_conn,
                    gq2::builder::select({"coll_id", "coll_name", "parent_coll_name"})
                        .from("COLLECTION")
                        .where(col("parent_coll_name") == old_coll_name || col("parent_coll_name").like(coll_name_slash + "%"))
                        .build());

                std::vector<std::tuple<std::string, std::string, std::string>> sub_updates;
                if (subcolls.query_result) {
                    while (subcolls.query_result->next()) {
                        auto cid = subcolls.query_result->get<std::string>(0);
                        auto cname = subcolls.query_result->get<std::string>(1);
                        auto pname = subcolls.query_result->get<std::string>(2);
                        if (cname == old_coll_name) {
                            cname = new_coll_name;
                        } else if (cname.starts_with(coll_name_slash)) {
                            cname = new_coll_name + cname.substr(old_coll_name.size());
                        }
                        if (pname == old_coll_name) {
                            pname = new_coll_name;
                        } else if (pname.starts_with(coll_name_slash)) {
                            pname = new_coll_name + pname.substr(old_coll_name.size());
                        }
                        sub_updates.emplace_back(std::move(cid), std::move(cname), std::move(pname));
                    }
                }

                for (const auto& [cid, updated_cname, updated_pname] : sub_updates) {
                    auto upd = gq2::builder::update("COLLECTION")
                        .set("coll_name", updated_cname)
                        .set("parent_coll_name", updated_pname)
                        .where(col("coll_id") == cid)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd);
                }

                trans.commit();
                return SUCCESS();
            }
        }

        // Both collection and dataObj failed, determine specific error
        auto data_check = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"COLL_ID"}).from("DATA_OBJECT").where(col("DATA_ID") == obj_id_str).build());
        if ( data_check.has_value() ) {
            return ERROR( CAT_NO_ACCESS_PERMISSION, "select coll_id failed" );
        }

        auto coll_check = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"COLL_ID"}).from("COLLECTION").where(col("COLL_ID") == obj_id_str).build());
        if ( coll_check.has_value() ) {
            return ERROR( CAT_NO_ACCESS_PERMISSION, "select coll_id failed" );
        }

        return ERROR( CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION, "invalid object or collection" );
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_move_object_op

irods::error db_reg_token_op(
    irods::plugin_context& _ctx,
    const char*            _name_space,
    const char*            _name,
    const char*            _value,
    const char*            _value2,
    const char*            _value3,
    const char*            _comment ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlRegToken");

    if ( _name_space == NULL || strlen( _name_space ) == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "namespace null or 0 len" );
    }
    if ( _name == NULL || strlen( _name ) == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "name null or 0 len" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        log_sql::debug("chlRegToken SQL 1 ");
        const auto ns_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"token_id"})
                .from("TOKEN")
                .where(col("token_namespace") == "token_namespace" && col("token_name") == _name_space)
                .build());

        if (!ns_id.has_value()) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "Token namespace '{}' does not exist", _name_space ).c_str() );
            return ERROR( CAT_INVALID_ARGUMENT, "namespace does not exist" );
        }

        log_sql::debug("chlRegToken SQL 2");
        const auto token_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"token_id"})
                .from("TOKEN")
                .where(col("token_namespace") == _name_space && col("token_name") == _name)
                .build());

        if (token_id.has_value()) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "Token '{}' already exists in namespace '{}'", _name, _name_space ).c_str() );
            return ERROR( CAT_INVALID_ARGUMENT, "token is already in namespace" );
        }

        const char *myValue1 = _value ? _value : "";
        const char *myValue2 = _value2 ? _value2 : "";
        const char *myValue3 = _value3 ? _value3 : "";
        const char *myComment = _comment ? _comment : "";

        log_sql::debug("chlRegToken SQL 3");
        const int64_t seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");

        char myTime[50];
        getNowStr( myTime );

        log_sql::debug("chlRegToken SQL 4");
        auto ins = gq2::builder::insert_into("TOKEN")
            .set("token_namespace", _name_space)
            .set("token_id", std::to_string(seqNum))
            .set("token_name", _name)
            .set("token_value", myValue1)
            .set("token_value2", myValue2)
            .set("token_value3", myValue3)
            .set("r_comment", myComment)
            .set("create_ts", myTime)
            .set("modify_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_reg_token_op

irods::error db_del_token_op(
    irods::plugin_context& _ctx,
    const char*            _name_space,
    const char*            _name ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlDelToken");

    if ( _name_space == NULL || strlen( _name_space ) == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "namespace is null or 0 len" );
    }
    if ( _name == NULL || strlen( _name ) == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "name is null or 0 len" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        log_sql::debug("chlDelToken SQL 1 ");
        const auto objId = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"token_id"})
                .from("TOKEN")
                .where(col("token_namespace") == _name_space && col("token_name") == _name)
                .build());

        if (!objId.has_value()) {
            addRErrorMsg( &_ctx.comm()->rError, 0,
                          fmt::format( "Token '{}' does not exist in namespace '{}'", _name, _name_space ).c_str() );
            return ERROR( CAT_INVALID_ARGUMENT, "token is not in namespace" );
        }

        log_sql::debug("chlDelToken SQL 2");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto del_tokn = gq2::builder::remove_from("TOKEN")
            .where(col("token_namespace") == _name_space && col("token_name") == _name)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_tokn);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_del_token_op

irods::error db_reg_server_load_op(
    irods::plugin_context& _ctx,
    const char*            _host_name,
    const char*            _resc_name,
    const char*            _cpu_used,
    const char*            _mem_used,
    const char*            _swap_used,
    const char*            _run_q_load,
    const char*            _disk_space,
    const char*            _net_input,
    const char*            _net_output ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlRegServerLoad");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        char myTime[50];
        getNowStr( myTime );

        log_sql::debug("chlRegServerLoad SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        auto ins = gq2::builder::insert_into("SERVER_LOAD")
            .set("host_name", _host_name ? _host_name : "")
            .set("resc_name", _resc_name ? _resc_name : "")
            .set("cpu_used", _cpu_used ? _cpu_used : "")
            .set("mem_used", _mem_used ? _mem_used : "")
            .set("swap_used", _swap_used ? _swap_used : "")
            .set("runq_load", _run_q_load ? _run_q_load : "")
            .set("disk_space", _disk_space ? _disk_space : "")
            .set("net_input", _net_input ? _net_input : "")
            .set("net_output", _net_output ? _net_output : "")
            .set("create_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_reg_server_load_op

irods::error db_purge_server_load_op(
    irods::plugin_context& _ctx,
    const char*            _seconds_ago ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlPurgeServerLoad");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        char nowStr[50];
        getNowStr( nowStr );
        time_t nowTime = atoll( nowStr );
        time_t secondsAgoTime = atoll( _seconds_ago );
        time_t thenTime = nowTime - secondsAgoTime;
        const auto thenStr = fmt::format( "{:011d}", static_cast<unsigned int>( thenTime ) );

        log_sql::debug("chlPurgeServerLoad SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto del_load = gq2::builder::remove_from("R_SERVER_LOAD")
            .where(col("create_ts") < thenStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_load);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_purge_server_load_op

irods::error db_reg_server_load_digest_op(
    irods::plugin_context& _ctx,
    const char*            _resc_name,
    const char*            _load_factor ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlRegServerLoadDigest");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        char myTime[50];
        getNowStr( myTime );

        log_sql::debug("chlRegServerLoadDigest SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        auto ins = gq2::builder::insert_into("SERVER_LOAD_DIGEST")
            .set("resc_name", _resc_name ? _resc_name : "")
            .set("load_factor", _load_factor ? _load_factor : "")
            .set("create_ts", myTime)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_reg_server_load_digest_op

irods::error db_purge_server_load_digest_op(
    irods::plugin_context& _ctx,
    const char*            _seconds_ago ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlPurgeServerLoadDigest");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        char nowStr[50];
        getNowStr( nowStr );
        time_t nowTime = atoll( nowStr );
        time_t secondsAgoTime = atoll( _seconds_ago );
        time_t thenTime = nowTime - secondsAgoTime;
        const auto thenStr = fmt::format( "{:011d}", static_cast<unsigned int>( thenTime ) );

        log_sql::debug("chlPurgeServerLoadDigest SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto del_digest = gq2::builder::remove_from("R_SERVER_LOAD_DIGEST")
            .where(col("create_ts") < thenStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_digest);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_purge_server_load_digest_op

irods::error db_get_grid_configuration_value_op(
    irods::plugin_context& _ctx,
    const char*            _namespace,
    const char*            _option_name,
    char*                  _option_value,
    std::size_t            _option_value_buffer_size)
{
    if (const irods::error ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    log_sql::debug("chlGetGridConfigurationValue");

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto val_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"option_value"})
                .from("GRID_CONFIGURATION")
                .where(col("namespace") == (_namespace ? _namespace : "") &&
                       col("option_name") == (_option_name ? _option_name : ""))
                .build());

        if (!val_opt.has_value()) {
            return ERROR(CAT_NO_ROWS_FOUND, "Get Grid Configuration Value select failure");
        }

        std::strncpy(_option_value, val_opt->c_str(), _option_value_buffer_size);
        if (_option_value_buffer_size > 0) {
            _option_value[_option_value_buffer_size - 1] = '\0';
        }
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
} // db_get_grid_configuration_value_op

irods::error db_set_grid_configuration_value_op(
    irods::plugin_context& _ctx,
    const char*            _namespace,
    const char*            _option_name,
    const char*            _option_value)
{
    if (!irods::is_privileged_client(*_ctx.comm())) {
        return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level");
    }

    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    log_sql::debug("chlSetGridConfigurationValue");

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto val_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"option_value"})
                .from("GRID_CONFIGURATION")
                .where(col("namespace") == (_namespace ? _namespace : "") &&
                       col("option_name") == (_option_name ? _option_name : ""))
                .build());

        if (!val_opt.has_value()) {
            return ERROR(CAT_NO_ROWS_FOUND, "Set Grid Configuration Value select failure");
        }

        log_sql::debug("chlSetGridConfigurationValue SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto upd = gq2::builder::update("R_GRID_CONFIGURATION")
            .set("option_value", _option_value ? _option_value : "")
            .where(col("namespace") == (_namespace ? _namespace : "") &&
                   col("option_name") == (_option_name ? _option_name : ""))
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
} // db_set_grid_configuration_value_op

irods::error db_calc_usage_and_quota_op(
    irods::plugin_context& _ctx ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    log_db::info("chlCalcUsageAndQuota called");

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        char myTime[50]{};
        getNowStr( myTime );

        /* Delete the old rows from R_QUOTA_USAGE */
        log_sql::debug("chlCalcUsageAndQuota SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        auto del_stmt = gq2::builder::remove_from("QUOTA_USAGE").build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

        /* Add a row to R_QUOTA_USAGE for each user's usage on each resource */
        log_sql::debug("chlCalcUsageAndQuota SQL 2");

        // Map (user_name, zone_name) -> user_id
        auto users_res = irods::experimental::catalog::execute_catalog(
            executor, db_conn,
            gq2::builder::select({"user_id", "user_name", "zone_name"}).from("USER").build());
        std::map<std::pair<std::string, std::string>, std::string> user_id_map;
        if (users_res.query_result) {
            while (users_res.query_result->next()) {
                auto uid = users_res.query_result->get<std::string>(0, "");
                auto uname = users_res.query_result->get<std::string>(1, "");
                auto zname = users_res.query_result->get<std::string>(2, "");
                user_id_map.emplace(std::make_pair(std::move(uname), std::move(zname)), std::move(uid));
            }
        }

        auto usage_sel = gq2::builder::select({"resc_id", "data_owner_name", "data_owner_zone"})
            .project(gq2::builder::sum("data_size"))
            .from("DATA_OBJECT")
            .group_by({"resc_id", "data_owner_name", "data_owner_zone"})
            .build();
        auto usage_res = irods::experimental::catalog::execute_catalog(executor, db_conn, usage_sel);
        if (usage_res.query_result) {
            while (usage_res.query_result->next()) {
                auto resc_id = usage_res.query_result->get<std::string>(0, "");
                auto owner_name = usage_res.query_result->get<std::string>(1, "");
                auto owner_zone = usage_res.query_result->get<std::string>(2, "");
                auto sum_size = usage_res.query_result->get<std::string>(3, "0");

                auto it = user_id_map.find({owner_name, owner_zone});
                if (it != user_id_map.end()) {
                    auto ins = gq2::builder::insert_into("QUOTA_USAGE")
                        .set("quota_usage", sum_size)
                        .set("resc_id", resc_id)
                        .set("user_id", it->second)
                        .set("modify_ts", myTime)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
                }
            }
        }

        trans.commit();
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __func__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }

    /* Set the over_quota flags where appropriate */
    int status = setOverQuota( _ctx.comm() );
    if ( status != 0 ) {
        return ERROR( status, "setOverQuota failed" );
    }

    return SUCCESS();
} // db_calc_usage_and_quota_op

irods::error db_set_quota_op(
    irods::plugin_context& _ctx,
    const char*            _type,
    const char*            _name,
    const char*            _resc_name,
    const char*            _limit ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    int status;
    rodsLong_t rescId = 0;
    rodsLong_t userId = 0;
    char userZone[NAME_LEN];
    char userName[NAME_LEN];
    char myTime[50];
    int itype = 0;

    if ( strncmp( _type, "user", 4 ) == 0 ) {
        itype = 1;
    }
    if ( strncmp( _type, "group", 5 ) == 0 ) {
        itype = 2;
    }
    if ( itype == 0 ) {
        return ERROR( CAT_INVALID_ARGUMENT, _type );
    }

    std::string zone;
    ret = getLocalZone( _ctx.prop_map(), &icss, zone );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    /* Get the resource id; use rescId=0 for 'total' */
    if ( strncmp( _resc_name, "total", 5 ) != 0 ) {
        log_sql::debug("chlSetQuota SQL 1");
        try {
            auto opt_resc = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"resc_id"})
                    .from("RESOURCE")
                    .where(col("resc_name") == _resc_name && col("zone_name") == zone)
                    .build());
            if (!opt_resc) {
                return ERROR( CAT_INVALID_RESOURCE, _resc_name );
            }
            rescId = *opt_resc;
        }
        catch (const std::exception& e) {
            log_db::error("{}: select resc_id failed: {}", __func__, e.what());
            return ERROR( CAT_SQL_ERR, "select resc_id failed" );
        }
    }

    status = validateAndParseUserName( _name, userName, userZone );
    if ( status ) {
        return ERROR( status, "Invalid username format" );
    }
    if ( userZone[0] == '\0' ) {
        snprintf( userZone, sizeof( userZone ), "%s", zone.c_str() );
    }

    if (!_limit) {
        const auto msg = "Invalid argument for quota limit. Received null input argument.";
        log_db::error(msg);
        return ERROR(SYS_INVALID_INPUT_PARAM, msg);
    }

    std::int64_t int_limit = -1;

    if (const auto [ptr, ec] = std::from_chars(_limit, _limit + std::strlen(_limit), int_limit); ec != std::errc{}) {
        const auto msg = fmt::format("Invalid argument for quota limit. Could not convert [{}] to an integer.", _limit);
        log_db::error(msg);
        return ERROR(SYS_INVALID_INPUT_PARAM, msg);
    }

    // Handling user quota.
    if ( itype == 1 ) {
        if (int_limit != 0) {
            const auto msg = fmt::format("Setting user quota limit to anything other than zero is not allowed. "
                                         "Received [{}].",
                                         int_limit);
            log_db::error(msg);
            return ERROR(SYS_NOT_ALLOWED, msg);
        }

        log_sql::debug("chlSetQuota SQL 2");
        try {
            auto opt_user = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == userName && col("zone_name") == userZone && col("user_type_name") != "rodsgroup")
                    .build());
            if (!opt_user) {
                return ERROR( CAT_INVALID_USER, userName );
            }
            userId = *opt_user;
        }
        catch (const std::exception& e) {
            log_db::error("{}: select user_id failed: {}", __func__, e.what());
            return ERROR( CAT_SQL_ERR, "select user_id failed" );
        }
    }
    // Handling group quota.
    else {
        log_sql::debug("chlSetQuota SQL 3");
        try {
            auto opt_group = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == userName && col("zone_name") == userZone && col("user_type_name") == "rodsgroup")
                    .build());
            if (!opt_group) {
                return ERROR( CAT_INVALID_GROUP, "invalid group" );
            }
            userId = *opt_group;
        }
        catch (const std::exception& e) {
            log_db::error("{}: select failure: {}", __func__, e.what());
            return ERROR( CAT_SQL_ERR, "select failure" );
        }
    }

    const auto userIdStr = std::to_string( userId );
    const auto rescIdStr = std::to_string( rescId );

    try {
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        log_sql::debug("chlSetQuota SQL 4");
        auto del_stmt = gq2::builder::remove_from("QUOTA")
            .where(col("user_id") == userIdStr && col("resc_id") == rescIdStr)
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

        if (int_limit > 0) {
            getNowStr( myTime );
            log_sql::debug("chlSetQuota SQL 5");
            auto ins_stmt = gq2::builder::insert_into("QUOTA")
                .set("user_id", userIdStr)
                .set("resc_id", rescIdStr)
                .set("quota_limit", _limit)
                .set("modify_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
        }

        trans.commit();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

    /* Reset the over_quota flags based on previous usage info. */
    status = setOverQuota( _ctx.comm() );
    if ( status != 0 ) {
        return ERROR( status, "setOverQuota failed" );
    }

    return SUCCESS();
} // db_set_quota_op

irods::error db_check_quota_op(
    irods::plugin_context& _ctx,
    const char*            _user_name,
    const char*            _resc_name,
    rodsLong_t*            _user_quota,
    int*                   _quota_status ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    *_user_quota = 0;
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        // 1. Get user_id for _user_name
        auto user_id_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _user_name)
                .build());
        if (!user_id_opt) {
            *_quota_status = QUOTA_UNRESTRICTED;
            return SUCCESS();
        }

        // 2. Get group_user_ids for groups the user belongs to
        auto group_ids = irods::experimental::catalog::query_catalog_strings(
            executor, db_conn,
            gq2::builder::select({"group_user_id"})
                .from("USER_GROUP")
                .where(col("user_id") == *user_id_opt)
                .build());

        std::vector<std::string> user_and_group_ids;
        user_and_group_ids.push_back(*user_id_opt);
        user_and_group_ids.insert(user_and_group_ids.end(), group_ids.begin(), group_ids.end());

        // 3. Get resc_id for _resc_name
        auto resc_id_opt = irods::experimental::catalog::query_catalog_string(
            executor, db_conn,
            gq2::builder::select({"resc_id"})
                .from("RESOURCE")
                .where(col("resc_name") == _resc_name)
                .build());

        // 4. Query R_QUOTA_MAIN for matches and find max quota_over
        bool found = false;
        int64_t max_quota_over = std::numeric_limits<int64_t>::min();
        std::string selected_resc_id;

        for (const auto& uid : user_and_group_ids) {
            auto cond = (resc_id_opt.has_value())
                ? (col("user_id") == uid && (col("resc_id") == *resc_id_opt || col("resc_id") == "0"))
                : (col("user_id") == uid && col("resc_id") == "0");

            auto q_res = irods::experimental::catalog::execute_catalog(
                executor, db_conn,
                gq2::builder::select({"resc_id", "quota_limit", "quota_over"})
                    .from("QUOTA")
                    .where(std::move(cond))
                    .build());

            if (q_res.query_result) {
                while (q_res.query_result->next()) {
                    found = true;
                    std::string r_id = q_res.query_result->get<std::string>(0);
                    int64_t q_over = q_res.query_result->get<int64_t>(2, 0);
                    if (q_over > max_quota_over) {
                        max_quota_over = q_over;
                        selected_resc_id = r_id;
                    }
                }
            }
        }

        if (!found) {
            log_db::info("chlCheckQuota - CAT_NO_ROWS_FOUND");
            *_quota_status = QUOTA_UNRESTRICTED;
            return SUCCESS();
        }

        log_db::info(
            "checkQuota: inUser:{} inResc:{} RescId:{} Quota:{}",
            _user_name,
            _resc_name,
            selected_resc_id,
            max_quota_over);

        *_user_quota = max_quota_over;
        if ( selected_resc_id == "0" ) {
            *_quota_status = QUOTA_GLOBAL;
        }
        else {
            *_quota_status = QUOTA_RESOURCE;
        }

        return SUCCESS();
    }
    catch (const std::exception& e) {
        log_db::error("{}: check quota query failed: {}", __func__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
} // db_check_quota_op

irods::error db_del_unused_avus_op(irods::plugin_context& _ctx)
{
    if (irods::error ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    if (!irods::is_privileged_client(*_ctx.comm())) {
        return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Insufficient privileges");
    }

    // Remove any AVUs that are currently not associated with any object.
    // This is done as a separate operation for efficiency.  See 'iadmin h rum'.
    const int remove_status = removeAVUs();
    if ( remove_status != 0 && remove_status != CAT_SUCCESS_BUT_WITH_NO_INFO ) {
        return ERROR( remove_status, "removeAVUs failed" );
    }

    return SUCCESS();
} // db_del_unused_avus_op

irods::error db_ins_rule_table_op(
    irods::plugin_context& _ctx,
    const char*            _base_name,
    const char*            _map_priority_str,
    const char*            _rule_name,
    const char*            _rule_head,
    const char*            _rule_condition,
    const char*            _rule_action,
    const char*            _rule_recovery,
    const char*            _rule_id_str,
    const char*            _my_time ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlInsRuleTable");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        std::string rule_id_str;
        log_sql::debug("chlInsRuleTable SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto rule_id_opt = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"rule_id"})
                .from("RULE")
                .where(col("rule_base_name") == (_base_name ? _base_name : "") &&
                       col("rule_name") == (_rule_name ? _rule_name : "") &&
                       col("rule_event") == (_rule_head ? _rule_head : "") &&
                       col("rule_condition") == (_rule_condition ? _rule_condition : "") &&
                       col("rule_body") == (_rule_action ? _rule_action : "") &&
                       col("rule_recovery") == (_rule_recovery ? _rule_recovery : ""))
                .build());

        if (!rule_id_opt.has_value()) {
            const int64_t seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
            rule_id_str = fmt::format("{}{}", _rule_id_str ? _rule_id_str : "", seqNum);

            log_sql::debug("chlInsRuleTable SQL 2");
            auto ins = gq2::builder::insert_into("RULE")
                .set("rule_id", rule_id_str)
                .set("rule_base_name", _base_name ? _base_name : "")
                .set("rule_name", _rule_name ? _rule_name : "")
                .set("rule_event", _rule_head ? _rule_head : "")
                .set("rule_condition", _rule_condition ? _rule_condition : "")
                .set("rule_body", _rule_action ? _rule_action : "")
                .set("rule_recovery", _rule_recovery ? _rule_recovery : "")
                .set("rule_owner_name", _ctx.comm()->clientUser.userName)
                .set("rule_owner_zone", _ctx.comm()->clientUser.rodsZone)
                .set("create_ts", _my_time ? _my_time : "")
                .set("modify_ts", _my_time ? _my_time : "")
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
        }
        else {
            rule_id_str = fmt::format("{}{}", _rule_id_str ? _rule_id_str : "", *rule_id_opt);
        }

        log_sql::debug("chlInsRuleTable SQL 3");
        auto ins_map = gq2::builder::insert_into("RULE_BASE_MAP")
            .set("map_base_name", _base_name ? _base_name : "")
            .set("map_priority", _map_priority_str ? _map_priority_str : "")
            .set("rule_id", rule_id_str)
            .set("map_owner_name", _ctx.comm()->clientUser.userName)
            .set("map_owner_zone", _ctx.comm()->clientUser.rodsZone)
            .set("create_ts", _my_time ? _my_time : "")
            .set("modify_ts", _my_time ? _my_time : "")
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_map);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_ins_rule_table_op

irods::error db_ins_dvm_table_op(
    irods::plugin_context& _ctx,
    const char*            _base_name,
    const char*            _var_name,
    const char*            _action,
    const char*            _var_2_cmap,
    const char*            _my_time ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlInsDvmTable");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        std::string dvmIdStr;
        log_sql::debug("chlInsDvmTable SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto dvm_id_opt = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"dvm_id"})
                .from("RULE_DVM")
                .where(col("dvm_base_name") == (_base_name ? _base_name : "") &&
                       col("dvm_ext_var_name") == (_var_name ? _var_name : "") &&
                       col("dvm_condition") == (_action ? _action : "") &&
                       col("dvm_int_map_path") == (_var_2_cmap ? _var_2_cmap : ""))
                .build());

        if (!dvm_id_opt.has_value()) {
            const int64_t seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
            dvmIdStr = std::to_string(seqNum);

            log_sql::debug("chlInsDvmTable SQL 2");
            auto ins = gq2::builder::insert_into("RULE_DVM")
                .set("dvm_id", dvmIdStr)
                .set("dvm_base_name", _base_name ? _base_name : "")
                .set("dvm_ext_var_name", _var_name ? _var_name : "")
                .set("dvm_condition", _action ? _action : "")
                .set("dvm_int_map_path", _var_2_cmap ? _var_2_cmap : "")
                .set("dvm_owner_name", _ctx.comm()->clientUser.userName)
                .set("dvm_owner_zone", _ctx.comm()->clientUser.rodsZone)
                .set("create_ts", _my_time ? _my_time : "")
                .set("modify_ts", _my_time ? _my_time : "")
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
        }
        else {
            dvmIdStr = std::to_string(*dvm_id_opt);
        }

        log_sql::debug("chlInsDvmTable SQL 3");
        auto ins_map = gq2::builder::insert_into("RULE_DVM_MAP")
            .set("map_dvm_base_name", _base_name ? _base_name : "")
            .set("dvm_id", dvmIdStr)
            .set("map_owner_name", _ctx.comm()->clientUser.userName)
            .set("map_owner_zone", _ctx.comm()->clientUser.rodsZone)
            .set("create_ts", _my_time ? _my_time : "")
            .set("modify_ts", _my_time ? _my_time : "")
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_map);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_ins_dvm_table_op

irods::error db_ins_fnm_table_op(
    irods::plugin_context& _ctx,
    const char*            _base_name,
    const char*            _func_name,
    const char*            _func_2_cmap,
    const char*            _my_time ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlInsFnmTable");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        std::string fnmIdStr;
        log_sql::debug("chlInsFnmTable SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto fnm_id_opt = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"fnm_id"})
                .from("RULE_FNM")
                .where(col("fnm_base_name") == (_base_name ? _base_name : "") &&
                       col("fnm_ext_func_name") == (_func_name ? _func_name : "") &&
                       col("fnm_int_func_name") == (_func_2_cmap ? _func_2_cmap : ""))
                .build());

        if (!fnm_id_opt.has_value()) {
            const int64_t seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
            fnmIdStr = std::to_string(seqNum);

            log_sql::debug("chlInsFnmTable SQL 2");
            namespace gq2 = irods::experimental::genquery2;
            auto ins = gq2::builder::insert_into("RULE_FNM")
                .set("fnm_id", fnmIdStr)
                .set("fnm_base_name", _base_name ? _base_name : "")
                .set("fnm_ext_func_name", _func_name ? _func_name : "")
                .set("fnm_int_func_name", _func_2_cmap ? _func_2_cmap : "")
                .set("fnm_owner_name", _ctx.comm()->clientUser.userName)
                .set("fnm_owner_zone", _ctx.comm()->clientUser.rodsZone)
                .set("create_ts", _my_time ? _my_time : "")
                .set("modify_ts", _my_time ? _my_time : "")
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
        }
        else {
            fnmIdStr = std::to_string(*fnm_id_opt);
        }

        log_sql::debug("chlInsFnmTable SQL 3");
        namespace gq2 = irods::experimental::genquery2;
        auto ins_map = gq2::builder::insert_into("RULE_FNM_MAP")
            .set("map_fnm_base_name", _base_name ? _base_name : "")
            .set("fnm_id", fnmIdStr)
            .set("map_owner_name", _ctx.comm()->clientUser.userName)
            .set("map_owner_zone", _ctx.comm()->clientUser.rodsZone)
            .set("create_ts", _my_time ? _my_time : "")
            .set("modify_ts", _my_time ? _my_time : "")
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_map);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_ins_fnm_table_op

irods::error db_ins_msrvc_table_op(
    irods::plugin_context& _ctx,
    const char*            _module_name,
    const char*            _msrvc_name,
    const char*            _msrvc_signature,
    const char*            _msrvc_version,
    const char*            _msrvc_host,
    const char*            _msrvc_location,
    const char*            _msrvc_language,
    const char*            _msrvc_type_name,
    const char*            _msrvc_status,
    const char*            _my_time ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlInsMsrvcTable");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        std::string msrvcIdStr;
        log_sql::debug("chlInsMsrvcTable SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto msrvc_id_opt = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"msrvc_id"})
                .from("MICROSERVICE")
                .where(col("msrvc_module_name") == (_module_name ? _module_name : "") &&
                       col("msrvc_name") == (_msrvc_name ? _msrvc_name : ""))
                .build());

        if (!msrvc_id_opt.has_value()) {
            const int64_t seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
            msrvcIdStr = std::to_string(seqNum);

            log_sql::debug("chlInsMsrvcTable SQL 2");
            auto ins = gq2::builder::insert_into("MICROSERVICE")
                .set("msrvc_id", msrvcIdStr)
                .set("msrvc_name", _msrvc_name ? _msrvc_name : "")
                .set("msrvc_module_name", _module_name ? _module_name : "")
                .set("msrvc_signature", _msrvc_signature ? _msrvc_signature : "")
                .set("msrvc_doxygen", "NONE")
                .set("msrvc_variations", "NONE")
                .set("msrvc_owner_name", _ctx.comm()->clientUser.userName)
                .set("msrvc_owner_zone", _ctx.comm()->clientUser.rodsZone)
                .set("create_ts", _my_time ? _my_time : "")
                .set("modify_ts", _my_time ? _my_time : "")
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);

            log_sql::debug("chlInsMsrvcTable SQL 3");
            auto ins_ver = gq2::builder::insert_into("MICROSERVICE_VER")
                .set("msrvc_id", msrvcIdStr)
                .set("msrvc_version", _msrvc_version ? _msrvc_version : "")
                .set("msrvc_host", _msrvc_host ? _msrvc_host : "")
                .set("msrvc_location", _msrvc_location ? _msrvc_location : "")
                .set("msrvc_language", _msrvc_language ? _msrvc_language : "")
                .set("msrvc_type_name", _msrvc_type_name ? _msrvc_type_name : "")
                .set("msrvc_status", _msrvc_status ? _msrvc_status : "")
                .set("msrvc_owner_name", _ctx.comm()->clientUser.userName)
                .set("msrvc_owner_zone", _ctx.comm()->clientUser.rodsZone)
                .set("create_ts", _my_time ? _my_time : "")
                .set("modify_ts", _my_time ? _my_time : "")
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_ver);
        }
        else {
            msrvcIdStr = std::to_string(*msrvc_id_opt);

            log_sql::debug("chlInsMsrvcTable SQL 4");
            const auto ver_check = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"msrvc_id"})
                    .from("MICROSERVICE_VER")
                    .where(col("msrvc_id") == msrvcIdStr &&
                           col("msrvc_host") == (_msrvc_host ? _msrvc_host : "") &&
                           col("msrvc_location") == (_msrvc_location ? _msrvc_location : ""))
                    .build());

            if (!ver_check.has_value()) {
                log_sql::debug("chlInsMsrvcTable SQL 5");
                namespace gq2 = irods::experimental::genquery2;
                auto ins_ver2 = gq2::builder::insert_into("MICROSERVICE_VER")
                    .set("msrvc_id", msrvcIdStr)
                    .set("msrvc_version", _msrvc_version ? _msrvc_version : "")
                    .set("msrvc_host", _msrvc_host ? _msrvc_host : "")
                    .set("msrvc_location", _msrvc_location ? _msrvc_location : "")
                    .set("msrvc_language", _msrvc_language ? _msrvc_language : "")
                    .set("msrvc_type_name", _msrvc_type_name ? _msrvc_type_name : "")
                    .set("msrvc_owner_name", _ctx.comm()->clientUser.userName)
                    .set("msrvc_owner_zone", _ctx.comm()->clientUser.rodsZone)
                    .set("create_ts", _my_time ? _my_time : "")
                    .set("modify_ts", _my_time ? _my_time : "")
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, ins_ver2);
            }
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_ins_msrvc_table_op

irods::error db_version_rule_base_op(
    irods::plugin_context& _ctx,
    const char*            _base_name,
    const char*            _my_time ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlVersionRuleBase");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlVersionRuleBase SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto upd = gq2::builder::update("RULE_BASE_MAP")
            .set("map_version", _my_time ? _my_time : "")
            .set("modify_ts", _my_time ? _my_time : "")
            .where(col("map_base_name") == (_base_name ? _base_name : "") && col("map_version") == "0")
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_version_rule_base_op

irods::error db_version_dvm_base_op(
    irods::plugin_context& _ctx,
    const char*            _base_name,
    const char*            _my_time ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlVersionDvmBase");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlVersionDvmBase SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto upd = gq2::builder::update("RULE_DVM_MAP")
            .set("map_dvm_version", _my_time ? _my_time : "")
            .set("modify_ts", _my_time ? _my_time : "")
            .where(col("map_dvm_base_name") == (_base_name ? _base_name : "") && col("map_dvm_version") == "0")
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_version_dvm_base_op

irods::error db_version_fnm_base_op(
    irods::plugin_context& _ctx,
    const char*            _base_name,
    const char*            _my_time ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlVersionFnmBase");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlVersionFnmBase SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto upd = gq2::builder::update("RULE_FNM_MAP")
            .set("map_fnm_version", _my_time ? _my_time : "")
            .set("modify_ts", _my_time ? _my_time : "")
            .where(col("map_fnm_base_name") == (_base_name ? _base_name : "") && col("map_fnm_version") == "0")
            .build();
        irods::experimental::catalog::execute_catalog(executor, db_conn, upd);

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_version_fnm_base_op

irods::error db_add_specific_query_op(
    irods::plugin_context& _ctx,
    const char*            _sql,
    const char*            _alias ) {
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlAddSpecificQuery");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    if ( strlen( _sql ) < 5 ) {
        return ERROR( CAT_INVALID_ARGUMENT, "sql string is invalid" );
    }

    char myTime[50];
    getNowStr( myTime );

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        if ( _alias != NULL && strlen( _alias ) > 0 ) {
            log_sql::debug("chlAddSpecificQuery SQL 1");
            namespace gq2 = irods::experimental::genquery2;
            using gq2::builder::col;

            const auto ts_opt = irods::experimental::catalog::query_catalog_string(
                executor,
                db_conn,
                gq2::builder::select({"create_ts"})
                    .from("SPECIFIC_QUERY")
                    .where(col("alias") == _alias)
                    .build());

            if ( ts_opt.has_value() ) {
                addRErrorMsg( &_ctx.comm()->rError, 0, "Alias is not unique" );
                return ERROR( CAT_INVALID_ARGUMENT, "alias is not unique" );
            }

            log_sql::debug("chlAddSpecificQuery SQL 2");
            namespace gq2 = irods::experimental::genquery2;
            auto ins = gq2::builder::insert_into("SPECIFIC_QUERY")
                .set("sqlStr", _sql)
                .set("alias", _alias)
                .set("create_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
        }
        else {
            log_sql::debug("chlAddSpecificQuery SQL 3");
            namespace gq2 = irods::experimental::genquery2;
            auto ins = gq2::builder::insert_into("SPECIFIC_QUERY")
                .set("sqlStr", _sql)
                .set("create_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_add_specific_query_op

irods::error db_del_specific_query_op(
    irods::plugin_context& _ctx,
    const char*            _sql_or_alias ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    log_sql::debug("chlDelSpecificQuery");

    if ( _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return ERROR( CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level" );
    }

    if ( !icss.status ) {
        return ERROR( CATALOG_NOT_CONNECTED, "catalog not connected" );
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        log_sql::debug("chlDelSpecificQuery SQL 1");
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        auto del_sql = gq2::builder::remove_from("SPECIFIC_QUERY")
            .where(col("sqlStr") == _sql_or_alias)
            .build();
        const auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, del_sql);

        if ( res.affected_rows == 0 ) {
            log_sql::debug("chlDelSpecificQuery SQL 2");
            auto del_alias = gq2::builder::remove_from("SPECIFIC_QUERY")
                .where(col("alias") == _sql_or_alias)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, del_alias);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_del_specific_query_op

#define MINIMUM_COL_SIZE 50
irods::error db_specific_query_op(
    irods::plugin_context& _ctx,
    specificQueryInp_t*    _spec_query_inp,
    genQueryOut_t*         _result ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_spec_query_inp
       ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );

    }

    int i, j, k;
    int needToGetNextRow;

    char combinedSQL[MAX_SQL_SIZE];

    int status, statementNum = UNINITIALIZED_STATEMENT_NUMBER;
    int numOfCols;
    int attriTextLen;
    int totalLen;
    int maxColSize;
    int currentMaxColSize;
    char *tResult, *tResult2;

    log_sql::debug("chlSpecificQuery");

    _result->attriCnt = 0;
    _result->rowCnt = 0;
    _result->totalRowCount = 0;

    currentMaxColSize = 0;

    if ( _spec_query_inp->continueInx == 0 ) {
        if ( _spec_query_inp->sql == NULL ) {
            return ERROR( CAT_INVALID_ARGUMENT, "null sql string" );
        }
        /*
          First check that this SQL is one of the allowed forms.
        */
        log_sql::debug("chlSpecificQuery SQL 1");
        try {
            auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
            auto opt_ts = query_catalog_string(
                executor, db_conn,
                gq2::builder::select({"create_ts"})
                    .from("SPECIFIC_QUERY")
                    .where(col("sqlStr") == _spec_query_inp->sql)
                    .build());
            if ( !opt_ts ) {
                log_sql::debug("chlSpecificQuery SQL 2");
                auto opt_sql = query_catalog_string(
                    executor, db_conn,
                    gq2::builder::select({"sqlStr"})
                        .from("SPECIFIC_QUERY")
                        .where(col("alias") == _spec_query_inp->sql)
                        .build());
                if ( !opt_sql ) {
                    return ERROR( CAT_UNKNOWN_SPECIFIC_QUERY, "unknown query" );
                }
                snprintf( combinedSQL, sizeof( combinedSQL ), "%s", opt_sql->c_str() );
            }
            else {
                snprintf( combinedSQL, sizeof( combinedSQL ), "%s", _spec_query_inp->sql );
            }
        }
        catch (const nanodbc::database_error& e) {
            log_db::error("{}: database error: {}", __func__, e.what());
            return ERROR(CAT_SQL_ERR, e.what());
        }
        catch (const std::exception& e) {
            log_db::error("{}: exception: {}", __func__, e.what());
            return ERROR(SYS_INTERNAL_ERR, e.what());
        }

        std::vector<std::string> bind_vars;
        int arg_idx = 0;
        while ( _spec_query_inp->args[arg_idx] != NULL && strlen( _spec_query_inp->args[arg_idx] ) > 0 ) {
            bind_vars.push_back( _spec_query_inp->args[arg_idx++] );
        }

        log_sql::debug("chlSpecificQuery SQL 3");
        status = db_get_first_row_from_sql( combinedSQL, &statementNum,
                                            _spec_query_inp->rowOffset, bind_vars, &icss );
        if ( status < 0 ) {
            if ( status != CAT_NO_ROWS_FOUND ) {
                log_db::info("chlSpecificQuery db_get_first_row_from_sql failure {}", status);
            }
            db_free_statement(statementNum, &icss);
            return ERROR( status, "db_get_first_row_from_sql failure" );
        }

        _result->continueInx = statementNum + 1;
        needToGetNextRow = 0;
    }
    else {
        statementNum = _spec_query_inp->continueInx - 1;
        needToGetNextRow = 1;
        if ( _spec_query_inp->maxRows <= 0 ) { /* caller is closing out the query */
            status = db_free_statement( statementNum, &icss );
            if ( status < 0 ) {
                return ERROR( status, "failed in free statement" );
            }
            else {
                return CODE( status );
            }
        }
    }
    for ( i = 0; i < _spec_query_inp->maxRows; i++ ) {
        if ( needToGetNextRow ) {
            status = db_get_next_row_from_statement( statementNum, &icss );
            if ( status == CAT_NO_ROWS_FOUND ) {
                db_free_statement( statementNum, &icss );
                _result->continueInx = 0;
                if ( _result->rowCnt == 0 ) {
                    return ERROR( status, "no rows found" );
                } /* NO ROWS; in this
                                                          case a continuation call is finding no more rows */
                return SUCCESS();
            }
            if ( status < 0 ) {
                db_free_statement(statementNum, &icss);
                return ERROR( status, "failed to get next row" );
            }
        }
        needToGetNextRow = 1;

        _result->rowCnt++;
        numOfCols = icss.stmtPtr[statementNum]->numOfCols;
        _result->attriCnt = numOfCols;
        _result->continueInx = statementNum + 1;

        maxColSize = 0;

        for ( k = 0; k < numOfCols; k++ ) {
            j = strlen( icss.stmtPtr[statementNum]->resultValue[k] );
            if ( maxColSize <= j ) {
                maxColSize = j;
            }
        }
        maxColSize++; /* for the null termination */
        if ( maxColSize < MINIMUM_COL_SIZE ) {
            maxColSize = MINIMUM_COL_SIZE; /* make it a reasonable size */
        }

        if ( i == 0 ) { /* first time thru, allocate and initialize */
            attriTextLen = numOfCols * maxColSize;
            totalLen = attriTextLen * _spec_query_inp->maxRows;
            for ( j = 0; j < numOfCols; j++ ) {
                tResult = ( char * ) malloc( totalLen );
                if ( tResult == NULL ) {
                    db_free_statement(statementNum, &icss);
                    return ERROR( SYS_MALLOC_ERR, "malloc error" );
                }
                memset( tResult, 0, totalLen );
                _result->sqlResult[j].attriInx = 0;
                /* In Gen-query this would be set to _spec_query_inp->selectInp.inx[j]; */

                _result->sqlResult[j].len = maxColSize;
                _result->sqlResult[j].value = tResult;
            }
            currentMaxColSize = maxColSize;
        }


        /* Check to see if the current row has a max column size that
           is larger than what we've been using so far.  If so, allocate
           new result strings, copy each row value over, and free the
           old one. */
        if ( maxColSize > currentMaxColSize ) {
            maxColSize += MINIMUM_COL_SIZE; /* bump it up to try to avoid
                                               some multiple resizes */
            attriTextLen = numOfCols * maxColSize;
            totalLen = attriTextLen * _spec_query_inp->maxRows;
            for ( j = 0; j < numOfCols; j++ ) {
                char *cp1, *cp2;
                int k;
                tResult = ( char * ) malloc( totalLen );
                if ( tResult == NULL ) {
                    db_free_statement(statementNum, &icss);
                    return ERROR( SYS_MALLOC_ERR, "failed to allocate result" );
                }
                memset( tResult, 0, totalLen );
                cp1 = _result->sqlResult[j].value;
                cp2 = tResult;
                for ( k = 0; k < _result->rowCnt; k++ ) {
                    strncpy( cp2, cp1, _result->sqlResult[j].len );
                    cp1 += _result->sqlResult[j].len;
                    cp2 += maxColSize;
                }
                free( _result->sqlResult[j].value );
                _result->sqlResult[j].len = maxColSize;
                _result->sqlResult[j].value = tResult;
            }
            currentMaxColSize = maxColSize;
        }

        /* Store the current row values into the appropriate spots in
           the attribute string */
        for ( j = 0; j < numOfCols; j++ ) {
            tResult2 = _result->sqlResult[j].value; /* ptr to value str */
            tResult2 += currentMaxColSize * ( _result->rowCnt - 1 );  /* skip forward
                                                                  for this row */
            strncpy( tResult2, icss.stmtPtr[statementNum]->resultValue[j],
                     currentMaxColSize ); /* copy in the value text */
        }

    }

    _result->continueInx = statementNum + 1;  /* the statementnumber but
                                            always >0 */
    return SUCCESS();

} // db_specific_query_op

irods::error db_get_distinct_data_obj_count_on_resource_op(
    irods::plugin_context& _ctx,
    const char*            _resc_name,
    long long*             _count ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check incoming pointers
    if ( !_resc_name ||
            !_count ) {
        return ERROR(
                   SYS_INVALID_INPUT_PARAM,
                   "null input param" );
    }

    // =-=-=-=-=-=-=-
    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        const std::string pattern1 = std::string(_resc_name) + ";%";
        const std::string pattern2 = std::string("%;") + _resc_name + ";%";
        const std::string pattern3 = std::string("%;") + _resc_name;

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto stmt = gq2::builder::select({gq2::builder::count("distinct data_id")})
            .from("DATA_OBJECT")
            .where(col("resc_hier").like(pattern1) || col("resc_hier").like(pattern2) || col("resc_hier").like(pattern3))
            .build();

        auto opt_cnt = irods::experimental::catalog::query_catalog_integer(executor, db_conn, stmt);
        if ( !opt_cnt ) {
            return ERROR( CAT_NO_ROWS_FOUND, "query failed" );
        }

        ( *_count ) = *opt_cnt;
        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_get_distinct_data_obj_count_on_resource_op

irods::error db_get_distinct_data_objs_missing_from_child_given_parent_op(
    irods::plugin_context& _ctx,
    const std::string*     _parent,
    const std::string*     _child,
    int                    _limit,
    dist_child_result_t*   _results ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check incoming pointers
    if ( !_parent    ||
            !_child     ||
            _limit <= 0 ||
            !_results ) {
        return ERROR(
                   SYS_INVALID_INPUT_PARAM,
                   "null or invalid input param" );
    }

    // =-=-=-=-=-=-=-
    // the basic query string
    const auto& flavor = irods::experimental::catalog::get_db_flavor(icss.databaseType);
    const std::string query = fmt::format(
        fmt::runtime(flavor.get_hier_resc_vault_template),
        fmt::format("{};%", *_parent),
        fmt::format("%;{};%", *_parent),
        fmt::format("%;{}", *_parent),
        fmt::format("{};%", *_child),
        fmt::format("%;{};%", *_child),
        fmt::format("%;{}", *_child),
        _limit);

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        auto rows = executor.execute_query(db_conn, query);
        while (rows.next()) {
            _results->push_back(rows.get<int>(0));
        }

        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

    return SUCCESS();

} // db_get_distinct_data_objs_missing_from_child_given_parent_op

irods::error db_get_repl_list_for_leaf_bundles_op(
    irods::plugin_context&      _ctx,
    rodsLong_t                  _count,
    size_t                      _child_index,
    const std::vector<leaf_bundle_t>* _bundles,
    const std::string*          _invocation_timestamp,
    dist_child_result_t*        _results ) {

    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if (_count <= 0) {
        return ERROR(SYS_INVALID_INPUT_PARAM, boost::format("invalid _count [%d]") % _count);
    }
    if (_bundles->empty()) {
        return ERROR(SYS_INVALID_INPUT_PARAM, "no bundles");
    }
    if (!_invocation_timestamp) {
        return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "invocation timestamp is NULL");
    }
    if (_invocation_timestamp->empty()) {
        return ERROR(SYS_INVALID_INPUT_PARAM, "invocation timestamp is empty");
    }

    // capture list of child resc ids
    std::stringstream child_array_stream;
    for( auto id : (*_bundles)[_child_index] ) {
        child_array_stream << id << ",";
    }
    std::string child_array = child_array_stream.str();
    if (child_array.empty()) {
        return ERROR(SYS_INVALID_INPUT_PARAM, "leaf array is empty");
    }
    child_array.pop_back(); // trim last ','

    std::stringstream not_child_stream;
    for( size_t idx = 0; idx < _bundles->size(); ++idx ) {
        if( idx == _child_index ) {
            continue;
        }
        for( auto id : (*_bundles)[idx] ) {
            not_child_stream << id << ",";
        }
    } // for idx

    std::string not_child_array = not_child_stream.str();
    if (not_child_array.empty()) {
        return SUCCESS();
    }
    not_child_array.pop_back(); // trim last ','

    const auto& flavor = irods::experimental::catalog::get_db_flavor(icss.databaseType);
    const std::string query = fmt::format(
        fmt::runtime(flavor.get_repl_list_leaf_bundles_template),
        not_child_array,
        *_invocation_timestamp,
        child_array,
        _count);

    _results->reserve(_count);

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        auto rows = executor.execute_query(db_conn, query);
        while (rows.next()) {
            _results->push_back(rows.get<rodsLong_t>(0));
        }

        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }

} // db_get_repl_list_for_leaf_bundles_op

irods::error db_get_hierarchy_for_resc_op(
    irods::plugin_context& _ctx,
    const std::string*     _resc_name,
    const std::string*     _zone_name,
    std::string*           _hierarchy ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check incoming pointers
    if ( !_resc_name    ||
         !_zone_name    ||
         !_hierarchy ) {
        return ERROR(
                   SYS_INVALID_INPUT_PARAM,
                   "null or invalid input param" );
    }

    ( *_hierarchy ) = ( *_resc_name ); // Initialize hierarchy string with resource

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        std::string current_node = *_resc_name;
        while ( !current_node.empty() ) {
            auto q_res = irods::experimental::catalog::execute_catalog(
                executor, db_conn,
                gq2::builder::select({"resc_parent"})
                    .from("RESOURCE")
                    .where(col("resc_name") == current_node && col("zone_name") == *_zone_name)
                    .build());

            if ( !q_res.query_result || !q_res.query_result->next() ) {
                // Check if the resource actually exists
                auto type_res = irods::experimental::catalog::execute_catalog(
                    executor, db_conn,
                    gq2::builder::select({"resc_type_name"})
                        .from("RESOURCE")
                        .where(col("resc_name") == current_node && col("zone_name") == *_zone_name)
                        .build());
                if ( !type_res.query_result || !type_res.query_result->next() ) {
                    return ERROR( CAT_UNKNOWN_RESOURCE, "resource does not exist" );
                }
                *_hierarchy = "";
                return SUCCESS();
            }

            if ( q_res.query_result->is_null(0) ) {
                current_node.clear();
            }
            else {
                const auto parent = q_res.query_result->get<std::string>(0);
                if ( !parent.empty() ) {
                    ( *_hierarchy ) = parent + irods::hierarchy_parser::delimiter() + ( *_hierarchy );
                    current_node = parent;
                }
                else {
                    current_node.clear();
                }
            }
        }

        return SUCCESS();
    }
    catch (const nanodbc::database_error& e) {
        log_db::error("{}: database error: {}", __FUNCTION__, e.what());
        return ERROR( CAT_SQL_ERR, e.what() );
    }
    catch (const std::exception& e) {
        log_db::error("{}: exception: {}", __FUNCTION__, e.what());
        return ERROR( SYS_INTERNAL_ERR, e.what() );
    }
} // db_get_hierarchy_for_resc_op

namespace
{
    // NOLINTNEXTLINE(readability-function-cognitive-complexity)
    irods::error execute_ticket_operation_as_admin(irods::plugin_context& _ctx,
                                                   const char* _op_name,
                                                   const char* _ticket_string,
                                                   const char* _arg3,
                                                   const char* _arg4,
                                                   const char* _arg5,
                                                   const KeyValPair* _cond_input)
    {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        //
        // Get the ticket's id.
        //

        int64_t ticket_id = 0;
        try {
            namespace gq2 = irods::experimental::genquery2;
            using gq2::builder::col;

            auto opt_id = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"ticket_id"})
                    .from("TICKET")
                    .where(col("ticket_string") == _ticket_string)
                    .build());
            if (opt_id) {
                ticket_id = *opt_id;
            }
            else {
                opt_id = irods::experimental::catalog::query_catalog_integer(
                    executor, db_conn,
                    gq2::builder::select({"ticket_id"})
                        .from("TICKET")
                        .where(col("ticket_id") == _ticket_string)
                        .build());
                if (opt_id) {
                    ticket_id = *opt_id;
                }
                else {
                    return ERROR(CAT_TICKET_INVALID, _ticket_string);
                }
            }
        }
        catch (const std::exception&) {
            return ERROR(CAT_TICKET_INVALID, _ticket_string);
        }

        const auto ticket_id_string = std::to_string(ticket_id);

        //
        // Handle the operation.
        //

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        // Delete ticket operation.
        if (std::strcmp(_op_name, "delete") == 0) {
            try {
                nanodbc::transaction trans{db_conn};
                auto del_ticket = gq2::builder::remove_from("R_TICKET_MAIN")
                    .where(col("ticket_id") == ticket_id_string)
                    .build();
                auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, del_ticket);
                if (res.affected_rows == 0) {
                    return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                }

                // Delete all relationships stored in the secondary ticket tables.
                try {
                    auto del_hosts = gq2::builder::remove_from("R_TICKET_ALLOWED_HOSTS")
                        .where(col("ticket_id") == ticket_id_string)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, del_hosts);
                }
                catch (const std::exception& e) {
                    log_db::warn("Failed to delete ticket information [error={}, ticket={}, table=R_TICKET_ALLOWED_HOSTS]",
                        e.what(), ticket_id_string);
                }

                try {
                    auto del_users = gq2::builder::remove_from("R_TICKET_ALLOWED_USERS")
                        .where(col("ticket_id") == ticket_id_string)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, del_users);
                }
                catch (const std::exception& e) {
                    log_db::warn("Failed to delete ticket information [error={}, ticket={}, table=R_TICKET_ALLOWED_USERS]",
                        e.what(), ticket_id_string);
                }

                try {
                    auto del_groups = gq2::builder::remove_from("R_TICKET_ALLOWED_GROUPS")
                        .where(col("ticket_id") == ticket_id_string)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, del_groups);
                }
                catch (const std::exception& e) {
                    log_db::warn("Failed to delete ticket information [error={}, ticket={}, table=R_TICKET_ALLOWED_GROUPS]",
                        e.what(), ticket_id_string);
                }

                trans.commit();
                return SUCCESS();
            }
            catch (const std::exception& e) {
                log_db::error("{}: Failed to delete ticket: {}", __func__, e.what());
                return ERROR(CAT_SQL_ERR, fmt::format("Failed to delete ticket with id [{}] as user [{}].",
                    ticket_id_string, _ctx.comm()->clientUser.userName));
            }
        } // delete ticket operation

        // Modify ticket operation.
        if (std::strcmp(_op_name, "mod") == 0) {
            if (std::strcmp(_arg3, "uses") == 0) {
                char myTime[TIME_LEN]{};
                getNowStr(myTime);
                try {
                    nanodbc::transaction trans{db_conn};
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("uses_limit", _arg4)
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticket_id_string)
                        .build();
                    auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                    if (res.affected_rows == 0) {
                        return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                    }
                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    log_db::error("{}: Failed to update uses: {}", __func__, e.what());
                    return ERROR(CAT_SQL_ERR, "SQL execution error.");
                }
            } // uses

            if (std::strcmp(_arg3, "write-file") == 0) {
                char myTime[TIME_LEN]{};
                getNowStr(myTime);
                try {
                    nanodbc::transaction trans{db_conn};
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("write_file_limit", _arg4)
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticket_id_string)
                        .build();
                    auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                    if (res.affected_rows == 0) {
                        return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                    }
                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    log_db::error("{}: Failed to update write-file: {}", __func__, e.what());
                    return ERROR(CAT_SQL_ERR, "SQL execution error.");
                }
            } // write-file

            if (std::strcmp(_arg3, "write-bytes") == 0) {
                char myTime[TIME_LEN]{};
                getNowStr(myTime);
                try {
                    nanodbc::transaction trans{db_conn};
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("write_byte_limit", _arg4)
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticket_id_string)
                        .build();
                    auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                    if (res.affected_rows == 0) {
                        return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                    }
                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    log_db::error("{}: Failed to update write-bytes: {}", __func__, e.what());
                    return ERROR(CAT_SQL_ERR, "SQL execution error.");
                }
            } // write-bytes

            if (std::strcmp(_arg3, "expire") == 0) {
                std::string ticket_expiration_string;

                // Empty strings and zero (i.e. 0) are special in that they instruct the system
                // to clear the expiration timestamp.
                //
                // Prior versions of iRODS would result in setting the expiration timestamp
                // to the value passed, but it would be better to consolidate these values into
                // one outcome. Admins should not rely on the value in the database directly.
                // Admins should use the values returned by the APIs and tools.
                //
                // For this reason, if the server receives a zero or empty string, we don't have
                // to modify "ticket_expiration_string" because it is already an empty string.
                if (std::strcmp(_arg4, "") != 0 && std::strcmp(_arg4, "0") != 0) {
                    try {
                        // Try to parse the timestamp argument as seconds since epoch.
                        const auto seconds_since_epoch = boost::lexical_cast<std::int64_t>(_arg4);
                        ticket_expiration_string = fmt::format("{:011}", seconds_since_epoch);
                    }
                    catch (const boost::bad_lexical_cast&) {
                        //
                        // If an exception was thrown, the timestamp argument was not something that
                        // represented seconds since epoch. For that reason, the client may have passed
                        // an actual timestamp, which we attempt to process here.
                        //

                        std::istringstream ss{_arg4};

                        // The facet allocated via the "new" operator is managed by the std::locale.
                        // The use of "new" here is correct and there are no memory leaks caused by this line.
                        ss.imbue(std::locale(ss.getloc(), new boost::posix_time::time_input_facet{"%Y-%m-%d.%H:%M:%S"}));

                        boost::posix_time::ptime t;
                        if (!(ss >> t)) {
                            return ERROR(SYS_INTERNAL_ERR, "Could not parse timestamp string into appropriate object.");
                        }

                        ticket_expiration_string = fmt::format("{:011}", to_time_t(t));
                    }
                }

                char myTime[TIME_LEN]{};
                getNowStr(myTime);
                try {
                    nanodbc::transaction trans{db_conn};
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("ticket_expiry_ts", ticket_expiration_string)
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticket_id_string)
                        .build();
                    auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                    if (res.affected_rows == 0) {
                        return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                    }
                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    log_db::error("{}: Failed to update expire: {}", __func__, e.what());
                    return ERROR(CAT_SQL_ERR, "SQL execution error.");
                }
            } // expire

            if (std::strcmp(_arg3, "add") == 0) {
                if (std::strcmp(_arg4, "host") == 0) {
                    char* hostIp = convertHostToIp(_arg5);
                    if (!hostIp) {
                        return ERROR(CAT_HOSTNAME_INVALID, _arg5);
                    }

                    try {
                        nanodbc::transaction trans{db_conn};
                        auto ins_stmt = gq2::builder::insert_into("TICKET_ALLOWED_HOSTS")
                            .set("ticket_id", ticket_id_string)
                            .set("host", hostIp)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);

                        char myTime[TIME_LEN]{};
                        getNowStr(myTime);
                        auto upd_stmt = gq2::builder::update("TICKET")
                            .set("modify_ts", myTime)
                            .where(col("ticket_id") == ticket_id_string)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                        trans.commit();
                        return SUCCESS();
                    }
                    catch (const std::exception& e) {
                        auto err_msg = fmt::format(
                            "Failed to add host [{}] to ticket [{}]: {}", hostIp, ticket_id_string, e.what());
                        log_sql::error(err_msg);
                        return ERROR(CAT_SQL_ERR, std::move(err_msg));
                    }
                } // host

                if (std::strcmp(_arg4, "user") == 0) {
                    char user_id_string[MAX_NAME_LEN];
                    auto ec = icatGetTicketUserId(_ctx.prop_map(), _arg5, user_id_string);
                    if (0 != ec) {
                        return ERROR(ec, "icatGetTicketUserId failed");
                    }

                    try {
                        nanodbc::transaction trans{db_conn};
                        auto ins_stmt = gq2::builder::insert_into("TICKET_ALLOWED_USERS")
                            .set("ticket_id", ticket_id_string)
                            .set("user_name", _arg5)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);

                        char myTime[TIME_LEN]{};
                        getNowStr(myTime);
                        auto upd_stmt = gq2::builder::update("TICKET")
                            .set("modify_ts", myTime)
                            .where(col("ticket_id") == ticket_id_string)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                        trans.commit();
                        return SUCCESS();
                    }
                    catch (const std::exception& e) {
                        auto err_msg = fmt::format(
                            "Failed to add user [{}] to ticket [{}]: {}", _arg5, ticket_id_string, e.what());
                        log_sql::error(err_msg);
                        return ERROR(CAT_SQL_ERR, std::move(err_msg));
                    }
                } // user

                if (std::strcmp(_arg4, "group") == 0) {
                    char user_id_string[MAX_NAME_LEN];
                    auto ec = icatGetTicketGroupId(_ctx.prop_map(), _arg5, user_id_string);
                    if (0 != ec) {
                        return ERROR(ec, "icatGetTicketGroupId failed");
                    }

                    try {
                        nanodbc::transaction trans{db_conn};
                        auto ins_stmt = gq2::builder::insert_into("TICKET_ALLOWED_GROUPS")
                            .set("ticket_id", ticket_id_string)
                            .set("group_name", _arg5)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);

                        char myTime[TIME_LEN]{};
                        getNowStr(myTime);
                        auto upd_stmt = gq2::builder::update("TICKET")
                            .set("modify_ts", myTime)
                            .where(col("ticket_id") == ticket_id_string)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                        trans.commit();
                        return SUCCESS();
                    }
                    catch (const std::exception& e) {
                        auto err_msg = fmt::format(
                            "Failed to add group [{}] to ticket [{}]: {}", _arg5, ticket_id_string, e.what());
                        log_sql::error(err_msg);
                        return ERROR(CAT_SQL_ERR, std::move(err_msg));
                    }
                } // group
            } // add
            else if (std::strcmp(_arg3, "remove") == 0) {
                if (std::strcmp(_arg4, "host") == 0) {
                    char* hostIp = convertHostToIp(_arg5);
                    if (!hostIp) {
                        return ERROR(CAT_HOSTNAME_INVALID, "host name null");
                    }

                    try {
                        nanodbc::transaction trans{db_conn};
                        auto del_stmt = gq2::builder::remove_from("TICKET_ALLOWED_HOSTS")
                            .where(col("ticket_id") == ticket_id_string && col("host") == hostIp)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

                        char myTime[TIME_LEN]{};
                        getNowStr(myTime);
                        auto upd_stmt = gq2::builder::update("TICKET")
                            .set("modify_ts", myTime)
                            .where(col("ticket_id") == ticket_id_string)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                        trans.commit();
                        return SUCCESS();
                    }
                    catch (const std::exception& e) {
                        auto err_msg = fmt::format(
                            "Failed to remove host [{}] from ticket [{}]: {}", hostIp, ticket_id_string, e.what());
                        log_sql::error(err_msg);
                        return ERROR(CAT_SQL_ERR, std::move(err_msg));
                    }
                } // host

                if (std::strcmp(_arg4, "user") == 0) {
                    char user_id_string[MAX_NAME_LEN];
                    auto ec = icatGetTicketUserId(_ctx.prop_map(), _arg5, user_id_string);
                    if (0 != ec) {
                        return ERROR(ec, "icatGetTicketUserId failed");
                    }

                    try {
                        nanodbc::transaction trans{db_conn};
                        auto del_stmt = gq2::builder::remove_from("TICKET_ALLOWED_USERS")
                            .where(col("ticket_id") == ticket_id_string && col("user_name") == _arg5)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

                        char myTime[TIME_LEN]{};
                        getNowStr(myTime);
                        auto upd_stmt = gq2::builder::update("TICKET")
                            .set("modify_ts", myTime)
                            .where(col("ticket_id") == ticket_id_string)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                        trans.commit();
                        return SUCCESS();
                    }
                    catch (const std::exception& e) {
                        auto err_msg = fmt::format(
                            "Failed to remove user [{}] from ticket [{}]: {}", _arg5, ticket_id_string, e.what());
                        log_sql::error(err_msg);
                        return ERROR(CAT_SQL_ERR, std::move(err_msg));
                    }
                } // user

                if (std::strcmp(_arg4, "group") == 0) {
                    char group_id_string[MAX_NAME_LEN];
                    auto ec = icatGetTicketGroupId(_ctx.prop_map(), _arg5, group_id_string);
                    if (0 != ec) {
                        return ERROR(ec, "icatGetTicketGroupId failed");
                    }

                    try {
                        nanodbc::transaction trans{db_conn};
                        auto del_stmt = gq2::builder::remove_from("TICKET_ALLOWED_GROUPS")
                            .where(col("ticket_id") == ticket_id_string && col("group_name") == _arg5)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

                        char myTime[TIME_LEN]{};
                        getNowStr(myTime);
                        auto upd_stmt = gq2::builder::update("TICKET")
                            .set("modify_ts", myTime)
                            .where(col("ticket_id") == ticket_id_string)
                            .build();
                        irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                        trans.commit();
                        return SUCCESS();
                    }
                    catch (const std::exception& e) {
                        auto err_msg = fmt::format(
                            "Failed to remove group [{}] from ticket [{}]: {}", _arg5, ticket_id_string, e.what());
                        log_sql::error(err_msg);
                        return ERROR(CAT_SQL_ERR, std::move(err_msg));
                    }
                } // group
            } // remove
        } // modify ticket operation

        return ERROR(CAT_INVALID_ARGUMENT, "invalid op name");
    } // execute_ticket_operation_as_admin
} // anonymous namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
irods::error db_mod_ticket_op(
    irods::plugin_context& _ctx,
    const char*            _op_name,
    const char*            _ticket_string,
    const char*            _arg3,
    const char*            _arg4,
    const char*            _arg5,
    const KeyValPair*      _cond_input)
{
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    rodsLong_t status, status2, status3;
    char logicalEndName[MAX_NAME_LEN];
    char logicalParentDirName[MAX_NAME_LEN];
    rodsLong_t objId = 0;
    rodsLong_t userId;
    rodsLong_t ticketId;
    rodsLong_t seqNum;
    char objTypeStr[NAME_LEN];
    std::string userIdStr;
    char user2IdStr[MAX_NAME_LEN];
    std::string ticketIdStr;

    /* session ticket */
    if ( strcmp( _op_name, "session" ) == 0 ) {
        if ( strlen( _arg3 ) > 0 ) {
            /* for 2 server hops, arg3 is the original client addr */
            if (const auto ec = chlGenQueryTicketSetup(_ticket_string, _arg3); ec < 0) {
                return ERROR(ec, "failed in chlGenQueryTicketSetup");
            }
            snprintf( mySessionTicket, sizeof( mySessionTicket ), "%s", _ticket_string );
            snprintf( mySessionClientAddr, sizeof( mySessionClientAddr ), "%s", _arg3 );
        }
        else {
            /* for direct connections, rsComm has the original client addr */
            if (const auto ec = chlGenQueryTicketSetup(_ticket_string, _ctx.comm()->clientAddr); ec < 0) {
                return ERROR(ec, "failed in chlGenQueryTicketSetup");
            }
            snprintf( mySessionTicket, sizeof( mySessionTicket ), "%s", _ticket_string );
            snprintf( mySessionClientAddr, sizeof( mySessionClientAddr ), "%s", _ctx.comm()->clientAddr );
        }

        return SUCCESS();
    }

    // Handle operations that understand the admin keyword (i.e. delete and mod).
    if (getValByKey(_cond_input, ADMIN_KW)) {
        if (!irods::is_privileged_client(*_ctx.comm())) {
            const auto msg = fmt::format("User [{}] is not a rodsadmin.", _ctx.comm()->clientUser.userName);
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, msg);
        }

        const auto ops = {"delete", "mod"};
        const auto pred = [&_op_name](const std::string_view _s) { return _s == _op_name; };

        if (std::any_of(std::begin(ops), std::end(ops), pred)) {
            return execute_ticket_operation_as_admin(_ctx, _op_name, _ticket_string, _arg3, _arg4, _arg5, _cond_input);
        }

        // The admin keyword is ignored for other operations.
    }

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

    // create
    if ( strcmp( _op_name, "create" ) == 0 ) {
        if (isInteger(const_cast<char*>(_ticket_string))) {
            log_db::info("chlModTicket create ticket, string cannot be a number [{}]", _ticket_string);
            return ERROR( CAT_TICKET_INVALID, "ticket string cannot be a number" );
        }

        if (const auto ec = splitPathByKey(_arg4, logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/'); ec < 0) {
            return ERROR(ec, fmt::format(
                         "[{}:{}] - failed in splitPathByKey [path=[{}], ec=[{}]]",
                         __func__, __LINE__, _arg4, ec));
        }

        if ( strlen( logicalParentDirName ) == 0 ) {
            snprintf( logicalParentDirName, sizeof( logicalParentDirName ), "%s", PATH_SEPARATOR );
            snprintf( logicalEndName, sizeof( logicalEndName ), "%s", _arg4 + 1 );
        }

        status2 = irods::experimental::catalog::access_control::check_data_object_only(
            executor, db_conn,
            logicalParentDirName, logicalEndName,
            _ctx.comm()->clientUser.userName,
            _ctx.comm()->clientUser.rodsZone,
            ACCESS_OWN );
        if ( status2 > 0 ) {
            snprintf( objTypeStr, sizeof( objTypeStr ), "%s", TICKET_TYPE_DATA );
            objId = status2;
        }
        else {
            status3 = irods::experimental::catalog::access_control::check_collection_access(
                executor, db_conn,
                _arg4,
                _ctx.comm()->clientUser.userName,
                _ctx.comm()->clientUser.rodsZone,
                ACCESS_OWN );
            if ( status3 == CAT_NO_ROWS_FOUND && status2 == CAT_NO_ROWS_FOUND ) {
                return ERROR( CAT_UNKNOWN_COLLECTION, _arg4 );
            }
            if ( status3 < 0 ) {
                return ERROR( status3, "check_collection_access failed" );
            }
            snprintf( objTypeStr, sizeof( objTypeStr ), "%s", TICKET_TYPE_COLL );
            objId = status3;
        }

        log_sql::debug("chlModTicket SQL 1");
        try {
            auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
                executor,
                db_conn,
                gq2::builder::select({"user_id"})
                    .from("USER")
                    .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                    .build());
            if (!opt_user_id) {
                return ERROR( CAT_INVALID_USER, "select user_id failed" );
            }
            userId = *opt_user_id;

            seqNum = executor.get_next_sequence_value(db_conn, "R_ObjectID");
            if ( seqNum < 0 ) {
                log_db::info("chlModTicket failure {}d", seqNum);
                return ERROR( seqNum, "get_next_sequence_value failed" );
            }
            const char* ticketType = ( strncmp( _arg3, "write", 5 ) == 0 ) ? "write" : "read";

            char myTime[TIME_LEN]{};
            getNowStr( myTime );
            log_sql::debug("chlModTicket SQL 2");
            nanodbc::transaction trans{db_conn};
            auto ins_stmt = gq2::builder::insert_into("TICKET")
                .set("ticket_id", std::to_string(seqNum))
                .set("ticket_string", _ticket_string)
                .set("ticket_type", ticketType)
                .set("user_id", std::to_string(userId))
                .set("object_id", std::to_string(objId))
                .set("object_type", objTypeStr)
                .set("modify_ts", myTime)
                .set("create_ts", myTime)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);
            trans.commit();
            return SUCCESS();
        }
        catch (const std::exception& e) {
            log_db::error("{}: chlModTicket insert failure: {}", __func__, e.what());
            return ERROR( CAT_SQL_ERR, "insert failure" );
        }
    } // create operation

    log_sql::debug("chlModTicket SQL 3");

    // Get user id of user matching (user name, zone name).
    try {
        auto opt_user_id = irods::experimental::catalog::query_catalog_integer(
            executor,
            db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _ctx.comm()->clientUser.userName && col("zone_name") == _ctx.comm()->clientUser.rodsZone)
                .build());
        if (!opt_user_id) {
            addRErrorMsg( &_ctx.comm()->rError, 0, "Invalid user" );
            return ERROR( CAT_INVALID_USER, _ctx.comm()->clientUser.userName );
        }
        userId = *opt_user_id;
    }
    catch (const std::exception& e) {
        log_db::error("{}: select user_id failed: {}", __func__, e.what());
        return ERROR( CAT_SQL_ERR, "failed to select user_id" );
    }
    userIdStr = std::to_string( userId );

    log_sql::debug("chlModTicket SQL 4");

    // Get ticket id of ticket matching (user id, ticket string).
    try {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto opt_ticket_id = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"ticket_id"})
                .from("TICKET")
                .where(col("user_id") == userIdStr && col("ticket_string") == _ticket_string)
                .build());
        if (opt_ticket_id) {
            ticketId = *opt_ticket_id;
        }
        else {
            log_sql::debug("chlModTicket SQL 5");
            opt_ticket_id = irods::experimental::catalog::query_catalog_integer(
                executor, db_conn,
                gq2::builder::select({"ticket_id"})
                    .from("TICKET")
                    .where(col("user_id") == userIdStr && col("ticket_id") == _ticket_string)
                    .build());
            if (opt_ticket_id) {
                ticketId = *opt_ticket_id;
            }
            else {
                return ERROR( CAT_TICKET_INVALID, _ticket_string );
            }
        }
    }
    catch (const std::exception&) {
        return ERROR( CAT_TICKET_INVALID, _ticket_string );
    }
    ticketIdStr = std::to_string( ticketId );

    //
    // At this point, we have the user id and ticket id for the non-admin user.
    //

    // delete
    if ( strcmp( _op_name, "delete" ) == 0 ) {
        log_sql::debug("chlModTicket SQL 6");
        try {
            nanodbc::transaction trans{db_conn};
            auto del_ticket = gq2::builder::remove_from("R_TICKET_MAIN")
                .where(col("ticket_id") == ticketIdStr && col("user_id") == userIdStr)
                .build();
            auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, del_ticket);
            if ( res.affected_rows == 0 ) {
                return CODE( CAT_SUCCESS_BUT_WITH_NO_INFO );
            }

            try {
                auto del_hosts = gq2::builder::remove_from("R_TICKET_ALLOWED_HOSTS")
                    .where(col("ticket_id") == ticketIdStr)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_hosts);
            } catch (const std::exception& e) {
                log_db::info("delete allowed hosts failed: {}", e.what());
            }

            try {
                auto del_users = gq2::builder::remove_from("R_TICKET_ALLOWED_USERS")
                    .where(col("ticket_id") == ticketIdStr)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_users);
            } catch (const std::exception& e) {
                log_db::info("delete allowed users failed: {}", e.what());
            }

            try {
                auto del_groups = gq2::builder::remove_from("R_TICKET_ALLOWED_GROUPS")
                    .where(col("ticket_id") == ticketIdStr)
                    .build();
                irods::experimental::catalog::execute_catalog(executor, db_conn, del_groups);
            } catch (const std::exception& e) {
                log_db::info("delete allowed groups failed: {}", e.what());
            }

            trans.commit();
            return SUCCESS();
        }
        catch (const std::exception& e) {
            log_db::error("{}: delete failure: {}", __func__, e.what());
            return ERROR( CAT_SQL_ERR, "delete failure" );
        }
    } // delete operation

    // modify
    if ( strcmp( _op_name, "mod" ) == 0 ) {
        if (strcmp(_arg3, "uses") == 0) {
            char myTime[TIME_LEN]{};
            getNowStr(myTime);
            log_sql::debug("chlModTicket SQL 7");

            try {
                nanodbc::transaction trans{db_conn};
                auto upd_stmt = gq2::builder::update("TICKET")
                    .set("uses_limit", _arg4)
                    .set("modify_ts", myTime)
                    .where(col("ticket_id") == ticketIdStr && col("user_id") == userIdStr)
                    .build();
                auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                if (res.affected_rows == 0) {
                    return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                }
                trans.commit();
                return SUCCESS();
            }
            catch (const std::exception& e) {
                log_db::error("{}: update uses failed: {}", __func__, e.what());
                return ERROR(CAT_SQL_ERR, "SQL execution error.");
            }
        } // uses

        if ( strcmp(_arg3, "write-file") == 0 ) {
            char myTime[TIME_LEN]{};
            getNowStr(myTime);
            log_sql::debug("chlModTicket SQL 8");

            try {
                nanodbc::transaction trans{db_conn};
                auto upd_stmt = gq2::builder::update("TICKET")
                    .set("write_file_limit", _arg4)
                    .set("modify_ts", myTime)
                    .where(col("ticket_id") == ticketIdStr && col("user_id") == userIdStr)
                    .build();
                auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                if (res.affected_rows == 0) {
                    return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                }
                trans.commit();
                return SUCCESS();
            }
            catch (const std::exception& e) {
                log_db::error("{}: update write-file failed: {}", __func__, e.what());
                return ERROR(CAT_SQL_ERR, "SQL execution error.");
            }
        } // write-file

        if (strcmp(_arg3, "write-bytes") == 0) {
            char myTime[TIME_LEN]{};
            getNowStr(myTime);
            log_sql::debug("chlModTicket SQL 9");

            try {
                nanodbc::transaction trans{db_conn};
                auto upd_stmt = gq2::builder::update("TICKET")
                    .set("write_byte_limit", _arg4)
                    .set("modify_ts", myTime)
                    .where(col("ticket_id") == ticketIdStr && col("user_id") == userIdStr)
                    .build();
                auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                if (res.affected_rows == 0) {
                    return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                }
                trans.commit();
                return SUCCESS();
            }
            catch (const std::exception& e) {
                log_db::error("{}: update write-bytes failed: {}", __func__, e.what());
                return ERROR(CAT_SQL_ERR, "SQL execution error.");
            }
        } // write-bytes

        if (strcmp(_arg3, "expire") == 0 ) {
            std::string ticket_expiration_string;

            // Empty strings and zero (i.e. 0) are special in that they instruct the system
            // to clear the expiration timestamp.
            //
            // Prior versions of iRODS would result in setting the expiration timestamp
            // to the value passed, but it would be better to consolidate these values into
            // one outcome. Admins should not rely on the value in the database directly.
            // Admins should use the values returned by the APIs and tools.
            //
            // For this reason, if the server receives a zero or empty string, we don't have
            // to modify "ticket_expiration_string" because it is already an empty string.
            if (std::strcmp(_arg4, "") != 0 && std::strcmp(_arg4, "0") != 0) {
                try {
                    // Try to parse the timestamp argument as seconds since epoch.
                    const auto seconds_since_epoch = boost::lexical_cast<std::int64_t>(_arg4);
                    ticket_expiration_string = fmt::format("{:011}", seconds_since_epoch);
                }
                catch (const boost::bad_lexical_cast&) {
                    //
                    // If an exception was thrown, the timestamp argument was not something that
                    // represented seconds since epoch. For that reason, the client may have passed
                    // an actual timestamp, which we attempt to process here.
                    //

                    std::istringstream ss{_arg4};

                    // The facet allocated via the "new" operator is managed by the std::locale.
                    // The use of "new" here is correct and there are no memory leaks caused by this line.
                    ss.imbue(std::locale(ss.getloc(), new boost::posix_time::time_input_facet{"%Y-%m-%d.%H:%M:%S"}));

                    boost::posix_time::ptime t;
                    if (!(ss >> t)) {
                        return ERROR(SYS_INTERNAL_ERR, "Could not parse timestamp string into appropriate object.");
                    }

                    ticket_expiration_string = fmt::format("{:011}", to_time_t(t));
                }
            }

            char myTime[TIME_LEN]{}; // NOLINT(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays)
            getNowStr(myTime);
            log_sql::debug("chlModTicket SQL 10");

            try {
                nanodbc::transaction trans{db_conn};
                auto upd_stmt = gq2::builder::update("TICKET")
                    .set("ticket_expiry_ts", ticket_expiration_string)
                    .set("modify_ts", myTime)
                    .where(col("ticket_id") == ticketIdStr && col("user_id") == userIdStr)
                    .build();
                auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
                if (res.affected_rows == 0) {
                    return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);
                }
                trans.commit();
                return SUCCESS();
            }
            catch (const std::exception& e) {
                log_db::error("{}: update expire failed: {}", __func__, e.what());
                return ERROR(CAT_SQL_ERR, "SQL execution error.");
            }
        } // expire

        if ( strcmp( _arg3, "add" ) == 0 ) {
            if ( strcmp( _arg4, "host" ) == 0 ) {
                char *hostIp = convertHostToIp( _arg5 );
                if (!hostIp) {
                    return ERROR( CAT_HOSTNAME_INVALID, _arg5 );
                }

                try {
                    nanodbc::transaction trans{db_conn};
                    auto ins_stmt = gq2::builder::insert_into("TICKET_ALLOWED_HOSTS")
                        .set("ticket_id", ticketIdStr)
                        .set("host", hostIp)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);

                    char myTime[TIME_LEN]{}; // NOLINT(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays)
                    getNowStr(myTime);
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticketIdStr)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    auto err_msg = fmt::format(
                        "Failed to add host [{}] to ticket [{}]: {}", hostIp, ticketIdStr, e.what());
                    log_sql::error(err_msg);
                    return ERROR(CAT_SQL_ERR, std::move(err_msg));
                }
            } // host

            if ( strcmp( _arg4, "user" ) == 0 ) {
                status = icatGetTicketUserId( _ctx.prop_map(), _arg5, user2IdStr );
                if ( status != 0 ) {
                    return ERROR( status, "icatGetTicketUserId failed" );
                }

                try {
                    nanodbc::transaction trans{db_conn};
                    auto ins_stmt = gq2::builder::insert_into("TICKET_ALLOWED_USERS")
                        .set("ticket_id", ticketIdStr)
                        .set("user_name", _arg5)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);

                    char myTime[TIME_LEN]{}; // NOLINT(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays)
                    getNowStr(myTime);
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticketIdStr)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    auto err_msg = fmt::format(
                        "Failed to add user [{}] to ticket [{}]: {}", _arg5, ticketIdStr, e.what());
                    log_sql::error(err_msg);
                    return ERROR(CAT_SQL_ERR, std::move(err_msg));
                }
            } // user

            if ( strcmp( _arg4, "group" ) == 0 ) {
                status = icatGetTicketGroupId( _ctx.prop_map(), _arg5, user2IdStr );
                if ( status != 0 ) {
                    return ERROR( status, "icatGetTicketGroupId failed" );
                }

                try {
                    nanodbc::transaction trans{db_conn};
                    auto ins_stmt = gq2::builder::insert_into("TICKET_ALLOWED_GROUPS")
                        .set("ticket_id", ticketIdStr)
                        .set("group_name", _arg5)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, ins_stmt);

                    char myTime[TIME_LEN]{}; // NOLINT(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays)
                    getNowStr(myTime);
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticketIdStr)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    auto err_msg = fmt::format(
                        "Failed to add group [{}] to ticket [{}]: {}", _arg5, ticketIdStr, e.what());
                    log_sql::error(err_msg);
                    return ERROR(CAT_SQL_ERR, std::move(err_msg));
                }
            } // group
        } // add
        else if ( strcmp( _arg3, "remove" ) == 0 ) {
            if ( strcmp( _arg4, "host" ) == 0 ) {
                char *hostIp = convertHostToIp( _arg5 );
                if (!hostIp) {
                    return ERROR( CAT_HOSTNAME_INVALID, "host name null" );
                }

                try {
                    nanodbc::transaction trans{db_conn};
                    auto del_stmt = gq2::builder::remove_from("TICKET_ALLOWED_HOSTS")
                        .where(col("ticket_id") == ticketIdStr && col("host") == hostIp)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

                    char myTime[TIME_LEN]{}; // NOLINT(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays)
                    getNowStr(myTime);
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticketIdStr)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    auto err_msg = fmt::format(
                        "Failed to remove host [{}] from ticket [{}]: {}", hostIp, ticketIdStr, e.what());
                    log_sql::error(err_msg);
                    return ERROR(CAT_SQL_ERR, std::move(err_msg));
                }
            } // host

            if ( strcmp( _arg4, "user" ) == 0 ) {
                status = icatGetTicketUserId( _ctx.prop_map(), _arg5, user2IdStr );
                if ( status != 0 ) {
                    return ERROR( status, "icatGetTicketUserId failed" );
                }

                try {
                    nanodbc::transaction trans{db_conn};
                    auto del_stmt = gq2::builder::remove_from("TICKET_ALLOWED_USERS")
                        .where(col("ticket_id") == ticketIdStr && col("user_name") == _arg5)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

                    char myTime[TIME_LEN]{}; // NOLINT(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays)
                    getNowStr(myTime);
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticketIdStr)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    auto err_msg = fmt::format(
                        "Failed to remove user [{}] from ticket [{}]: {}", _arg5, ticketIdStr, e.what());
                    log_sql::error(err_msg);
                    return ERROR(CAT_SQL_ERR, std::move(err_msg));
                }
            } // user

            if ( strcmp( _arg4, "group" ) == 0 ) {
                status = icatGetTicketGroupId( _ctx.prop_map(), _arg5, user2IdStr );
                if ( status != 0 ) {
                    return ERROR( status, "icatGetTicketGroupId failed" );
                }

                try {
                    nanodbc::transaction trans{db_conn};
                    auto del_stmt = gq2::builder::remove_from("TICKET_ALLOWED_GROUPS")
                        .where(col("ticket_id") == ticketIdStr && col("group_name") == _arg5)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, del_stmt);

                    char myTime[TIME_LEN]{}; // NOLINT(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays)
                    getNowStr(myTime);
                    auto upd_stmt = gq2::builder::update("TICKET")
                        .set("modify_ts", myTime)
                        .where(col("ticket_id") == ticketIdStr)
                        .build();
                    irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);

                    trans.commit();
                    return SUCCESS();
                }
                catch (const std::exception& e) {
                    auto err_msg = fmt::format(
                        "Failed to remove group [{}] from ticket [{}]: {}", _arg5, ticketIdStr, e.what());
                    log_sql::error(err_msg);
                    return ERROR(CAT_SQL_ERR, std::move(err_msg));
                }
            } // group
        } // remove
    } // modify operation

    return ERROR( CAT_INVALID_ARGUMENT, "invalid op name" );
} // db_mod_ticket_op

irods::error db_get_icss_op(
    irods::plugin_context& _ctx,
    icatSessionStruct**    _icss ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check incoming pointers
    if ( !_icss ) {
        return ERROR(
                   SYS_INVALID_INPUT_PARAM,
                   "null or invalid input param" );
    }

    log_sql::debug("chlGetRcs");
    if ( icss.status != 1 ) {
        ( *_icss ) = 0;
        return ERROR( icss.status, "catalog not connected" );
    }

    ( *_icss ) = &icss;
    return SUCCESS();

} // db_get_icss_op

// =-=-=-=-=-=-=-
// from general_query.cpp ::
int chl_gen_query_impl( genQueryInp_t, genQueryOut_t* );

irods::error db_gen_query_op(
    irods::plugin_context& _ctx,
    genQueryInp_t*         _gen_query_inp,
    genQueryOut_t*         _result ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_gen_query_inp
       ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );

    }

    int status = chl_gen_query_impl(
                     *_gen_query_inp,
                     _result );
//         if( status < 0 ) {
//             return ERROR( status, "chl_gen_query_impl failed" );
//         } else {
//             return SUCCESS();
//         }
    return CODE( status );

} // db_gen_query_op

// =-=-=-=-=-=-=-
// from general_query.cpp ::
int chl_gen_query_access_control_setup_impl( const char*, const char*, const char*, int, int );

irods::error db_gen_query_access_control_setup_op(
    irods::plugin_context& _ctx,
    const char*            _user,
    const char*            _zone,
    const char*            _host,
    int                    _priv,
    int                    _control_flag ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    //if ( ) {
    //    return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );
    //
    //}

    int status = chl_gen_query_access_control_setup_impl(
                     _user,
                     _zone,
                     _host,
                     _priv,
                     _control_flag );
    if ( status < 0 ) {
        return ERROR( status, "chl_gen_query_access_control_setup_impl failed" );
    }
    else {
        return CODE( status );
    }

} // db_gen_query_access_control_setup_op

// =-=-=-=-=-=-=-
// from general_query.cpp ::
int chl_gen_query_ticket_setup_impl( const char*, const char* );

irods::error db_gen_query_ticket_setup_op(
    irods::plugin_context& _ctx,
    const char*            _ticket,
    const char*            _client_addr ) {
    // =-=-=-=-=-=-=-
    // check the context
    irods::error ret = _ctx.valid();
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // check the params
    if ( !_ticket ||
            !_client_addr ) {
        return ERROR( CAT_INVALID_ARGUMENT, "null parameter" );

    }

    int status = chl_gen_query_ticket_setup_impl(
                     _ticket,
                     _client_addr );
    if ( status < 0 ) {
        return ERROR( status, "chl_gen_query_ticket_setup_impl failed" );
    }
    else {
        return SUCCESS();
    }

} // db_gen_query_ticket_setup_op

auto db_check_permission_to_modify_data_object_op(
    irods::plugin_context& _ctx,
    const rodsLong_t       _data_id) -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
    const auto ec = irods::experimental::catalog::access_control::check_data_object_id(
        executor, db_conn,
        std::to_string(_data_id),
        _ctx.comm()->clientUser.userName,
        _ctx.comm()->clientUser.rodsZone,
        ACCESS_MODIFY_METADATA,
        mySessionTicket,
        mySessionClientAddr);

    if (ec != 0) {
        const auto msg = fmt::format("user does not have permission to modify object with data id [{}]", _data_id);
        log_db::info("[{}:{}] - [{}]", __func__, __LINE__, msg);
        return ERROR(ec, msg);
    }

    return SUCCESS();
} // db_check_permission_to_modify_data_object_op

auto db_update_ticket_write_byte_count_op(
    irods::plugin_context& _ctx,
    const rodsLong_t       _data_id,
    const rodsLong_t       _bytes_written) -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    if (std::string_view{mySessionTicket}.empty()) {
        // nothing to do
        return SUCCESS();
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};
        const auto ec = irods::experimental::catalog::access_control::ticket_update_write_bytes(
            executor, db_conn, mySessionTicket, std::to_string(_bytes_written), std::to_string(_data_id));

        if (ec != 0) {
            const auto msg = fmt::format(
                "failed to update write_byte_count for ticket "
                "[data_id=[{}], ticket=[{}], bytes_written=[{}]]",
                _data_id, mySessionTicket, _bytes_written);

            log_db::info("[{}:{}] - [{}]", __func__, __LINE__, msg);

            return ERROR(ec, msg);
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const std::exception& e) {
        log_db::error("{}: Exception caught: {}", __func__, e.what());
        return ERROR(CAT_SQL_ERR, e.what());
    }
} // db_update_ticket_write_byte_count_op

auto db_get_delay_rule_info_op(irods::plugin_context& _ctx, const char* _rule_id, std::vector<std::string>* _info)
    -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    if (!_rule_id || !_info) {
        return ERROR(SYS_INVALID_INPUT_PARAM, "Invalid input: rule id or info container is null");
    }

    log_db::debug("{}: _rule_id => [{}]", __func__, _rule_id);

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto stmt = gq2::builder::select({
                "rule_name", "rei_file_path", "user_name", "exe_address", "exe_time",
                "exe_frequency", "priority", "last_exe_time", "exe_status", "estimated_exe_time",
                "notification_addr", "exe_context"})
            .from("RULE_EXEC")
            .where(col("rule_exec_id") == _rule_id)
            .build();

        auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
        if (!res.query_result || !res.query_result->next()) {
            const auto msg = fmt::format("Could not find a delay rule with id [{}].", _rule_id);
            log_db::error(msg);
            return ERROR(CAT_NO_ROWS_FOUND, msg);
        }

        constexpr auto number_of_columns = 12;

        for (int i = 0; i < number_of_columns; ++i) {
            _info->push_back(res.query_result->get<std::string>(i, ""));
        }

        return SUCCESS();
    }
    catch (const std::exception& e) {
        return ERROR(SYS_LIBRARY_ERROR, e.what());
    }
} // db_get_delay_rule_info_op

auto db_data_object_finalize_op(irods::plugin_context& _ctx, const char* _json_input) -> irods::error
{
    using json = nlohmann::json;

    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    try {
        auto input = json::parse(_json_input);

        auto& replicas = input.at("replicas");
        if (replicas.empty()) {
            return ERROR(JSON_VALIDATION_ERROR, "JSON does not conform to the expected format");
        }

        const auto get_json_string_value = [](const json& _json, const char* _key) -> const std::string& {
            return _json.at(_key).get_ref<const std::string&>();
        };

        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        // Loops over all the replicas and executes an update on that row in R_DATA_MAIN using the replica information
        // found in the "after" entry for each replica.
        for (auto& r : replicas) {
            const auto& before = r.at("before");

            auto& after = r.at("after");

            // SET_TIME_TO_NOW_KW allows the database update to reflect the time of the modification as close as
            // possible to the actual modification of the replica.
            {
                using object_time_type = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>;
                using clock_type = object_time_type::clock;
                using duration_type = object_time_type::duration;

                if (auto& modify_ts = after.at("modify_ts"); modify_ts.get_ref<std::string&>() == SET_TIME_TO_NOW_KW) {
                    const auto now = std::chrono::time_point_cast<duration_type>(clock_type::now());

                    modify_ts = fmt::format("{:011}", now.time_since_epoch().count());
                }
            }

            irods::log(LOG_DEBUG9, fmt::format("before:[{}]", before.dump()));
            irods::log(LOG_DEBUG9, fmt::format("after:[{}]", after.dump()));

            auto upd_stmt = gq2::builder::update("DATA_OBJECT")
                .set("data_repl_num", get_json_string_value(after, "data_repl_num"))
                .set("data_version", get_json_string_value(after, "data_version"))
                .set("data_type_name", get_json_string_value(after, "data_type_name"))
                .set("data_size", get_json_string_value(after, "data_size"))
                .set("data_path", get_json_string_value(after, "data_path"))
                .set("data_owner_name", get_json_string_value(after, "data_owner_name"))
                .set("data_owner_zone", get_json_string_value(after, "data_owner_zone"))
                .set("data_is_dirty", get_json_string_value(after, "data_is_dirty"))
                .set("data_status", get_json_string_value(after, "data_status"))
                .set("data_checksum", get_json_string_value(after, "data_checksum"))
                .set("data_expiry_ts", get_json_string_value(after, "data_expiry_ts"))
                .set("data_map_id", get_json_string_value(after, "data_map_id"))
                .set("data_mode", get_json_string_value(after, "data_mode"))
                .set("r_comment", get_json_string_value(after, "r_comment"))
                .set("create_ts", get_json_string_value(after, "create_ts"))
                .set("modify_ts", get_json_string_value(after, "modify_ts"))
                .set("resc_id", get_json_string_value(after, "resc_id"))
                .where(col("data_id") == get_json_string_value(before, "data_id") &&
                       col("resc_id") == get_json_string_value(before, "resc_id"))
                .build();

            irods::experimental::catalog::execute_catalog(executor, db_conn, upd_stmt);
        }

        trans.commit();
    }
    catch (const json::exception& e) {
        std::string msg = fmt::format("[{}:{}] - JSON error occurred [{}]", __func__, __LINE__, e.what());
        log_db::error(msg);
        return ERROR(SYS_LIBRARY_ERROR, std::move(msg));
    }
    catch (const nanodbc::database_error& e) {
        std::string msg = fmt::format("[{}:{}] - Database error occurred [{}]", __func__, __LINE__, e.what());
        log_db::error(msg);
        return ERROR(CAT_SQL_ERR, std::move(msg));
    }
    catch (const std::exception& e) {
        std::string msg = fmt::format("[{}:{}] - Exception occurred [{}]", __func__, __LINE__, e.what());
        log_db::error(msg);
        return ERROR(SYS_INTERNAL_ERR, std::move(msg));
    }
    catch (...) {
        std::string msg = fmt::format("[{}:{}] - Unknown error occurred", __func__, __LINE__);
        log_db::error(msg);
        return ERROR(SYS_UNKNOWN_ERROR, std::move(msg));
    }

    return SUCCESS();
} // db_data_object_finalize_op

auto db_check_auth_credentials_op(irods::plugin_context& _ctx,
                                  const char* _username,
                                  const char* _zone,
                                  const char* _password,
                                  int* _correct) -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    if (!_username || !_zone || !_password || !_correct) {
        log_db::error("{}: Received one or more null pointers.", __func__);
        return ERROR(SYS_INVALID_INPUT_PARAM, "Received one or more null pointers.");
    }

    *_correct = -1; // Indicates the correctness of the credentials is unknown.

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
    std::array<char, MAX_PASSWORD_LEN + 20> decoded_password{};

    if (const auto ec = decodePw(_ctx.comm(), _password, decoded_password.data()); ec < 0) {
        log_db::error("{}: Failed to decode password with error code [{}].", __func__, ec);
        return ERROR(ec, "Password decode error.");
    }

    icatScramble(decoded_password.data());

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto opt_uid = irods::experimental::catalog::query_catalog_integer(
            executor, db_conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _username && col("zone_name") == _zone)
                .build());

        std::optional<int64_t> opt_user_id;
        if (opt_uid) {
            auto opt_pw = irods::experimental::catalog::query_catalog_string(
                executor, db_conn,
                gq2::builder::select({"rcat_password"})
                    .from("USER_PASSWORD")
                    .where(col("user_id") == std::to_string(*opt_uid) && col("rcat_password") == decoded_password.data())
                    .build());
            if (opt_pw) {
                opt_user_id = *opt_uid;
            }
        }

        if (!opt_user_id) {
            log_db::warn("{}: Incorrect credentials for user [{}#{}].", __func__, _username, _zone);
            *_correct = 0;
        }
        else {
            *_correct = 1;
        }

        return SUCCESS();
    }
    catch (const irods::exception& e) {
        log_db::error("{}: {}", __func__, e.client_display_what());
        return ERROR(SYS_LIBRARY_ERROR, e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: {}", __func__, e.what());
        return ERROR(SYS_LIBRARY_ERROR, e.what());
    }
    catch (...) {
        log_db::error("{}: An unknown error was caught.", __func__);
        return ERROR(SYS_UNKNOWN_ERROR, "An unknown error was caught.");
    }
} // db_check_auth_credentials_op

auto db_execute_genquery2_sql(irods::plugin_context& _ctx,
                              const char* _sql,
                              const std::vector<std::string>* _values,
                              char** _output) -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    if (!_sql || !_values || !_output) {
        log_db::error("{}: Received one or more null pointers.", __func__);
        return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Received one or more null pointers.");
    }

    *_output = nullptr;

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        auto row = executor.execute_query(db_conn, _sql, *_values);

        using json = nlohmann::json;

        auto json_array = json::array();
        auto json_row = json::array();

        const auto n_cols = row.columns();

        while (row.next()) {
            for (std::remove_cvref_t<decltype(n_cols)> i = 0; i < n_cols; ++i) {
                json_row.push_back(row.get<std::string>(i, ""));
            }

            json_array.push_back(json_row);
            json_row.clear();
        }

        *_output = strdup(json_array.dump().c_str());

        return SUCCESS();
    }
    catch (const irods::exception& e) {
        log_db::error("{}: {}", __func__, e.client_display_what());
        return ERROR(e.code(), e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: {}", __func__, e.what());
        return ERROR(SYS_LIBRARY_ERROR, e.what());
    }
    catch (...) {
        log_db::error("{}: An unknown error was caught.", __func__);
        return ERROR(SYS_UNKNOWN_ERROR, "An unknown error was caught.");
    }
} // db_execute_genquery2_sql

auto db_delay_rule_lock(irods::plugin_context& _ctx, const char* _rule_id, const char* _lock_host, int _lock_host_pid)
    -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    if (!_rule_id || !_lock_host || !_lock_host_pid) {
        log_db::error("{}: Received one or more null pointers.", __func__);
        return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Received one or more null pointers.");
    }

    try {
        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        const auto [secs, millis] = get_current_time();
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto stmt = gq2::builder::update("RULE_EXEC")
            .set("lock_host", _lock_host)
            .set("lock_host_pid", std::to_string(_lock_host_pid))
            .set("lock_ts", secs)
            .where(col("rule_exec_id") == _rule_id && col("lock_host") == "" && col("lock_host_pid") == "" && col("lock_ts") == "")
            .build();

        const auto res = irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
        const auto affected = res.affected_rows;

        if (affected != 1) {
            auto msg = fmt::format("{}: Failed to lock delay rule [rule_id={}, lock_host={}, lock_host_pid={}].",
                                   __func__,
                                   _rule_id,
                                   _lock_host,
                                   _lock_host_pid);
            log_db::error(msg);
            return ERROR(CAT_NO_ROWS_UPDATED, std::move(msg));
        }

        trans.commit();
        return SUCCESS();
    }
    catch (const irods::exception& e) {
        log_db::error("{}: {}", __func__, e.client_display_what());
        return ERROR(e.code(), e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: {}", __func__, e.what());
        return ERROR(SYS_LIBRARY_ERROR, e.what());
    }
} // db_delay_rule_lock

auto db_delay_rule_unlock(irods::plugin_context& _ctx, const char* _rule_ids) -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    if (!_rule_ids) {
        log_db::error("{}: Rule ID list cannot be a null pointer.", __func__);
        return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Rule ID list cannot be a null pointer.");
    }

    try {
        const auto rule_ids = nlohmann::json::parse(_rule_ids);

        auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
        nanodbc::transaction trans{db_conn};

        const auto [secs, millis] = get_current_time();
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        for (auto&& rule_id : rule_ids) {
            const auto& rule_id_string = rule_id.get_ref<const std::string&>();
            try {
                log_db::debug("{}: Successfully converted rule_id string [{}] to integer [{}].",
                              __func__,
                              rule_id_string,
                              std::stoll(rule_id_string));
            }
            catch (const std::exception& e) {
                log_db::error(
                    "{}: Could not convert rule_id string [{}] to integer: {}", __func__, rule_id_string, e.what());
                return ERROR(SYS_INVALID_INPUT_PARAM,
                             fmt::format("Could not convert rule_id string [{}] to integer", rule_id_string));
            }

            auto stmt = gq2::builder::update("RULE_EXEC")
                .set("lock_host", "")
                .set("lock_host_pid", "")
                .set("lock_ts", "")
                .set("modify_ts", secs)
                .where(col("rule_exec_id") == rule_id_string)
                .build();
            irods::experimental::catalog::execute_catalog(executor, db_conn, stmt);
        }

        trans.commit();

        return SUCCESS();
    }
    catch (const irods::exception& e) {
        log_db::error("{}: {}", __func__, e.client_display_what());
        return ERROR(e.code(), e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: {}", __func__, e.what());
        return ERROR(SYS_LIBRARY_ERROR, e.what());
    }
} // db_delay_rule_unlock

auto db_update_replica_access_time(irods::plugin_context& _ctx,
                                   const char* _json_input,
                                   [[maybe_unused]] char** _output) -> irods::error
{
    if (const auto ret = _ctx.valid(); !ret.ok()) {
        return PASS(ret);
    }

    // The output pointer is not used because the ODBC API isn't guaranteed to tell us
    // which SQL statement failed. However, it is made available to future-proof the APIs
    // supporting access time. Even though the output pointer isn't used, we still check it
    // as a way to enforce correctness.
    if (!_json_input || !_output) {
        log_db::error("{}: Received one or more null pointers.", __func__);
        return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Received one or more null pointers.");
    }

    try {
        const auto json_input = nlohmann::json::parse(_json_input);
        const auto& updates = json_input.at("access_time_updates");

        std::vector<std::size_t> data_ids;
        std::vector<std::size_t> replica_numbers;
        std::vector<std::string> access_times;

        data_ids.reserve(updates.size());
        replica_numbers.reserve(updates.size());
        access_times.reserve(updates.size());

        std::for_each(std::begin(updates),
                      std::end(updates),
                      [&data_ids, &replica_numbers, &access_times](const nlohmann::json& _j) {
                          data_ids.emplace_back(_j.at("data_id").get<std::size_t>());
                          replica_numbers.emplace_back(_j.at("replica_number").get<std::size_t>());
                          access_times.emplace_back(_j.at("atime").get<std::string>());
                      });

        auto [db_instance, db_conn] = irods::experimental::catalog::get_session_connection();

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        static const auto [sql, unused_params] = gq2::to_sql(
            gq2::builder::update("DATA_OBJECT")
                .set("DATA_ACCESS_TIME", "?")
                .where(col("DATA_ID") == "?" && col("DATA_REPL_NUM") == "?")
                .build(),
            gq2::options{.admin_mode = true});
        log_sql::debug("{}: SQL = [{}]", __func__, sql);

        nanodbc::statement stmt{db_conn};
        nanodbc::prepare(stmt, sql);

        stmt.bind_strings(0, access_times);
        stmt.bind(1, data_ids.data(), data_ids.size());
        stmt.bind(2, replica_numbers.data(), replica_numbers.size());

        // Execute the batch operation within a transaction. From a behavior perspective, it
        // would be better to use execute() instead of just_transact() because each update
        // would be committed independently. However, we've chosen to update all rows atomically.
        // This decision is strictly for performance reasons and ultimately means we feel it's
        // acceptable to rollback all updates on failure.
        just_transact(stmt, updates.size());

        return SUCCESS();
    }
    catch (const irods::exception& e) {
        log_db::error("{}: {}", __func__, e.client_display_what());
        return ERROR(e.code(), e.what());
    }
    catch (const std::exception& e) {
        log_db::error("{}: {}", __func__, e.what());
        return ERROR(SYS_LIBRARY_ERROR, e.what());
    }
} // db_update_replica_access_time

// =-=-=-=-=-=-=-
//
irods::error db_start_operation( irods::plugin_property_map& _props ) {
    return SUCCESS();

} // db_start_operation


// =-=-=-=-=-=-=-
// derive a new tcp network plugin from
// the network plugin base class for handling
// tcp communications
class postgres_database_plugin : public irods::database {
    public:
        postgres_database_plugin(const std::string& _nm, const std::string& _ctx)
            : irods::database(_nm, _ctx)
        {
            // =-=-=-=-=-=-=-
            // create a property for the icat session
            // which will manage the lifetime of the db
            // connection - use a copy ctor to init
            icatSessionStruct icss;
            std::memset(&icss, 0, sizeof(icss));
            properties_.set< icatSessionStruct >( ICSS_PROP, icss );

            set_start_operation( db_start_operation );
        } // ctor

        ~postgres_database_plugin()
        {
        }
}; // class postgres_database_plugin

// =-=-=-=-=-=-=-
// factory function to provide instance of the plugin
extern "C"
irods::database* plugin_factory(
    const std::string& _inst_name,
    const std::string& _context ) {
    // =-=-=-=-=-=-=-
    // create a postgres database plugin instance
    postgres_database_plugin* pg = new postgres_database_plugin(
        _inst_name,
        _context );

    // =-=-=-=-=-=-=-
    // fill in the operation table mapping call
    // names to function na,mes
    using namespace irods;
    using namespace std;
    pg->add_operation(
        DATABASE_OP_START,
        function<error(plugin_context&)>(
            db_start_op ) );
    pg->add_operation(
        DATABASE_OP_DEBUG,
        function<error(plugin_context&, const char*)>(
            db_debug_op ) );
    pg->add_operation(
        DATABASE_OP_OPEN,
        function<error(plugin_context&)>(
            db_open_op ) );
    pg->add_operation(
        DATABASE_OP_CLOSE,
        function<error(plugin_context&)>(
            db_close_op ) );
    pg->add_operation(
        DATABASE_OP_GET_LOCAL_ZONE,
        function<error(plugin_context&,std::string*)>(
            db_get_local_zone_op ) );
    pg->add_operation(
        DATABASE_OP_UPDATE_RESC_OBJ_COUNT,
        function<error(plugin_context&,const std::string*, int)>(
            db_update_resc_obj_count_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_DATA_OBJ_META,
        function<error(plugin_context&,dataObjInfo_t*,keyValPair_t*)>(
            db_mod_data_obj_meta_op ) );
    pg->add_operation(
        DATABASE_OP_REG_DATA_OBJ,
        function<error(plugin_context&,dataObjInfo_t*)>(
            db_reg_data_obj_op ) );
    pg->add_operation(
        DATABASE_OP_REG_REPLICA,
        function<error(plugin_context&,dataObjInfo_t*,dataObjInfo_t*,keyValPair_t*)>(
            db_reg_replica_op ) );
    pg->add_operation(
        DATABASE_OP_UNREG_REPLICA,
        function<error(plugin_context&,dataObjInfo_t*,keyValPair_t*)>(
            db_unreg_replica_op ) );
    pg->add_operation(
        DATABASE_OP_REG_RULE_EXEC,
        function<error(plugin_context&,ruleExecSubmitInp_t*)>(
            db_reg_rule_exec_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_RULE_EXEC,
        function<error(plugin_context&,const char*,keyValPair_t*)>(
            db_mod_rule_exec_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_RULE_EXEC,
        function<error(plugin_context&,const char*)>(
            db_del_rule_exec_op ) );
    pg->add_operation(
        DATABASE_OP_ADD_CHILD_RESC,
        function<error(plugin_context&,map<string,string>*)>(
            db_add_child_resc_op ) );
    pg->add_operation(
        DATABASE_OP_REG_RESC,
        function<error(plugin_context&,map<string, string>*)>(
            db_reg_resc_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_CHILD_RESC,
        function<error(plugin_context&,map<string,string>*)>(
            db_del_child_resc_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_RESC,
        function<error(plugin_context&,const char*,int)>(
            db_del_resc_op ) );
    pg->add_operation(
        DATABASE_OP_ROLLBACK,
        function<error(plugin_context&)>(
            db_rollback_op ) );
    pg->add_operation(
        DATABASE_OP_COMMIT,
        function<error(plugin_context&)>(
            db_commit_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_USER_RE,
        function<error(plugin_context&,userInfo_t*)>(
            db_del_user_re_op ) );
    pg->add_operation(
        DATABASE_OP_REG_COLL_BY_ADMIN,
        function<error(plugin_context&,collInfo_t*)>(
            db_reg_coll_by_admin_op ) );
    pg->add_operation(
        DATABASE_OP_REG_COLL,
        function<error(plugin_context&,collInfo_t*)>(
            db_reg_coll_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_COLL,
        function<error(plugin_context&,collInfo_t*)>(
            db_mod_coll_op ) );
    pg->add_operation(
        DATABASE_OP_REG_ZONE,
        function<error(plugin_context&,const char*,const char*,const char*,const char*)>(
            db_reg_zone_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_ZONE,
        function<error(plugin_context&,const char*,const char*,const char*)>(
            db_mod_zone_op ) );
    pg->add_operation(
        DATABASE_OP_RENAME_COLL,
        function<error(plugin_context&,const char*,const char*)>(
            db_rename_coll_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_ZONE_COLL_ACL,
        function<error(plugin_context&,const char*,const char*,const char*)>(
            db_mod_zone_coll_acl_op ) );
    pg->add_operation(
        DATABASE_OP_RENAME_LOCAL_ZONE,
        function<error(plugin_context&,const char*,const char*)>(
            db_rename_local_zone_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_ZONE,
        function<error(plugin_context&,const char*)>(
            db_del_zone_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_COLL_BY_ADMIN,
        function<error(plugin_context&,collInfo_t*)>(
            db_del_coll_by_admin_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_COLL,
        function<error(plugin_context&,collInfo_t*)>(
            db_del_coll_op ) );
    pg->add_operation(
        DATABASE_OP_CHECK_AUTH,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,int*,int*)>(
            db_check_auth_op ) );
    pg->add_operation(
        DATABASE_OP_MAKE_TEMP_PW,
        function<error(plugin_context&,char*, const char*)>(
            db_make_temp_pw_op ) );
    pg->add_operation(DATABASE_OP_UPDATE_PAM_PASSWORD,
                      function<error(plugin_context&, const char*, int, const char*, char**, std::size_t)>(
                          db_update_pam_password_op));
    pg->add_operation(
        DATABASE_OP_MOD_USER,
        function<error(plugin_context&,const char*,const char*,const char*)>(
            db_mod_user_op ) );
    pg->add_operation(
        DATABASE_OP_MAKE_LIMITED_PW,
        function<error(plugin_context&,int,char*)>(
            db_make_limited_pw_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_GROUP,
        function<error(plugin_context&,const char*,const char*,const char*,const char*)>(
            db_mod_group_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_RESC,
        function<error(plugin_context&,const char*,const char*,const char*)>(
            db_mod_resc_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_RESC_DATA_PATHS,
        function<error(plugin_context&,const char*,const char*,const char*,const char*)>(
            db_mod_resc_data_paths_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_RESC_FREESPACE,
        function<error(plugin_context&,const char*,int)>(
            db_mod_resc_freespace_op ) );
    pg->add_operation(
        DATABASE_OP_REG_USER_RE,
        function<error(plugin_context&,userInfo_t*)>(
            db_reg_user_re_op ) );
    pg->add_operation(
        DATABASE_OP_SET_AVU_METADATA,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const KeyValPair*)>(
            db_set_avu_metadata_op ) );
    pg->add_operation(
        DATABASE_OP_ADD_AVU_METADATA,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const KeyValPair*)>(
            db_add_avu_metadata_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_AVU_METADATA,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const KeyValPair*)>(
            db_mod_avu_metadata_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_AVU_METADATA,
        function<error(plugin_context&,int,const char*,const char*,const char*,const char*,const char*,int,const KeyValPair*)>(
            db_del_avu_metadata_op ) );
    pg->add_operation(
        DATABASE_OP_COPY_AVU_METADATA,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const KeyValPair*)>(
            db_copy_avu_metadata_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_ACCESS_CONTROL_RESC,
        function<error(plugin_context&,int,const char*,const char*,const char*,const char*)>(
            db_mod_access_control_resc_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_ACCESS_CONTROL,
        function<error(plugin_context&,int,const char*,const char*,const char*,const char*)>(
            db_mod_access_control_op ) );
    pg->add_operation(
        DATABASE_OP_RENAME_OBJECT,
        function<error(plugin_context&,rodsLong_t,const char*)>(
            db_rename_object_op ) );
    pg->add_operation(
        DATABASE_OP_MOVE_OBJECT,
        function<error(plugin_context&,rodsLong_t,rodsLong_t)>(
            db_move_object_op ) );
    pg->add_operation(
        DATABASE_OP_REG_TOKEN,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const char*)>(
            db_reg_token_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_TOKEN,
        function<error(plugin_context&,const char*,const char*)>(
            db_del_token_op ) );
    pg->add_operation(
        DATABASE_OP_REG_SERVER_LOAD,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*)>(
            db_reg_server_load_op ) );
    pg->add_operation(
        DATABASE_OP_PURGE_SERVER_LOAD,
        function<error(plugin_context&,const char*)>(
            db_purge_server_load_op ) );
    pg->add_operation(
        DATABASE_OP_REG_SERVER_LOAD_DIGEST,
        function<error(plugin_context&,const char*,const char*)>(
            db_reg_server_load_digest_op ) );
    pg->add_operation(
        DATABASE_OP_PURGE_SERVER_LOAD_DIGEST,
        function<error(plugin_context&,const char*)>(
            db_purge_server_load_digest_op ) );
    pg->add_operation(
        DATABASE_OP_GET_GRID_CONFIGURATION_VALUE,
        function<error(plugin_context&, const char*, const char*, char*, std::size_t)>(
            db_get_grid_configuration_value_op));
    pg->add_operation(
        DATABASE_OP_SET_GRID_CONFIGURATION_VALUE,
        function<error(plugin_context&, const char*, const char*, const char*)>(
            db_set_grid_configuration_value_op));
    pg->add_operation(
        DATABASE_OP_CALC_USAGE_AND_QUOTA,
        function<error(plugin_context&)>(
            db_calc_usage_and_quota_op ) );
    pg->add_operation(
        DATABASE_OP_SET_QUOTA,
        function<error(plugin_context&,const char*,const char*,const char*,const char*)>(
            db_set_quota_op ) );
    pg->add_operation(
        DATABASE_OP_CHECK_QUOTA,
        function<error(plugin_context&,const char*,const char*,rodsLong_t*,int*)>(
            db_check_quota_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_UNUSED_AVUS,
        function<error(plugin_context&)>(
            db_del_unused_avus_op ) );
    pg->add_operation(
        DATABASE_OP_INS_RULE_TABLE,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*)>(
            db_ins_rule_table_op ) );
    pg->add_operation(
        DATABASE_OP_INS_DVM_TABLE,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*)>(
            db_ins_dvm_table_op ) );
    pg->add_operation(
        DATABASE_OP_INS_FNM_TABLE,
        function<error(plugin_context&,const char*,const char*,const char*,const char*)>(
            db_ins_fnm_table_op ) );
    pg->add_operation(
        DATABASE_OP_INS_MSRVC_TABLE,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*,const char*)>(
            db_ins_msrvc_table_op ) );
    pg->add_operation(
        DATABASE_OP_VERSION_RULE_BASE,
        function<error(plugin_context&,const char*,const char*)>(
            db_version_rule_base_op ) );
    pg->add_operation(
        DATABASE_OP_VERSION_DVM_BASE,
        function<error(plugin_context&,const char*,const char*)>(
            db_version_dvm_base_op ) );
    pg->add_operation(
        DATABASE_OP_VERSION_FNM_BASE,
        function<error(plugin_context&,const char*,const char*)>(
            db_version_fnm_base_op ) );
    pg->add_operation(
        DATABASE_OP_ADD_SPECIFIC_QUERY,
        function<error(plugin_context&,const char*,const char*)>(
            db_add_specific_query_op ) );
    pg->add_operation(
        DATABASE_OP_DEL_SPECIFIC_QUERY,
        function<error(plugin_context&,const char*)>(
            db_del_specific_query_op ) );
    pg->add_operation(
        DATABASE_OP_SPECIFIC_QUERY,
        function<error(plugin_context&,specificQueryInp_t*,genQueryOut_t*)>(
            db_specific_query_op ) );
    pg->add_operation(
        DATABASE_OP_GET_HIERARCHY_FOR_RESC,
        function<error(plugin_context&,const string*, const string*,std::string*)>(
            db_get_hierarchy_for_resc_op ) );
    pg->add_operation(
        DATABASE_OP_MOD_TICKET,
        function<error(plugin_context&,const char*,const char*,const char*,const char*,const char*,const KeyValPair*)>(
            db_mod_ticket_op ) );
    pg->add_operation(
        DATABASE_OP_CHECK_AND_GET_OBJ_ID,
        function<error(plugin_context&,const char*,const char*,const char*)>(
            db_check_and_get_object_id_op ) );
    pg->add_operation(
        DATABASE_OP_GET_RCS,
        function<error(plugin_context&,icatSessionStruct**)>(
            db_get_icss_op ) );
    pg->add_operation(
        DATABASE_OP_GEN_QUERY,
        function<error(plugin_context&,genQueryInp_t*,genQueryOut_t*)>(
            db_gen_query_op ) );
    pg->add_operation(
        DATABASE_OP_GEN_QUERY_ACCESS_CONTROL_SETUP,
        function<error(plugin_context&,const char*,const char*,const char*,int,int)>(
            db_gen_query_access_control_setup_op ) );
    pg->add_operation(
        DATABASE_OP_GEN_QUERY_TICKET_SETUP,
        function<error(plugin_context&,const char*,const char*)>(
            db_gen_query_ticket_setup_op ) );
    pg->add_operation(
        DATABASE_OP_GET_DISTINCT_DATA_OBJ_COUNT_ON_RESOURCE,
        function<error(plugin_context&,const char*,long long*)>(
            db_get_distinct_data_obj_count_on_resource_op ) );
    pg->add_operation(
        DATABASE_OP_GET_DISTINCT_DATA_OBJS_MISSING_FROM_CHILD_GIVEN_PARENT,
        function<error(plugin_context&,const string*, const string*, int, dist_child_result_t*)>(
            db_get_distinct_data_objs_missing_from_child_given_parent_op ) );
    pg->add_operation(
        DATABASE_OP_GET_REPL_LIST_FOR_LEAF_BUNDLES,
        function<error(plugin_context&,rodsLong_t,size_t,const std::vector<leaf_bundle_t>*,const std::string*,dist_child_result_t*)>(
            db_get_repl_list_for_leaf_bundles_op));
    pg->add_operation(
        DATABASE_OP_CHECK_PERMISSION_TO_MODIFY_DATA_OBJECT,
        function<error(plugin_context&,const rodsLong_t)>(
            db_check_permission_to_modify_data_object_op));
    pg->add_operation(
        DATABASE_OP_UPDATE_TICKET_WRITE_BYTE_COUNT,
        function<error(plugin_context&,const rodsLong_t,const rodsLong_t)>(
            db_update_ticket_write_byte_count_op));
    pg->add_operation<const char*, std::vector<std::string>*>(
        DATABASE_OP_GET_DELAY_RULE_INFO,
        function<error(plugin_context&, const char*, std::vector<std::string>*)>(db_get_delay_rule_info_op));
    pg->add_operation<const char*>(
        DATABASE_OP_DATA_OBJECT_FINALIZE, function<error(plugin_context&, const char*)>(db_data_object_finalize_op));
    pg->add_operation<const char*, const char*, const char*, int*>(
        DATABASE_OP_CHECK_AUTH_CREDENTIALS,
        function<error(plugin_context&, const char*, const char*, const char*, int*)>(db_check_auth_credentials_op));
    pg->add_operation<const char*, const std::vector<std::string>*, char**>(
        DATABASE_OP_EXECUTE_GENQUERY2_SQL,
        function<error(plugin_context&, const char*, const std::vector<std::string>*, char**)>(
            db_execute_genquery2_sql));
    pg->add_operation<const char*, const char*, int>(
        DATABASE_OP_DELAY_RULE_LOCK,
        function<error(plugin_context&, const char*, const char*, int)>(db_delay_rule_lock));
    pg->add_operation<const char*>(
        DATABASE_OP_DELAY_RULE_UNLOCK, function<error(plugin_context&, const char*)>(db_delay_rule_unlock));
    pg->add_operation<const char*, char**>(
        DATABASE_OP_UPDATE_REPLICA_ACCESS_TIME,
        function<error(plugin_context&, const char*, char**)>(db_update_replica_access_time));

    return pg;
} // plugin_factory
