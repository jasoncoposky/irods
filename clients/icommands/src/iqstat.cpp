#include "utility.hpp"
#include <irods/genquery2_builder.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/rcMisc.h>
#include <irods/rods.h>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>
#include <irods/rodsLog.h>
#include <irods/user_administration.hpp>

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

#define MAX_SQL 300
#define BIG_STR 200

char cwd[BIG_STR];

int debug = 0;

rcComm_t *Conn{};
rodsEnv myEnv;

void usage();

auto throw_if_id_cannot_be_converted_to_int(const std::string& key) -> void;

auto show_RuleExec( char *userName,
                    const char *ruleName="",
                    int allFlag = false,
                    bool brief = false ) -> int;

int
main( int argc, char **argv ) {

    signal( SIGPIPE, SIG_IGN );

    rErrMsg_t errMsg;

    rodsArguments_t myRodsArgs;

    char userName[NAME_LEN];

    rodsLogLevel( LOG_ERROR );

    int status = parseCmdLineOpt( argc, argv, "alu:vVh", 0, &myRodsArgs );
    if ( status ) {
        printf( "Use -h for help\n" );
        return 1;
    }
    if ( myRodsArgs.help == True ) {
        usage();
        return 0;
    }
    status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "main: getRodsEnv error. status = %d",
                 status );
        return 1;
    }

    if ( myRodsArgs.user && myRodsArgs.all ) {
        std::fprintf(stderr, "Incompatible options: -u and -a cannot be used together\n");
        return 2;
    }

    if ( myRodsArgs.user ) {
        snprintf( userName, sizeof( userName ), "%s", myRodsArgs.userString );
    }
    else {
        snprintf( userName, sizeof( userName ), "%s", myEnv.rodsUserName );
    }

    const auto disconnect = irods::at_scope_exit{[] { rcDisconnect(Conn); }};

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::api_entry_table&  api_tbl = irods::get_client_api_table();
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    init_api_table( api_tbl, pk_tbl );

    Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );
    if (nullptr == Conn) {
        return 2;
    }

    utils::warn_if_connected_to_potentially_incompatible_server(*Conn);

    status = utils::authenticate_client(Conn, myEnv);
    if ( status != 0 ) {
        print_error_stack_to_file(Conn->rError, stderr);
        if ( !debug ) {
            return 3;
        }
    }

    try{
        int nArgs { argc - myRodsArgs.optind };
        if (nArgs > 0) {
            throw_if_id_cannot_be_converted_to_int(argv[myRodsArgs.optind]);
        }

        status = show_RuleExec( userName,
                               nArgs > 0 ? argv[myRodsArgs.optind] : "",
                               myRodsArgs.all,
                               myRodsArgs.longOption == 0 && nArgs == 0 );
    }
    catch (const irods::exception& e) {
        fprintf(stderr, "Error: %s\n", e.client_display_what());
        status = 1;
    }

    if (Conn) {
        printErrorStack( Conn->rError );
    }

    return status;
}

/*
Print the main usage/help information.
 */
void usage() {
    char *msgs[] = {
        "Usage:     iqstat [-luvVh] [-u USER] [RULE_ID]",
        " ",
        "Show information about pending iRODS rule executions (i.e. delayed rules).",
        "By default, only the invoking user's rules are shown.",
        " ",
        "Options:",
        " -a        Display requests of all users.",
        " -l        Force use of the long format.",
        " -u USER   Display requests for the specified user (may not be used together with -a).",
        " RULE_ID   Limits the query to the specified integer rule ID (and implies long format).",
        " ",
        "See also iqdel and iqmod",
        ""
    };
    int i;
    for ( i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            break;
        }
        printf( "%s\n", msgs[i] );
    }
    printReleaseInfo( "iqstat" );
}

auto show_RuleExec( char *userName,
                    const char *ruleName,
                    int allFlag,
                    bool brief ) -> int
{
    namespace gq2 = irods::experimental::genquery2;

    auto q = brief
        ? gq2::builder::select({"RULE_EXEC_ID", "RULE_EXEC_NAME"})
        : gq2::builder::select({
            "RULE_EXEC_ID",
            "RULE_EXEC_NAME",
            "RULE_EXEC_REI_FILE_PATH",
            "RULE_EXEC_USER_NAME",
            "RULE_EXEC_ADDRESS",
            "RULE_EXEC_TIME",
            "RULE_EXEC_FREQUENCY",
            "RULE_EXEC_PRIORITY",
            "RULE_EXEC_ESTIMATED_EXE_TIME",
            "RULE_EXEC_NOTIFICATION_ADDR",
            "RULE_EXEC_LAST_EXE_TIME",
            "RULE_EXEC_STATUS"
        });

    const bool has_rule_id = (ruleName != nullptr && *ruleName != '\0');
    std::string diagnostic;
    if (has_rule_id) {
        diagnostic = fmt::format( " (and matching key '{}')", ruleName );
    }

    if (!allFlag && has_rule_id) {
        q.where(gq2::builder::col("RULE_EXEC_USER_NAME") == userName &&
                gq2::builder::col("RULE_EXEC_ID") == ruleName);
    }
    else if (!allFlag) {
        q.where(gq2::builder::col("RULE_EXEC_USER_NAME") == userName);
    }
    else if (has_rule_id) {
        q.where(gq2::builder::col("RULE_EXEC_ID") == ruleName);
    }

    nlohmann::json rows;
    const int status = gq2::try_execute(*Conn, q.build(), rows);

    if (status < 0 && status != CAT_NO_ROWS_FOUND) {
        char* errno_message{};
        const char* main_message = rodsErrorName(status, &errno_message);
        std::cerr << "rcGenQuery failed with error "
                  << status << " "
                  << main_message << " "
                  << errno_message << std::endl;
        std::free(errno_message);
        return 1;
    }

    if (status == CAT_NO_ROWS_FOUND || rows.empty()) {
        // Need to determine which table "no rows" refers to: USER or RULE.
        namespace ia = irods::experimental::administration;
        if (ia::client::exists(*Conn, ia::user{userName})) {
            if ( allFlag ) {
                printf( "No delayed rules pending%s\n", diagnostic.c_str() );
            }
            else {
                printf( "No delayed rules pending for user %s%s\n", userName, diagnostic.c_str());
            }
            return 0;
        }
        else {
            printf( "User %s does not exist.\n", userName );
            return 0;
        }
    }

    if (brief) {
        printf( "id     name\n" );
        for (const auto& row : rows) {
            printf("%s %s \n",
                   row[0].get_ref<const std::string&>().c_str(),
                   row[1].get_ref<const std::string&>().c_str());
        }
        return 0;
    }

    static const char* columnNames[] = {
        "id", "name", "rei_file_path", "user_name",
        "address", "time", "frequency", "priority",
        "estimated_exe_time", "notification_addr",
        "last_exe_time", "exec_status"
    };

    for (std::size_t i = 0; i < rows.size(); ++i) {
        if (i > 0) {
            printf( "----\n" );
        }
        const auto& row = rows[i];
        for (std::size_t j = 0; j < row.size(); ++j) {
            const auto& val = row[j].get_ref<const std::string&>();
            if (std::strcmp(columnNames[j], "time") == 0) {
                char localTime[TIME_LEN]{};
                getLocalTimeFromRodsTime(val.c_str(), localTime);
                printf("%s: %s : %s\n", columnNames[j], val.c_str(), localTime);
            }
            else {
                printf("%s: %s\n", columnNames[j], val.c_str());
            }
        }
    }

    return 0;
}

auto throw_if_id_cannot_be_converted_to_int(const std::string& key) -> void
{
    std::string k{key};
    boost::algorithm::trim(k);
    try {
        std::stoul(k);
    }
    catch(const std::invalid_argument&){
        THROW(SYS_INVALID_INPUT_PARAM, "Delay rule ID has incorrect format.");
    }
    catch(const std::out_of_range&){
        THROW(SYS_INVALID_INPUT_PARAM, "Delay rule ID is too large.");
    }
    catch(const std::exception& e){
        auto message = std::string{ "Error parsing delay rule ID: "} + e.what();
        THROW(SYS_INVALID_INPUT_PARAM, message);
    }
    catch(...){
        THROW(SYS_INVALID_INPUT_PARAM, "Unknown error parsing delay rule ID.");
    }
}
