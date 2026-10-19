#include "utility.hpp"
#include <irods/genquery2_builder.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/rods.h>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>

#include <nlohmann/json.hpp>

#include <cstdio>

int debug = 0;

rcComm_t *Conn;
rodsEnv myEnv;

void usage();

struct userinfo_t {
    char* user_name;
    char* zone_name;
};

bool print_general_info(const userinfo_t& _info) {
    namespace gq2 = irods::experimental::genquery2;

    const auto q = gq2::builder::select({
            "USER_NAME",
            "USER_ID",
            "USER_TYPE",
            "USER_ZONE",
            "USER_INFO",
            "USER_COMMENT",
            "USER_CREATE_TIME",
            "USER_MODIFY_TIME"
        })
        .where(gq2::builder::col("USER_NAME") == _info.user_name &&
               gq2::builder::col("USER_ZONE") == _info.zone_name)
        .build();

    nlohmann::json rows;
    if (gq2::try_execute(*Conn, q, rows) < 0 || rows.empty()) {
        return false;
    }

    const std::vector<std::string> general_info_labels{
        "name", "id", "type", "zone", "info", "comment", "create time", "modify time"};
    int i{};
    for (const auto& cell : rows.front()) {
        const auto& val = cell.get_ref<const std::string&>();
        if (std::string::npos != (general_info_labels[i].find("time"))) {
            char local_time[TIME_LEN]{};
            getLocalTimeFromRodsTime(val.c_str(), local_time);
            printf("%s: %s: %s\n", general_info_labels[i].c_str(), val.c_str(), local_time);
        }
        else {
            printf("%s: %s\n", general_info_labels[i].c_str(), val.c_str());
        }
        i++;
    }
    return true;
}

void print_auth_info(const userinfo_t& _info) {
    namespace gq2 = irods::experimental::genquery2;

    const auto q = gq2::builder::select({"USER_DN"})
        .where(gq2::builder::col("USER_NAME") == _info.user_name &&
               gq2::builder::col("USER_ZONE") == _info.zone_name)
        .build();

    nlohmann::json rows;
    if (gq2::try_execute(*Conn, q, rows) < 0) {
        return;
    }

    for (const auto& row : rows) {
        printf("GSI DN or Kerberos Principal Name: %s\n", row[0].get_ref<const std::string&>().c_str());
    }
}

void print_group_info(const userinfo_t& _info) {
    namespace gq2 = irods::experimental::genquery2;

    const auto q = gq2::builder::select({"USER_GROUP_NAME"})
        .where(gq2::builder::col("USER_NAME") == _info.user_name &&
               gq2::builder::col("USER_ZONE") == _info.zone_name)
        .build();

    nlohmann::json rows;
    if (gq2::try_execute(*Conn, q, rows) < 0 || rows.empty()) {
        printf("Not a member of any group\n");
        return;
    }

    for (const auto& row : rows) {
        printf("member of group: %s\n", row[0].get_ref<const std::string&>().c_str());
    }
}

int
showUser(const char *name) {
    char user_name[NAME_LEN]{};
    char zone_name[NAME_LEN]{};
    int status = parseUserName(name, user_name, zone_name);
    if (status < 0) {
        printf("Failed parsing input:[%s]\n", name);
        return status;
    }
    if (std::string(zone_name).empty()) {
        snprintf(zone_name, sizeof(zone_name), "%s", myEnv.rodsZone);
    }
    const userinfo_t info{user_name, zone_name};

    if (!print_general_info(info)) {
        printf("User %s#%s does not exist.\n", info.user_name, info.zone_name);
        return 0;
    }
    print_auth_info(info);
    print_group_info(info);

    return 0;
}

int
main( int argc, char **argv ) {

    signal( SIGPIPE, SIG_IGN );

    int status, nArgs;
    rErrMsg_t errMsg;

    rodsArguments_t myRodsArgs;

    rodsLogLevel( LOG_ERROR );

    status = parseCmdLineOpt( argc, argv, "vVh", 0, &myRodsArgs );
    if ( status ) {
        printf( "Use -h for help.\n" );
        exit( 1 );
    }
    if ( myRodsArgs.help == True ) {
        usage();
        exit( 0 );
    }

    status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "main: getRodsEnv error. status = %d",
                 status );
        exit( 1 );
    }

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::api_entry_table&  api_tbl = irods::get_client_api_table();
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    init_api_table( api_tbl, pk_tbl );

    Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );

    if ( Conn == NULL ) {
        exit( 2 );
    }

    const auto disconnect = irods::at_scope_exit{[] { rcDisconnect(Conn); }};

    utils::warn_if_connected_to_potentially_incompatible_server(*Conn);

    status = utils::authenticate_client(Conn, myEnv);
    if ( status != 0 ) {
        print_error_stack_to_file(Conn->rError, stderr);
        if ( !debug ) {
            return 3;
        }
    }

    nArgs = argc - myRodsArgs.optind;

    if ( nArgs == 1 ) {
        status = showUser(argv[myRodsArgs.optind]);
    }
    else {
        const std::string user_name = (boost::format("%s#%s") %
                                       myEnv.rodsUserName % myEnv.rodsZone).str();
        status = showUser(user_name.c_str());
    }

    printErrorStack( Conn->rError );

    return status;
}

/*
Print the main usage/help information.
 */
void usage() {
    char *msgs[] = {
        "Usage: iuserinfo [-vVh] [user]",
        " ",
        "Show information about your iRODS user account or the entered user",
        ""
    };
    int i;
    for ( i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            break;
        }
        printf( "%s\n", msgs[i] );
    }
    printReleaseInfo( "iuserinfo" );
}
