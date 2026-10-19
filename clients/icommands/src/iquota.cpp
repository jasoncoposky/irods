#include "utility.hpp"
#include <irods/genquery2_builder.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/rods.h>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>

#include <nlohmann/json.hpp>

#include <charconv>
#include <cstdio>
#include <string>
#include <vector>

#define BIG_STR 200
#define QUOTA_APPROACH_WARNING_SIZE -10000000000LL

int debug = 0;
char quotaTime[TIME_LEN] = "";
int printedFlag[3] = {0, 0, 0};
rcComm_t *Conn;
rodsEnv myEnv;

void usage();


/*
 print a number nicely, that is with commas and summary magnitude
 */
int
printNice( const char *inArg, int minLen, const char *Units ) {
    int i, n, k, len;
    int nextComma, firstComma, commaCount;
    char niceString[40];
    char firstPart[10];
    const char *numberName;
    char blanks[] = "                                        ";
    len = strlen( inArg );
    nextComma = len % 3;
    if ( inArg[0] == '-' ) {
        nextComma = ( len - 1 ) % 3;
    }
    if ( nextComma == 0 ) {
        nextComma = 3;
    }
    n = 0;
    k = 0;
    firstComma = 0;
    commaCount = 0;
    for ( i = 0; i < len; i++ ) {
        niceString[n++] = inArg[i];
        if ( firstComma == 0 ) {
            firstPart[k++] = inArg[i];
        }
        if ( inArg[i] != '-' ) {
            nextComma--;
        }
        if ( nextComma == 0 && i < len - 1 ) {
            niceString[n++] = ',';
            nextComma = 3;
            firstComma = 1;
            commaCount++;
        }
    }
    niceString[n] = '\0';
    firstPart[k] = '\0';
    numberName = "";
    if ( commaCount == 1 ) {
        numberName = "thousand";
    }
    if ( commaCount == 2 ) {
        numberName = "million";
    }
    if ( commaCount == 3 ) {
        numberName = "billion";
    }
    if ( commaCount == 4 ) {
        numberName = "trillion";
    }
    if ( commaCount == 5 ) {
        numberName = "quadrillion";
    }
    if ( commaCount == 6 ) {
        numberName = "quintillion";
    }
    if ( commaCount == 7 ) {
        numberName = "sextillion";
    }
    if ( commaCount == 8 ) {
        numberName = "septillion";
    }
    if ( commaCount > 8 ) {
        numberName = "very many";
    }
    len = strlen( niceString );
    if ( len < minLen ) {
        i = minLen - len;
        blanks[i] = '\0';
        printf( "%s", blanks );
        blanks[i] = ' ';
    }
    if ( commaCount == 0 ) {
        printf( "%s %s", niceString, Units );
    }
    else {
        printf( "%s (%s %s) %s", niceString,
                firstPart, numberName, Units );
    }
    return 0;
}



/*
  Show user quota information
*/
int
showQuotas( char *userName, int userOrGroup, int rescOrGlobal ) {
    namespace gq2 = irods::experimental::genquery2;

    int status;
    long long localiTime = 0;
    const char *colName[6];

    colName[0] = "Resource: ";
    colName[1] = ( userOrGroup == 0 ) ? "User:  " : "Group:  ";
    colName[2] = "Zone:  ";
    colName[3] = "Quota: ";
    colName[4] = "Over:  ";
    colName[5] = "Time";

    std::vector<std::string> select_cols{
        ( rescOrGlobal == 0 ) ? "QUOTA_RESC_NAME" : "QUOTA_RESC_ID",
        "QUOTA_USER_NAME",
        "QUOTA_USER_ZONE",
        "QUOTA_LIMIT",
        "QUOTA_OVER",
        "QUOTA_MODIFY_TIME"
    };

    auto cond = ( userOrGroup == 0 )
        ? ( gq2::builder::col("QUOTA_USER_TYPE") != "rodsgroup" )
        : ( gq2::builder::col("QUOTA_USER_TYPE") == "rodsgroup" );

    char userName2[NAME_LEN];
    char userZone[NAME_LEN];
    if ( userName[0] != '\0' ) {
        status = parseUserName( userName, userName2, userZone );
        if ( status < 0 ) {
            rodsLog( LOG_ERROR, "parseUserName error in showQuotas with status %d", status );
            return status;
        }
        if ( userZone[0] == '\0' ) {
            cond = cond && ( gq2::builder::col("QUOTA_USER_NAME") == userName );
        }
        else {
            cond = cond && ( gq2::builder::col("QUOTA_USER_NAME") == userName2 ) &&
                   ( gq2::builder::col("QUOTA_USER_ZONE") == userZone );
        }
    }

    if ( rescOrGlobal == 1 ) {
        cond = cond && ( gq2::builder::col("QUOTA_RESC_ID") == "0" );
    }

    const auto q = gq2::builder::select(std::move(select_cols))
        .where(std::move(cond))
        .limit(MAX_SQL_ROWS)
        .build();

    nlohmann::json rows;
    status = gq2::try_execute(*Conn, q, rows);
    if ( status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty()) ) {
        printf( "None\n\n" );
        return 0;
    }
    if ( status != 0 ) {
        printError( Conn, status, "rc_genquery2" );
        return status;
    }

    for ( const auto& row : rows ) {
        if ( !row.is_array() || row.size() < 6 ) {
            continue;
        }
        for ( std::size_t j = 0; j < 6; j++ ) {
            const auto& val = row[j].get_ref<const std::string&>();
            const char *tResult = val.c_str();
            if ( j == 5 ) {
                long long itime = 0;
                std::from_chars( val.data(), val.data() + val.size(), itime );
                if ( itime > localiTime ) {
                    localiTime = itime;
                    getLocalTimeFromRodsTime( tResult, quotaTime );
                }
            }
            else {
                printf( "  %s", colName[j] );
                if ( rescOrGlobal == 1 && j == 0 ) {
                    tResult = "All";
                }
                if ( j == 4 || j == 3 ) {
                    printNice( tResult, 0, "bytes" );
                    if ( strncmp( colName[j], "Over:", 5 ) == 0 ) {
                        rodsLong_t ival = 0;
                        std::from_chars( val.data(), val.data() + val.size(), ival );
                        if ( ival > 0 ) {
                            printf( " OVER QUOTA" );
                        }
                        else {
                            if ( ival > QUOTA_APPROACH_WARNING_SIZE ) {
                                printf( " (Nearing quota)" );
                            }
                            else {
                                printf( " (under quota)" );
                            }
                        }

                    }
                    printf( "\n" );
                }
                else {
                    printf( "%s\n", tResult );
                }
            }
        }
        printf( "\n" );
    }
    return 0;
}

/*
  Show user quota information
*/
int
showUserUsage( char *userName, char *usersZone ) {
    namespace gq2 = irods::experimental::genquery2;

    std::size_t k = 0;
    char header[] =
        "Resource      User            Data-stored (bytes)";
    const char *pad[14] = {"             ",
                           "            ",
                           "           ",
                           "          ",
                           "         ",
                           "        ",
                           "       ",
                           "      ",
                           "     ",
                           "    ",
                           "   ",
                           "  ",
                           " ",
                           ""
                          };

    auto qb = gq2::builder::select({
        "QUOTA_USAGE_MODIFY_TIME",
        "QUOTA_RESC_NAME",
        "QUOTA_USER_NAME",
        "QUOTA_USER_ZONE",
        "QUOTA_USAGE"
    });
    if ( userName[0] != '\0' ) {
        qb.where(gq2::builder::col("QUOTA_USER_NAME") == userName);
    }
    qb.limit(MAX_SQL_ROWS);

    nlohmann::json rows;
    const int status = gq2::try_execute(*Conn, qb.build(), rows);
    if ( status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty()) ) {
        printf( "No records found, run 'iadmin cu' to calculate usage\n" );
        return 0;
    }
    if ( status != 0 ) {
        printError( Conn, status, "rc_genquery2" );
        return status;
    }

    printf( "%s\n", header );
    long long localiTime = 0;
    for ( const auto& row : rows ) {
        if ( !row.is_array() || row.size() < 5 ) {
            continue;
        }
        for ( std::size_t j = 1; j < 5; j++ ) {
            const auto& val = row[j].get_ref<const std::string&>();
            const char *tResult = val.c_str();
            if ( j == 1 ) {
                printf( "%s ", tResult );
                k = val.size();
            }
            if ( j == 2 ) {
                j++;
                const auto& val2 = row[j].get_ref<const std::string&>();
                const char *tResult2 = val2.c_str();
                if ( strncmp( tResult2, usersZone, NAME_LEN ) == 0 ) {
                    printf( "%s", tResult );
                    k = val.size();
                }
                else {
                    printf( "%s#%s", tResult, tResult2 );
                    k = val.size() + 1 + val2.size();
                }
            }
            if ( j == 4 ) {
                printNice( tResult, 14, "" );
                k = val.size();
            }
            if ( k < 14 ) {
                printf( "%s", pad[k] );
            }
        }
        printf( "\n" );

        const auto& time_val = row[0].get_ref<const std::string&>();
        long long itime = 0;
        std::from_chars( time_val.data(), time_val.data() + time_val.size(), itime );
        if ( itime > localiTime ) {
            localiTime = itime;
            getLocalTimeFromRodsTime( time_val.c_str(), quotaTime );
        }
    }
    return 0;
}

/*
  Via a general query, show user group membership
*/
int
showUserGroupMembership( char *userNameIn, char *usersZone ) {
    namespace gq2 = irods::experimental::genquery2;

    char userName[NAME_LEN];
    char zoneName[NAME_LEN];
    int showUserZone = 1;

    int status = parseUserName( userNameIn, userName, zoneName );
    if ( status ) {
        return status;
    }
    if ( zoneName[0] == '\0' ) {
        snprintf( zoneName, sizeof( zoneName ), "%s", usersZone );
        showUserZone = 0;
    }

    const auto q = gq2::builder::select({"USER_GROUP_NAME"})
        .where(gq2::builder::col("USER_NAME") == userName &&
               gq2::builder::col("USER_ZONE") == zoneName)
        .limit(MAX_SQL_ROWS)
        .build();

    nlohmann::json rows;
    status = gq2::try_execute(*Conn, q, rows);
    if ( status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty()) ) {
        printf( "Not a member of any group\n" );
        return 0;
    }
    if ( status != 0 ) {
        printError( Conn, status, "rc_genquery2" );
        return status;
    }

    int printCount = 0;
    if ( showUserZone ) {
        printf( "User %s#%s is a member of groups: ", userName, zoneName );
    }
    else {
        printf( "User %s is a member of groups: ", userName );
    }
    for ( const auto& row : rows ) {
        if ( !row.is_array() ) {
            continue;
        }
        for ( const auto& cell : row ) {
            const auto& val = cell.get_ref<const std::string&>();
            if ( printCount > 0 ) {
                printf( ", %s", val.c_str() );
            }
            else {
                printf( "%s", val.c_str() );
            }
            printCount++;
        }
    }
    printf( "\n" );
    return 0;
}


int
main( int argc, char **argv ) {

    signal( SIGPIPE, SIG_IGN );

    int status, nArgs;
    rErrMsg_t errMsg;
    char userName[NAME_LEN];

    rodsArguments_t myRodsArgs;

    rodsLogLevel( LOG_ERROR );

    status = parseCmdLineOpt( argc, argv, "au:vVh", 0, &myRodsArgs );
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

    if ( myRodsArgs.user ) {
        snprintf( userName, sizeof( userName ), "%s", myRodsArgs.userString );
    }
    else {
        snprintf( userName, sizeof( userName ), "%s", myEnv.rodsUserName );
    }

    if ( myRodsArgs.all ) {
        userName[0] = '\0';
    }

    nArgs = argc - myRodsArgs.optind;

    if ( nArgs > 0 ) {
        if ( strncmp( argv[myRodsArgs.optind], "usage", 5 ) == 0 ) {
            status = showUserUsage( userName, myEnv.rodsZone );
        }
        else {
            usage();
        }
    }
    else {
        if ( userName[0] == '\0' ) {
            printf( "Resource quotas for users:\n" );
        }
        else {
            printf( "Resource quotas for user %s:\n", userName );
        }
        status = showQuotas( userName, 0, 0 ); /* users, resc */

        if ( userName[0] == '\0' ) {
            printf( "Global (total) quotas for users:\n" );
        }
        else {
            printf( "Global (total) quotas for user %s:\n", userName );
        }
        status = showQuotas( userName, 0, 1 ); /* users, global */

        printf( "Group quotas on resources:\n" );
        status = showQuotas( "", 1, 0 );     /* all groups, resc */

        printf( "Group global (total) quotas:\n" );
        status = showQuotas( "", 1, 1 );     /* all groups, global */

        if ( userName[0] != '\0' ) {
            status = showUserGroupMembership( userName, myEnv.rodsZone );
        }
    }
    if ( quotaTime[0] != '\0' ) {
        printf( "Information was set at %s\n", quotaTime );
    }

    printErrorStack( Conn->rError );

    return 0;
}

/*
Print the main usage/help information.
 */
void usage() {
    char *msgs[] = {
        "Usage: iquota [-uavVh] [UserName] [usage]",
        " ",
        "Show information on iRODS quotas (if any).",
        "By default, information is displayed for the current iRODS user.",
        " ",
        "The 'over' values indicate whether the user or group is over quota",
        "or not and by how much; positive values are over-quota.",
        "The 'usage' information shows how much data is stored on each resource,",
        "when last determined.",
        " ",
        "Options:",
        "-a All users",
        "-u UserName[#ZoneName]  - for the specified user",
        "usage     - show usage information",
        " ",
        "Examples:",
        "iquota",
        "iquota -a",
        "iquota usage",
        "iquota -a usage",
        "iquota -u josh",
        ""
    };
    int i;
    for ( i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            break;
        }
        printf( "%s\n", msgs[i] );
    }
    printReleaseInfo( "iquota" );
}
