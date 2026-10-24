#include "utility.hpp"
#include <irods/genquery2_builder.hpp>
#include <irods/irods_at_scope_exit.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/irods_random.hpp>
#include <irods/rcMisc.h>
#include <irods/rods.h>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>
#include <irods/rodsPath.h>

#include <nlohmann/json.hpp>

#include <charconv>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <locale>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#define MAX_SQL 300
#define BIG_STR 3000

extern int get64RandomBytes( char *buf );

char cwd[BIG_STR];

int debug = 0;
int longMode = 0; /* more detailed listing */

char zoneArgument[MAX_NAME_LEN + 2] = "";

rcComm_t *Conn;
rodsEnv myEnv;

int lastCommandStatus = 0;
int printCount = 0;
int printedRows = 0;
bool run_as_admin = false;

int usage( char *subOpt );

void showRestrictions( const char *inColumn );

std::string to_utc_timestamp(const std::string_view _seconds)
{
    std::time_t tt = 0;
    const auto [ptr, ec] = std::from_chars(_seconds.data(), _seconds.data() + _seconds.size(), tt);
    if (ec != std::errc{} || ptr != _seconds.data() + _seconds.size()) {
        std::cerr << "Timestamp conversion error: invalid integer\n";
        return "?";
    }
    std::tm utc_tm{};
    gmtime_r(&tt, &utc_tm);
    std::ostringstream ss;
    ss << std::put_time(&utc_tm, "%Y-%m-%d.%H:%M:%S");
    return ss.str();
}

/*
 print the results of a general query.
 */
void
printResultsAndSubQuery( rcComm_t *Conn, int status, const nlohmann::json& rows,
                         char *descriptions[], int subColumn, int dashOpt ) {
    lastCommandStatus = status;
    if ( status == CAT_NO_ROWS_FOUND ) {
        lastCommandStatus = 0;
    }

    if ( status != 0 && status != CAT_NO_ROWS_FOUND ) {
        printError( Conn, status, "rc_genquery2" );
    }
    else {
        if ( status == CAT_NO_ROWS_FOUND || rows.empty() ) {
            if ( printCount == 0 ) {
                printf( "No rows found\n" );
            }
        }
        else {
            for ( std::size_t i = 0; i < rows.size(); i++ ) {
                const auto& row = rows[i];
                if ( !row.is_array() ) {
                    continue;
                }
                printedRows++;
                const char *subCol = "";
                if ( i > 0 && dashOpt > 0 ) {
                    printf( "----\n" );
                }

                for ( std::size_t j = 0; j < row.size(); j++ ) {
                    const auto& val = row[j].get_ref<const std::string&>();
                    const char *tResult = val.c_str();
                    if ( subColumn >= 0 && static_cast<std::size_t>(subColumn) == j ) {
                        subCol = tResult;
                    }

                    if (*descriptions[j] != '\0') {
                        if (strstr( descriptions[j], "time") != 0) {
                            char localTime[TIME_LEN];

                            if (strcmp(tResult, "0") == 0 || *tResult == '\0') {
                                strcpy(localTime, "none");
                            }
                            else {
                                const auto ts = to_utc_timestamp(val);
                                strcpy(localTime, ts.data());
                            }

                            printf("%s: %s\n", descriptions[j], localTime);
                        }
                        else {
                            printf("%s: %s\n", descriptions[j], tResult);
                            printCount++;
                        }
                    }
                }

                if ( subColumn >= 0 ) {
                    showRestrictions( subCol );
                }
            }
        }
    }
}

void
showRestrictionsByHost( const char *inColumn ) {
    namespace gq2 = irods::experimental::genquery2;
    char *columnNames[] = {"restricted-to host"};
    printCount = 0;

    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"TICKET_ALLOWED_HOST"})
            .where(gq2::builder::col("TICKET_ALLOWED_HOST_TICKET_ID") == inColumn)
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        const int status = gq2::try_execute(*Conn, q, rows);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            printf( "No host restrictions\n" );
            return;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printResultsAndSubQuery( Conn, status, rows, columnNames, -1, 0 );
            }
            break;
        }

        printResultsAndSubQuery( Conn, status, rows, columnNames, -1, 0 );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }
}

void
showRestrictionsByUser( const char *inColumn ) {
    namespace gq2 = irods::experimental::genquery2;
    char *columnNames[] = {"restricted-to user"};
    printCount = 0;

    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"TICKET_ALLOWED_USER_NAME"})
            .where(gq2::builder::col("TICKET_ALLOWED_USER_TICKET_ID") == inColumn)
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        const int status = gq2::try_execute(*Conn, q, rows);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            printf( "No user restrictions\n" );
            return;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printResultsAndSubQuery( Conn, status, rows, columnNames, -1, 0 );
            }
            break;
        }

        printResultsAndSubQuery( Conn, status, rows, columnNames, -1, 0 );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }
}

void
showRestrictionsByGroup( const char *inColumn ) {
    namespace gq2 = irods::experimental::genquery2;
    char *columnNames[] = {"restricted-to group"};
    printCount = 0;

    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"TICKET_ALLOWED_GROUP_NAME"})
            .where(gq2::builder::col("TICKET_ALLOWED_GROUP_TICKET_ID") == inColumn)
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        const int status = gq2::try_execute(*Conn, q, rows);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            printf( "No group restrictions\n" );
            return;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printResultsAndSubQuery( Conn, status, rows, columnNames, -1, 0 );
            }
            break;
        }

        printResultsAndSubQuery( Conn, status, rows, columnNames, -1, 0 );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }
}

void
showRestrictions( const char *inColumn ) {
    showRestrictionsByHost( inColumn );
    showRestrictionsByUser( inColumn );
    showRestrictionsByGroup( inColumn );
}


/*
Via a general query, show the Tickets for this user
*/
int
showTickets1( char *inOption, char *inName ) {
    namespace gq2 = irods::experimental::genquery2;

    char *columnNames[] = {"id", "string", "ticket type", "obj type", "owner name", "owner zone", "uses count", "uses limit", "write file count", "write file limit", "write byte count", "write byte limit", "expire time", "collection name", "data collection"};

    printCount = 0;

    std::vector<std::string> select_cols{
        "TICKET_ID", "TICKET_STRING", "TICKET_TYPE", "TICKET_OBJECT_TYPE",
        "TICKET_OWNER_NAME", "TICKET_OWNER_ZONE", "TICKET_USES_COUNT", "TICKET_USES_LIMIT",
        "TICKET_WRITE_FILE_COUNT", "TICKET_WRITE_FILE_LIMIT",
        "TICKET_WRITE_BYTE_COUNT", "TICKET_WRITE_BYTE_LIMIT",
        "TICKET_EXPIRY_TS"
    };

    if ( strstr( inOption, "data" ) != 0 ) {
        columnNames[13] = "data-object name";
        select_cols.push_back("TICKET_DATA_NAME");
        select_cols.push_back("TICKET_DATA_COLL_NAME");
    }
    else if ( strstr( inOption, "basic" ) == 0 ) {
        select_cols.push_back("TICKET_COLL_NAME");
    }

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        auto qb = gq2::builder::select(select_cols);
        if ( inName != NULL && *inName != '\0' ) {
            const auto col_name = (isInteger( inName ) == 1) ? "TICKET_ID" : "TICKET_STRING";
            qb.where(gq2::builder::col(col_name) == inName);
        }
        qb.limit(maxRows, offset);

        nlohmann::json rows;
        const int status = gq2::try_execute(*Conn, qb.build(), rows, zone_sv);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            lastCommandStatus = 0;
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printResultsAndSubQuery( Conn, status, rows, columnNames, 0, 1 );
            }
            break;
        }
        if ( offset > 0 ) {
            printf( "----\n" );
        }
        printResultsAndSubQuery( Conn, status, rows, columnNames, 0, 1 );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    return 0;
}

void
showTickets( char *inName ) {
    printedRows = 0;
    showTickets1( "data", inName );
    if ( printedRows > 0 ) {
        printf( "----\n" );
    }
    showTickets1( "collection", inName );
    if ( printedRows == 0 ) {
        /* try a more basic query in case the data obj or collection is gone */
        showTickets1( "basic", inName );
        if ( printedRows > 0 &&
                inName != NULL && *inName != '\0' ) {
            printf( "Warning: the data-object or collection for this ticket no longer exists\n" );
        }
    }
}

auto canonical(const char *inName) -> std::optional<std::string>
{
    char out_path[MAX_NAME_LEN]{};
    if (parseRodsPathStr(inName, &myEnv, out_path) != 0) {
        return std::nullopt;
    }

    auto* escaped_path = escape_path(out_path);
    std::optional<std::string> p = escaped_path;
    std::free(escaped_path);

    return p;
}

/*
 Create, modify, or delete a ticket
 */
int
doTicketOp( const char *arg1, const char *arg2, const char *arg3,
            const char *arg4, const char *arg5 ) {
    ticketAdminInp_t ticketAdminInp{};

    ticketAdminInp.arg1 = strdup( arg1 );
    ticketAdminInp.arg2 = strdup( arg2 );
    ticketAdminInp.arg3 = strdup( arg3 );
    ticketAdminInp.arg4 = strdup( arg4 );
    ticketAdminInp.arg5 = strdup( arg5 );
    ticketAdminInp.arg6 = "";

    if (run_as_admin) {
        addKeyVal(&ticketAdminInp.condInput, ADMIN_KW, "");
    }

    const int status = rcTicketAdmin( Conn, &ticketAdminInp );
    lastCommandStatus = status;

    free( ticketAdminInp.arg1 );
    free( ticketAdminInp.arg2 );
    free( ticketAdminInp.arg3 );
    free( ticketAdminInp.arg4 );
    free( ticketAdminInp.arg5 );

    if ( status < 0 ) {
        if ( Conn->rError ) {
            rError_t *Err;
            rErrMsg_t *ErrMsg;
            int i, len;
            Err = Conn->rError;
            len = Err->len;
            for ( i = 0; i < len; i++ ) {
                ErrMsg = Err->errMsg[i];
                rodsLog( LOG_ERROR, "Level %d: %s", i, ErrMsg->msg );
            }
        }
        char *mySubName = NULL;
        const char *myName = rodsErrorName( status, &mySubName );
        rodsLog( LOG_ERROR, "rcTicketAdmin failed with error %d %s %s",
                 status, myName, mySubName );
        free( mySubName );
    }

    return status;
}

void
makeTicket( char *newTicket ) {
    const int ticket_len = 15;
    // random_bytes must be (unsigned char[]) to guarantee that following
    // modulo result is positive (i.e. in [0, 61])
    unsigned char random_bytes[ticket_len];
    irods::getRandomBytes( random_bytes, ticket_len );

    const char characterSet[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G',
    'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
    'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
    'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
    'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6',
    '7', '8', '9'};

    for ( int i = 0; i < ticket_len; ++i ) {
        const int ix = random_bytes[i] % sizeof(characterSet);
        newTicket[i] = characterSet[ix];
    }
    newTicket[ticket_len] = '\0';
    printf( "ticket:%s\n", newTicket );
}

/*
 Prompt for input and parse into tokens
*/
void
getInput( char *cmdToken[], int maxTokens ) {
    int lenstr, i;
    static char ttybuf[BIG_STR];
    int nTokens;
    int tokenFlag; /* 1: start reg, 2: start ", 3: start ' */
    char *cpTokenStart;
    char *stat;

    memset( ttybuf, 0, BIG_STR );
    fputs( "iticket>", stdout );
    stat = fgets( ttybuf, BIG_STR, stdin );
    if ( stat == 0 ) {
        printf( "\n" );
        rcDisconnect( Conn );
        if ( lastCommandStatus != 0 ) {
            exit( 4 );
        }
        exit( 0 );
    }
    lenstr = strlen( ttybuf );
    for ( i = 0; i < maxTokens; i++ ) {
        cmdToken[i] = "";
    }
    cpTokenStart = ttybuf;
    nTokens = 0;
    tokenFlag = 0;
    for ( i = 0; i < lenstr; i++ ) {
        if (nTokens >= maxTokens) {
            fprintf(stderr, "Too many command line arguments.\n");
            return;
        }
        if ( ttybuf[i] == '\n' ) {
            ttybuf[i] = '\0';
            cmdToken[nTokens++] = cpTokenStart;
            return;
        }
        if ( tokenFlag == 0 ) {
            if ( ttybuf[i] == '\'' ) {
                tokenFlag = 3;
                cpTokenStart++;
            }
            else if ( ttybuf[i] == '"' ) {
                tokenFlag = 2;
                cpTokenStart++;
            }
            else if ( ttybuf[i] == ' ' ) {
                cpTokenStart++;
            }
            else {
                tokenFlag = 1;
            }
        }
        else if ( tokenFlag == 1 ) {
            if ( ttybuf[i] == ' ' ) {
                ttybuf[i] = '\0';
                cmdToken[nTokens++] = cpTokenStart;
                cpTokenStart = &ttybuf[i + 1];
                tokenFlag = 0;
            }
        }
        else if ( tokenFlag == 2 ) {
            if ( ttybuf[i] == '"' ) {
                ttybuf[i] = '\0';
                cmdToken[nTokens++] = cpTokenStart;
                cpTokenStart = &ttybuf[i + 1];
                tokenFlag = 0;
            }
        }
        else if ( tokenFlag == 3 ) {
            if ( ttybuf[i] == '\'' ) {
                ttybuf[i] = '\0';
                cmdToken[nTokens++] = cpTokenStart;
                cpTokenStart = &ttybuf[i + 1];
                tokenFlag = 0;
            }
        }
    }
}


/* handle a command,
   return code is 0 if the command was (at least partially) valid,
   -1 for quitting,
   -2 for if invalid
   -3 if empty.
 */
int
doCommand( char *cmdToken[] ) {
    if ( strcmp( cmdToken[0], "help" ) == 0 ||
            strcmp( cmdToken[0], "h" ) == 0 ) {
        usage( cmdToken[1] );
        return 0;
    }
    if ( strcmp( cmdToken[0], "quit" ) == 0 ||
            strcmp( cmdToken[0], "q" ) == 0 ) {
        return -1;
    }

    if ( strcmp( cmdToken[0], "create" ) == 0
            || strcmp( cmdToken[0], "make" ) == 0
            || strcmp( cmdToken[0], "mk" ) == 0
       ) {
        char myTicket[30];
        if ( strlen( cmdToken[3] ) > 0 ) {
            snprintf( myTicket, sizeof( myTicket ), "%s", cmdToken[3] );
        }
        else {
            makeTicket( myTicket );
        }

        auto fullPath = canonical(cmdToken[2]);
        if (!fullPath.has_value()) {
            return -2;
        }

        doTicketOp( "create", myTicket, cmdToken[1], fullPath->c_str(),
                    cmdToken[3] );
        return 0;
    }


    if ( strcmp( cmdToken[0], "delete" ) == 0 ) {
        doTicketOp( "delete", cmdToken[1], cmdToken[2],
                    cmdToken[3], cmdToken[4] );
        return 0;
    }


    if ( strcmp( cmdToken[0], "mod" ) == 0 ) {
        doTicketOp( "mod", cmdToken[1], cmdToken[2],
                    cmdToken[3], cmdToken[4] );
        return 0;
    }

    if ( strcmp( cmdToken[0], "ls" ) == 0 ) {
        showTickets( cmdToken[1] );
        return 0;
    }

    if ( strcmp( cmdToken[0], "ls-all" ) == 0 ) {
        printf( "Listing all of your tickets, even those for which the target collection\nor data-object no longer exists:\n" );
        showTickets1( "basic", "" );
        return 0;
    }

    if ( *cmdToken[0] != '\0' ) {
        printf( "unrecognized command, try 'help'\n" );
        return -2;
    }
    return -3;
}

int
main( int argc, char **argv ) {

    signal( SIGPIPE, SIG_IGN );

    int status, i, j;
    rErrMsg_t errMsg;

    rodsArguments_t myRodsArgs;

    char *mySubName;

    int argOffset;

    int maxCmdTokens = 20;
    char *cmdToken[20];
    int keepGoing;
    int firstTime;

    rodsLogLevel( LOG_ERROR );

    status = parseCmdLineOpt( argc, argv, "vVMhgrcGRCdulz:", 0, &myRodsArgs );
    if ( status ) {
        printf( "Use -h for help.\n" );
        exit( 1 );
    }
    if ( myRodsArgs.help == True ) {
        usage( "" );
        exit( 0 );
    }

    if (myRodsArgs.admin == True) {
        run_as_admin = true;
    }

    if ( myRodsArgs.zone == True ) {
        strncpy( zoneArgument, myRodsArgs.zoneName, MAX_NAME_LEN );
    }

    if ( myRodsArgs.longOption ) {
        longMode = 1;
    }

    argOffset = myRodsArgs.optind;

    if (argc - argOffset > maxCmdTokens) {
        fprintf(stderr, "Too many command line arguments.\n");
        return 1;
    }
    if ( argOffset > 1 ) {
        if ( argOffset > 2 ) {
            if ( *argv[1] == '-' && *( argv[1] + 1 ) == 'z' ) {
                if ( *( argv[1] + 2 ) == '\0' ) {
                    argOffset = 3; /* skip -z zone */
                }
                else {
                    argOffset = 2; /* skip -zzone */
                }
            }
            else {
                argOffset = 1; /* Ignore the parseCmdLineOpt parsing
			    as -d etc handled  below*/
            }
        }
        else {
            argOffset = 1; /* Ignore the parseCmdLineOpt parsing
			 as -d etc handled  below*/
        }
    }

    status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "main: getRodsEnv error. status = %d",
                 status );
        exit( 1 );
    }
    strncpy( cwd, myEnv.rodsCwd, BIG_STR );
    if ( strlen( cwd ) == 0 ) {
        strcpy( cwd, "/" );
    }

    for ( i = 0; i < maxCmdTokens; i++ ) {
        cmdToken[i] = "";
    }
    j = 0;
    for ( i = argOffset; i < argc; i++ ) {
        // Skip the admin option.
        if (std::strcmp(argv[i], "-M") != 0) {
            cmdToken[j++] = argv[i];
        }
    }

#if defined(linux_platform)
    /*
      imeta cp -d TestFile1 -d TestFile3
      comes in as:     -d -d cp TestFile1 TestFile3
      so switch it to: cp -d -d TestFile1 TestFile3
    */
    if ( cmdToken[0] != NULL && *cmdToken[0] == '-' ) {
        /* args were toggled, switch them back */
        if ( cmdToken[1] != NULL && *cmdToken[1] == '-' ) {
            cmdToken[0] = argv[argOffset + 2];
            cmdToken[1] = argv[argOffset];
            cmdToken[2] = argv[argOffset + 1];
        }
        else {
            cmdToken[0] = argv[argOffset + 1];
            cmdToken[1] = argv[argOffset];
        }
    }
#else
    /* tested on Solaris, not sure other than Linux/Solaris */
    /*
      imeta cp -d TestFile1 -d TestFile3
      comes in as:     cp -d TestFile1 -d TestFile3
      so switch it to: cp -d -d TestFile1 TestFile3
    */
    if ( cmdToken[0] != NULL && cmdToken[1] != NULL && *cmdToken[1] == '-' &&
            cmdToken[2] != NULL && cmdToken[3] != NULL && *cmdToken[3] == '-' ) {
        /* two args */
        cmdToken[2] = argv[argOffset + 3];
        cmdToken[3] = argv[argOffset + 2];
    }

#endif

    if ( strcmp( cmdToken[0], "help" ) == 0 ||
            strcmp( cmdToken[0], "h" ) == 0 ) {
        usage( cmdToken[1] );
        exit( 0 );
    }

    if ( strcmp( cmdToken[0], "spass" ) == 0 ) {
        char scrambled[MAX_PASSWORD_LEN + 100];
        if ( strlen( cmdToken[1] ) > MAX_PASSWORD_LEN - 2 ) {
            printf( "Password exceeds maximum length\n" );
        }
        else {
            obfEncodeByKey( cmdToken[1], cmdToken[2], scrambled );
            printf( "Scrambled form is:%s\n", scrambled );
        }
        exit( 0 );
    }

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::api_entry_table&  api_tbl = irods::get_client_api_table();
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    init_api_table( api_tbl, pk_tbl );

    Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );

    if ( Conn == NULL ) {
        const char *myName = rodsErrorName( errMsg.status, &mySubName );
        rodsLog( LOG_ERROR, "rcConnect failure %s (%s) (%d) %s",
                 myName,
                 mySubName,
                 errMsg.status,
                 errMsg.msg );

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

    keepGoing = 1;
    firstTime = 1;
    while ( keepGoing ) {
        int status;
        status = doCommand( cmdToken );
        if ( status == -1 ) {
            keepGoing = 0;
        }
        if ( firstTime ) {
            if ( status == 0 ) {
                keepGoing = 0;
            }
            if ( status == -2 ) {
                keepGoing = 0;
                lastCommandStatus = -1;
            }
            firstTime = 0;
        }
        if ( keepGoing ) {
            getInput( cmdToken, maxCmdTokens );
        }
    }

    printErrorStack( Conn->rError );

    if ( lastCommandStatus != 0 ) {
        return 4;
    }
    return 0;
}

/*
Print the main usage/help information.
 */
void usageMain() {
    char *msgs[] = {
        "Usage: iticket [-h] [-M] [command]",
        " -h This help",
        " -M Run as administrator",
        " ",
        "Commands are:",
        " create read/write Object-Name [string] (create a new ticket)",
        " mod Ticket_string-or-id uses/expire string-or-none  (modify restrictions)",
        " mod Ticket_string-or-id write-bytes-or-file number-or-0 (modify restrictions)",
        " mod Ticket_string-or-id add/remove host/user/group string (modify restrictions)",
        " ls [Ticket_string-or-id] (non-admins will see just your own)",
        " ls-all (list all your tickets, even with missing targets)",
        " delete ticket_string-or-id",
        " quit",
        " ",
        "Tickets are another way to provide access to iRODS data-objects (files) or",
        "collections (directories or folders).  The 'iticket' command allows you",
        "to create, modify, list, and delete tickets.  When you create a ticket",
        "its 15 character string is given to you which you can share with others.",
        "This is less secure than normal iRODS login-based access control, but",
        "is useful in some situations.  See the 'ticket-based access' page on",
        "irods.org for more information.",
        " ",
        "A blank execute line invokes the interactive mode, where iticket",
        "prompts and executes commands until 'quit' or 'q' is entered.",
        "Like other unix utilities, a series of commands can be piped into it:",
        "'cat file1 | iticket' (maintaining one connection for all commands).",
        " ",
        "Admin users can pass -M to modify/delete tickets created by other users.",
        " ",
        "Use 'help command' for more help on a specific command.",
        ""
    };
    int i;
    for ( i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            break;
        }
        printf( "%s\n", msgs[i] );
    }
    printReleaseInfo( "iticket" );
}

/*
Print either main usage/help information, or some more specific
information on particular commands.
 */
int
usage( char *subOpt ) {
    int i;
    if ( *subOpt == '\0' ) {
        usageMain();
    }
    else {
        if ( strcmp( subOpt, "create" ) == 0 ) {
            char *msgs[] = {
                " create read/write Object-Name [string] (create a new ticket)",
                "Create a new ticket for Object-Name, which is either a data-object (file)",
                "or a collection (directory). ",
                "Example: create read myFile",
                "The ticket string, which can be used for access, will be displayed.",
                "If 'string' is provided on the command line, it is the ticket-string to use",
                "as the ticket instead of a randomly generated string of characters.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "mod" ) == 0 ) {
            char *msgs[] = {
                "   mod Ticket-id uses/expire string-or-none",
                "or mod Ticket-id add/remove host/user/group string (modify restrictions)",
                "Modify a ticket to use one of the specialized options.  By default a",
                "ticket can be used by anyone (and 'anonymous'), from any host, and any",
                "number of times, and for all time (until deleted).  You can modify it to",
                "add (or remove) these types of restrictions.",
                " ",
                " 'mod Ticket-id uses integer-or-0' will make the ticket only valid",
                "the specified number of times.  Use 0 to remove this restriction.",
                " ",
                " 'mod Ticket-id write-file integer-or-0' will make the write-ticket only",
                "valid for writing the specified number of times.  Use 0 to remove this",
                "restriction.",
                " ",
                " 'mod Ticket-id write-byte integer-or-0' will make the write-ticket only",
                "valid for writing the specified number of bytes.  Use 0 to remove this",
                "restriction.",
                " ",
                " 'mod Ticket-id add/remove user Username' will make the ticket only valid",
                "when used by that particular iRODS user.  You can use multiple mod commands",
                "to add more users to the allowed list.",
                " ",
                " 'mod Ticket-id add/remove group Groupname' will make the ticket only valid",
                "when used by iRODS users in that particular iRODS group.  You can use",
                "multiple mod commands to add more groups to the allowed list.",
                " ",
                " 'mod Ticket-id add/remove host Host/IP' will make the ticket only valid",
                "when used from that particular host computer.  Host (full DNS name) will be",
                "converted to the IP address for use in the internal checks or you can enter",
                "the IP address itself.  'iticket ls' will display the IP addresses.",
                "You can use multiple mod commands to add more hosts to the list.",
                " ",
                " 'mod Ticket-id expire date.time-or-0' will make the ticket only valid",
                "before the specified date-time.  You can cancel this expiration by using",
                "'0'.  The time is year-mo-da.hr:min:sec, for example: 2012-05-07.23:00:00",
                " ",
                " The Ticket-id is either the ticket object number or the ticket-string",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "delete" ) == 0 ) {
            char *msgs[] = {
                " delete Ticket-string",
                "Remove a ticket from the system.  Access will no longer be allowed",
                "via the ticket-string.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }

        if ( strcmp( subOpt, "ls" ) == 0 ) {
            char *msgs[] = {
                " ls [Ticket_string-or-id]",
                "List the tickets owned by you or, for admin users, all tickets.",
                "Include a ticket-string or the ticket-id (object number) to list only one",
                "(in this case, a numeric string is assumed to be an id).",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }

        if ( strcmp( subOpt, "ls-all" ) == 0 ) {
            char *msgs[] = {
                " ls-all",
                "Similar to 'ls' (with no ticket string-or-id) but will list all of your",
                "tickets even if the target collection or data-object no longer exists.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }

        if ( strcmp( subOpt, "quit" ) == 0 ) {
            char *msgs[] = {
                " Exits the interactive mode",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }

        printf( "Sorry, either %s is an invalid command or the help has not been written yet\n",
                subOpt );
    }
    return 0;
}
