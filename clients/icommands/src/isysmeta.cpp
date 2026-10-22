#include "utility.hpp"
#include <irods/filesystem/path.hpp>
#include <irods/genquery2_builder.hpp>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/data_object_modify_info.h>

#include <nlohmann/json.hpp>

#include <charconv>
#include <cstdio>
#include <string>
#include <vector>

void usage();

/*
 print the results of a general query.
 */
int
printGenQueryResults( rcComm_t *Conn, int status, const nlohmann::json& rows,
                      char *descriptions[], int doDashes ) {
    int printCount = 0;
    char localTime[TIME_LEN];
    if ( status < 0 ) {
        if ( status != CAT_NO_ROWS_FOUND ) {
            printError( Conn, status, "rc_genquery2" );
        }
    }
    else {
        for ( std::size_t i = 0; i < rows.size(); i++ ) {
            const auto& row = rows[i];
            if ( !row.is_array() ) {
                continue;
            }
            if ( i > 0 && doDashes ) {
                printf( "----\n" );
            }
            for ( std::size_t j = 0; j < row.size(); j++ ) {
                const auto& val = row[j].get_ref<const std::string&>();
                const char* tResult = val.c_str();
                if ( descriptions != 0 && *descriptions[j] != '\0' ) {
                    if ( strstr( descriptions[j], "_ts" ) != 0 ) {
                        getLocalTimeFromRodsTime( tResult, localTime );
                        long long ts_val = 0;
                        std::from_chars( val.data(), val.data() + val.size(), ts_val );
                        if ( ts_val == 0 ) {
                            rstrcpy( localTime, "None", 20 );
                        }
                        printf( "%s: %s: %s\n", descriptions[j], tResult,
                                localTime );
                    }
                    else if ( strstr( descriptions[j], "resc_group_name" ) != 0 ) {
                        // skip resource group
                    }
                    else {
                        printf( "%s: %s\n", descriptions[j], tResult );
                    }
                }
                else {
                    printf( "%s\n", tResult );
                }
                printCount++;
            }
        }
    }
    return printCount;
}


/* perform the ls command */
int
doLs( rcComm_t *Conn, char *objPath, int longOption ) {
    namespace gq2 = irods::experimental::genquery2;
    namespace fs = irods::filesystem;

    char *columnNames[] = {
        "data_name",
        "data_id", "coll_id", "data_repl_num", "data_version",
        "data_type_name", "data_size", "resc_name",
        "data_path", "data_owner_name", "data_owner_zone",
        "data_repl_status", "data_status",
        "data_checksum", "data_expiry_ts (expire time)",
        "data_map_id", "r_comment", "create_ts", "modify_ts"
    };

    int printCount = 0;

    printf( "doing ls of %s\n", objPath );

    const fs::path p{objPath ? objPath : ""};
    const auto parent_sv = p.parent_path_view();
    const auto obj_sv = p.object_name_view();

    std::vector<std::string> select_cols{"DATA_NAME"};
    if ( longOption ) {
        select_cols.insert(select_cols.end(), {
            "DATA_ID", "DATA_COLL_ID", "DATA_REPL_NUM", "DATA_VERSION",
            "DATA_TYPE_NAME", "DATA_SIZE", "DATA_RESC_NAME",
            "DATA_PATH", "DATA_OWNER_NAME", "DATA_OWNER_ZONE",
            "DATA_REPL_STATUS", "DATA_STATUS",
            "DATA_CHECKSUM", "DATA_EXPIRY",
            "DATA_MAP_ID", "DATA_COMMENTS", "DATA_CREATE_TIME", "DATA_MODIFY_TIME"
        });
    }
    else {
        columnNames[1] = "data_expiry_ts (expire time)";
        select_cols.push_back("DATA_EXPIRY");
    }

    constexpr std::size_t maxRows = 50;
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select(select_cols)
            .where(gq2::builder::col("COLL_NAME") == parent_sv &&
                   gq2::builder::col("DATA_NAME") == obj_sv)
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        const int status = gq2::try_execute(*Conn, q, rows);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            printf( "DataObject %s does not exist.\n", objPath );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, columnNames, longOption );
            }
            break;
        }
        if ( offset > 0 && longOption ) {
            printf( "----\n" );
        }
        printCount += printGenQueryResults( Conn, status, rows, columnNames, longOption );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    static_cast<void>(printCount);

    return 1;
}

/* perform the list data-types command */
int
doListDataTypes( rcComm_t *Conn ) {
    namespace gq2 = irods::experimental::genquery2;

    int printCount = 0;

    printf( "Defined data-types:\n" );

    constexpr std::size_t maxRows = 50;
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"TOKEN_NAME"})
            .where(gq2::builder::col("TOKEN_NAMESPACE") == "data_type")
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        const int status = gq2::try_execute(*Conn, q, rows);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            printf( "None exist.\n" );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, 0, 0 );
            }
            break;
        }
        printCount += printGenQueryResults( Conn, status, rows, 0, 0 );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    static_cast<void>(printCount);

    return 1;
}


/* perform the modify command */
int
doMod( rcComm_t *Conn, char *objPath, char *time ) {
    int status;

    modDataObjMeta_t modDataObjMetaInp;
    keyValPair_t regParam;
    dataObjInfo_t dataObjInfo;

    memset( &regParam, 0, sizeof( regParam ) );
    addKeyVal( &regParam, DATA_EXPIRY_KW, time );

    memset( &dataObjInfo, 0, sizeof( dataObjInfo ) );
    rstrcpy( dataObjInfo.objPath, objPath, MAX_NAME_LEN );

    modDataObjMetaInp.regParam = &regParam;
    modDataObjMetaInp.dataObjInfo = &dataObjInfo;

    status = rc_data_object_modify_info( Conn, &modDataObjMetaInp );
    if ( status ) {
        rodsLogError( LOG_ERROR, status, "rc_data_object_modify_info failure" );
    }
    return status;
}

/* perform the modify command to change the data_type*/
int
doModDatatype( rcComm_t *Conn, char *objPath, char *dataType ) {
    int status;

    modDataObjMeta_t modDataObjMetaInp;
    keyValPair_t regParam;
    dataObjInfo_t dataObjInfo;

    memset( &regParam, 0, sizeof( regParam ) );
    addKeyVal( &regParam, DATA_TYPE_KW, dataType );

    memset( &dataObjInfo, 0, sizeof( dataObjInfo ) );
    rstrcpy( dataObjInfo.objPath, objPath, MAX_NAME_LEN );

    modDataObjMetaInp.regParam = &regParam;
    modDataObjMetaInp.dataObjInfo = &dataObjInfo;

    status = rc_data_object_modify_info( Conn, &modDataObjMetaInp );
    if ( status ) {
        rodsLogError( LOG_ERROR, status, "rc_data_object_modify_info failure" );
    }
    return status;
}

/* perform the modify command to change the comment*/
int
doModComment( rcComm_t *Conn, char *objPath, int numRepl, char *theComment ) {
    int status;

    modDataObjMeta_t modDataObjMetaInp;
    keyValPair_t regParam;
    dataObjInfo_t dataObjInfo;

    memset( &regParam, 0, sizeof( regParam ) );
    addKeyVal( &regParam, DATA_COMMENTS_KW, theComment );

    memset( &dataObjInfo, 0, sizeof( dataObjInfo ) );
    rstrcpy( dataObjInfo.objPath, objPath, MAX_NAME_LEN );

    if ( numRepl > 0 ) {
        dataObjInfo.replNum = numRepl;
    }
    else {
        addKeyVal( &regParam, ALL_KW, theComment );
    }

    modDataObjMetaInp.regParam = &regParam;
    modDataObjMetaInp.dataObjInfo = &dataObjInfo;

    status = rc_data_object_modify_info( Conn, &modDataObjMetaInp );
    if ( status ) {
        rodsLogError( LOG_ERROR, status, "rc_data_object_modify_info failure" );
    }
    return status;
}

int
main( int argc, char **argv ) {

    signal( SIGPIPE, SIG_IGN );

    int status;
    rodsArguments_t myRodsArgs;
    rodsEnv myEnv;
    int i, j, didOne;
    char objPath[MAX_NAME_LEN];
    int maxCmdTokens = 20;
    char *cmdToken[20];
    int argOffset;
    rcComm_t *Conn;
    rErrMsg_t errMsg;

    status = parseCmdLineOpt( argc, argv, "lvVh", 0, &myRodsArgs );
    if ( status ) {
        printf( "Use -h for help\n" );
        exit( 1 );
    }

    if ( myRodsArgs.help == True ) {
        usage();
        exit( 0 );
    }

    argOffset = myRodsArgs.optind;

    if (argc - argOffset > maxCmdTokens) {
        fprintf(stderr, "Too many command line arguments.\n");
        return 1;
    }

    status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "main: getRodsEnv error. status = %d",
                 status );
        exit( 1 );
    }
    snprintf( objPath, sizeof( objPath ), "%s", myEnv.rodsCwd );

    for ( i = 0; i < maxCmdTokens; i++ ) {
        cmdToken[i] = "";
    }
    j = 0;
    for ( i = argOffset; i < argc; i++ ) {
        cmdToken[j++] = argv[i];
    }

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::api_entry_table&  api_tbl = irods::get_client_api_table();
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    init_api_table( api_tbl, pk_tbl );

    Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );

    if ( Conn == NULL ) {
        char *mySubName = NULL;
        const char *myName = rodsErrorName( errMsg.status, &mySubName );
        rodsLog( LOG_ERROR, "rcConnect failure %s (%s) (%d) %s",
                 myName,
                 mySubName,
                 errMsg.status,
                 errMsg.msg );
        free( mySubName );
        exit( 2 );
    }

    utils::warn_if_connected_to_potentially_incompatible_server(*Conn);

    status = utils::authenticate_client(Conn, myEnv);
    if ( status != 0 ) {
        print_error_stack_to_file(Conn->rError, stderr);
        rcDisconnect( Conn );
        exit( 3 );
    }

    if ( strcmp( cmdToken[0], "ldt" ) == 0 ) {
        if ( *cmdToken[1] == '\0' ) {
            cmdToken[1] = " "; /* just for subsequent checks below */
        }
    }

    if ( *cmdToken[1] == '\0' ) {
        usage();
        exit( 1 );
    }

    if ( *cmdToken[1] == '/' ) {
        rstrcpy( objPath, cmdToken[1], MAX_NAME_LEN );
    }
    else {
        rstrcat( objPath, "/", MAX_NAME_LEN );
        rstrcat( objPath, cmdToken[1], MAX_NAME_LEN );
    }

    didOne = 0;
    if ( strcmp( cmdToken[0], "ls" ) == 0 ) {
        doLs( Conn, objPath, myRodsArgs.longOption || myRodsArgs.verbose );
        didOne = 1;
    }
    if ( strcmp( cmdToken[0], "ldt" ) == 0 ) {
        doListDataTypes( Conn );
        didOne = 1;
    }

    if ( strcmp( cmdToken[0], "mod" ) == 0 ) {
        char theTime[TIME_LEN + 20];

        if ( *cmdToken[2] == '\0' ) {
            usage();
            exit( 1 );
        }
        if ( strcmp( cmdToken[2], "datatype" ) == 0 ) {
            if ( *cmdToken[3] == '\0' ) {
                usage();
                exit( 1 );
            }
            status = doModDatatype( Conn, objPath, cmdToken[3] );
            didOne = 1;
        }
        else if ( strcmp( cmdToken[2], "comment" ) == 0 ) {
            if ( *cmdToken[3] == '\0' ) {
                usage();
                exit( 1 );
            }
            if ( *cmdToken[4] == '\0' ) {
                status = doModComment( Conn, objPath, -1, cmdToken[3] );
            }
            else {
                status = doModComment( Conn, objPath, atoi( cmdToken[3] ), cmdToken[4] );
            }

            didOne = 1;
        }
        else {
            rstrcpy( theTime, cmdToken[2], TIME_LEN );
            status = 0;
            if ( *cmdToken[2] == '+' ) {
                rstrcpy( theTime, cmdToken[2] + 1, TIME_LEN ); /* skip the + */
                status = checkDateFormat( theTime );    /* check and convert the time value */
                getOffsetTimeStr( theTime, theTime );   /* convert delta format to now + this*/
            }
            else {
                status = checkDateFormat( theTime );    /* check and convert the time value */
            }
            if ( status == 0 ) {
                status = doMod( Conn, objPath, theTime );
                didOne = 1;
            }
            else {
                printf( "Invalid time format input\n" );
            }
        }
    }

    printErrorStack( Conn->rError );
    rcDisconnect( Conn );

    if ( didOne == 0 ) {
        usage();
        exit( 1 );
    }

    exit( 0 );
}

void
printMsgs( char *msgs[] ) {
    int i;
    for ( i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            return;
        }
        printf( "%s\n", msgs[i] );
    }
}

void
usage() {
    char *Msgs[] = {
        "Usage: isysmeta [-lvVh] [command]",
        " Show or modify system metadata.",
        "Commands are:",
        " mod DataObjectName Time (modify expire time)",
        " mod DataObjectName datatype Type (modify data-type)",
        " mod DataObjectName comment [replNum] Comment (modify the comment of the replica replNum or 0 by default)",
        " ls [-lvV] Name (list dataObject, -l -v for long form)",
        " ldt (list data types (those available))",
        " ",
        "Time can be full or partial date/time: '2024-12-01' or '2024-12-11.12:03'",
        "etc, or a delta time '+1h' (one hour from now), etc.",
        " ",
        "Examples:",
        " isysmeta mod foo +1h (set the expire time for file 'foo' to an hour from now)",
        " isysmeta mod /tempZone/home/rods/foo 2009-12-01",
        " isysmeta mod /tempZone/home/rods/foo datatype 'tar file'",
        " isysmeta mod /tempZone/home/rods/foo comment 1 'my comment'",
        " isysmeta ls foo",
        " isysmeta -l ls foo",
        " isysmeta ls -l foo",
        " isysmeta ldt",
        ""
    };

    printMsgs( Msgs );
    printReleaseInfo( "isysmeta" );
}
