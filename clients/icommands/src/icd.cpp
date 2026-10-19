#include "utility.hpp"
#include <fcntl.h>
#include <irods/rods.h>
#include <irods/parseCommandLine.h>
#include <irods/rodsPath.h>
#include <irods/miscUtil.h>
#include <irods/rcMisc.h>
#include <irods/genquery2_builder.hpp>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>

#include <nlohmann/json.hpp>

#include <cstdio>

void usage();

int
main( int argc, char **argv ) {

    signal( SIGPIPE, SIG_IGN );

    int status, ix, i, fd, len;
    rodsArguments_t myRodsArgs;
    rodsEnv myEnv;
    char *envFile;
    char buffer[MAX_NAME_LEN];
    rcComm_t *Conn;
    rErrMsg_t errMsg;

    status = parseCmdLineOpt( argc, argv, "vVh", 0, &myRodsArgs );
    if ( status ) {
        printf( "Use -h for help.\n" );
        exit( 1 );
    }
    if ( myRodsArgs.help == True ) {
        usage();
        exit( 0 );
    }
    ix = myRodsArgs.optind;

    status = getRodsEnv( &myEnv );
    envFile = getRodsEnvFileName();

    /* Just "icd", so cd to home, so just remove the session file */
    /* (can do this for now, since session has only the cwd). */
    if ( ix >= argc ) {
        status = unlink( envFile );
        if ( myRodsArgs.verbose == True ) {
            printf( "Deleting (if it exists) session envFile:%s\n", envFile );
            printf( "unlink status = %d\n", status );
        }
        exit( 0 );
    }

    rodsPath_t rodsPath{};
    irods::at_scope_exit freePath{[&rodsPath] { clearRodsPath(&rodsPath); }};
    rstrcpy( rodsPath.inPath, argv[ix], MAX_NAME_LEN );
    parseRodsPath( &rodsPath, &myEnv );

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    irods::api_entry_table& api_tbl = irods::get_client_api_table();
    init_api_table( api_tbl, pk_tbl );

    /* Connect and check that the path exists */
    Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );
    if ( Conn == NULL ) {
        exit( 2 );
    }

    utils::warn_if_connected_to_potentially_incompatible_server(*Conn);

    status = utils::authenticate_client(Conn, myEnv);
    if ( status != 0 ) {
        print_error_stack_to_file(Conn->rError, stderr);
        rcDisconnect( Conn );
        exit( 7 );
    }

    status = getRodsObjType( Conn, &rodsPath );
    printErrorStack( Conn->rError );
    rcDisconnect( Conn );

    if ( status < 0 ) {
        fprintf( stderr, "error %d getting type", status );
        exit( 4 );
    }

    if ( rodsPath.objType != COLL_OBJ_T || rodsPath.objState != EXIST_ST ) {
        printf( "No such collection: %s\n", rodsPath.outPath );
        exit( 3 );
    }


    /* open the sessionfile and write or update it */
    if ( ( fd = open( envFile, O_CREAT | O_RDWR | O_TRUNC, 0644 ) ) < 0 ) {
        fprintf( stderr, "Unable to open envFile %s\n", envFile );
        exit( 5 );
    }

    snprintf( buffer, sizeof( buffer ), "{\n\"irods_cwd\": \"%s\"\n}\n", rodsPath.outPath );
    len = strlen( buffer );
    i = write( fd, buffer, len );
    close( fd );
    if ( i != len ) {
        fprintf( stderr, "Unable to write envFile %s\n", envFile );
        exit( 6 );
    }

    return 0;
}

/* Check to see if a collection exists */
int
checkColl( rcComm_t *Conn, char *path ) {
    namespace gq2 = irods::experimental::genquery2;
    const auto q = gq2::builder::select({"COLL_ID"})
        .where(gq2::builder::col("COLL_NAME") == (path ? path : ""))
        .limit(1)
        .build();
    nlohmann::json rows;
    if (const int status = gq2::try_execute(*Conn, q, rows); status < 0) {
        return status;
    }
    return rows.empty() ? CAT_NO_ROWS_FOUND : 0;
}

void usage() {
    printf( "Change the current working collection.\n\n" );

    printf( "Usage: icd [-vVh] [COLLECTION]\n\n" );

    printf( "If invoked without any arguments, the current working collection is set\n" );
    printf( "back to your home collection as defined in your irods_environment.json\n" );
    printf( "file.\n\n" );

    printf( "If COLLECTION matches \"..\", the current working collection is set to the\n" );
    printf( "parent collection.\n\n" );

    printf( "If COLLECTION begins with a forward slash, the path is treated as an absolute\n" );
    printf( "path.\n\n" );

    printf( "Upon success, the current working collection is stored in\n" );
    printf( "irods_environment.json.PID where PID matches the shell's PID number. This\n" );
    printf( "allows multiple terminal sessions to exist within the same environment.\n\n" );

    printf( "If the inclusion of the PID isn't sufficient, this behavior can be overridden\n" );
    printf( "by setting the environment variable, IRODS_ENVIRONMENT_FILE, to the absolute\n" );
    printf( "path of a file that will serve as the new session file. This may require\n" );
    printf( "re-running iinit. Setting the environment variable causes icd to replace the\n" );
    printf( "\".PID\" extension with \".cwd\".\n\n" );

    printf( "Options:\n" );
    printf( " -v  Verbose.\n" );
    printf( " -V  Very verbose.\n" );
    printf( " -h  Show this message.\n" );

    char name[] = "icd";
    printReleaseInfo(name);
}
