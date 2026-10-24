/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
#include <irods/rods.h>
#include <irods/parseCommandLine.h>
#include <irods/rcMisc.h>

#include <charconv>
#include <cstdlib>
#include <cstring>
#include <iostream>

void usage( char *prog );

int
main( int argc, char **argv ) {
    set_ips_display_name("ierror");

    signal( SIGPIPE, SIG_IGN );

    if ( argc != 2 ) {
        std::cout << "Use -h for help" << std::endl;
        return 1;
    }

    if (std::strcmp(argv[1], "-h") == 0) {
        usage( argv[0] );
        return 0;
    }

    int errorCode = 0;
    std::from_chars(argv[1], argv[1] + std::strlen(argv[1]), errorCode);

    if ( errorCode > 0 ) {
        errorCode = -errorCode;
    }

    char *mySubErrName = NULL;
    const char *myErrName = rodsErrorName( errorCode, &mySubErrName );

    std::cout << "irods error: " << errorCode << ' ' << myErrName << ' ' << mySubErrName << std::endl;

    std::free(mySubErrName);
    return 0;
}


void usage( char *prog ) {
    std::cout << "Converts an irods error code to text.\n"
              << "Usage: " << prog << " [-vVh] errorNumber\n"
              << "The errorNumber can be preceded with minus sign (-) or not\n"
              << " -h  this help" << std::endl;
    printReleaseInfo( "ierror" );
}
