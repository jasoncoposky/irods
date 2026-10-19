#include "utility.hpp"
#include <irods/genquery2_builder.hpp>
#include <irods/irods_at_scope_exit.hpp>
#include <irods/irods_children_parser.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_exception.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/irods_resource_constants.hpp>
#include <irods/rcMisc.h>
#include <irods/rods.h>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>
#include <irods/rodsErrorTable.h>

#include <nlohmann/json.hpp>

#include <cstdio>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#define BIG_STR 200

// tree drawing gfx
enum DrawingStyle {
    DrawingStyle_ascii = 0,
    DrawingStyle_unicode
};

const std::string middle_child_connector[2] = {"|-- ", "\u251C\u2500\u2500 "};
const std::string last_child_connector[2] = {"`-- ", "\u2514\u2500\u2500 "};
const std::string vertical_pipe[2] = {"|   ", "\u2502   "};
const std::string indent = "    ";

void usage();

int
printGenQueryResults( rcComm_t *Conn, int status, const nlohmann::json& rows,
                      char *descriptions[], int doDashes ) {
    int printCount = 0;

    if (0 != status) {
        if (CAT_NO_ROWS_FOUND != status) {
            printError(Conn, status, "rc_genquery2");
        }
    }
    else {
        char localTime[TIME_LEN];

        for (std::size_t i = 0; i < rows.size(); i++ ) {
            const auto& row = rows[i];
            if (!row.is_array()) {
                continue;
            }
            if ( i > 0 && doDashes ) {
                printf( "----\n" );
            }

            for (std::size_t j = 0; j < row.size(); j++ ) {
                const auto& val = row[j].get_ref<const std::string&>();
                const char *tResult = val.c_str();

                if ( *descriptions[j] != '\0' ) {
                    if ( strstr( descriptions[j], "time" ) != 0 ) {
                        getLocalTimeFromRodsTime( tResult, localTime );
                        printf( "%s: %s: %s\n", descriptions[j], tResult, localTime );
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


// Via a general query, show a resource
int
showResc( char *name, int longOption, const char* zoneArgument, rcComm_t *Conn ) {
    namespace gq2 = irods::experimental::genquery2;

    int printCount = 0;
    char *columnNames[] = {
        "resource name", "id", "zone", "type",
        "location",  "vault", "free space", "free space time", "status",
        "info", "comment", "create time", "modify time",
        "context", "parent", "parent context"
    };

    std::vector<std::string> select_cols{"RESC_NAME"};
    if ( longOption ) {
        select_cols.insert(select_cols.end(), {
            "RESC_ID", "RESC_ZONE_NAME", "RESC_TYPE_NAME",
            "RESC_LOC", "RESC_VAULT_PATH", "RESC_FREE_SPACE",
            "RESC_FREE_SPACE_TIME", "RESC_STATUS", "RESC_INFO",
            "RESC_COMMENT", "RESC_CREATE_TIME", "RESC_MODIFY_TIME",
            "RESC_CONTEXT", "RESC_PARENT", "RESC_PARENT_CONTEXT"
        });
    }
    else {
        columnNames[0] = "";
    }

    // The user targeted a specific resource.
    if (name && *name != '\0') {
        // Return immediately if the user targeted the bundle resource.
        if (strncmp(name, BUNDLE_RESC, sizeof(BUNDLE_RESC)) == 0) {
            printf( "%s is a pseudo resource for system use only.\n", BUNDLE_RESC );
            return 0;
        }
    }

    const std::string_view zone_sv = (zoneArgument && *zoneArgument) ? zoneArgument : "";
    constexpr std::size_t maxRows = 50;
    std::size_t offset = 0;

    while (true) {
        auto qb = gq2::builder::select(select_cols);
        if (name && *name != '\0') {
            qb.where(gq2::builder::col("RESC_NAME") == name);
        }
        else {
            qb.where(gq2::builder::col("RESC_NAME") != BUNDLE_RESC);
        }
        qb.limit(maxRows, offset);

        nlohmann::json rows;
        const int status = gq2::try_execute(*Conn, qb.build(), rows, zone_sv);

        if (offset == 0 && (CAT_NO_ROWS_FOUND == status || (0 == status && rows.empty()))) {
            if (name && name[0] != '\0') {
                printf("Resource %s does not exist.\n", name);
            }
            else {
                printf("Resource does not exist.\n");
            }
            return CAT_NO_ROWS_FOUND;
        }
        if (status < 0) {
            if (CAT_NO_ROWS_FOUND != status) {
                printError(Conn, status, "rc_genquery2");
                return status;
            }
            break;
        }
        if (rows.empty()) {
            break;
        }

        if (offset > 0 && longOption) {
            printf("----\n");
        }

        printCount += printGenQueryResults(Conn, status, rows, columnNames, longOption);
        if (rows.size() < maxRows) {
            break;
        }
        offset += rows.size();
    }

    static_cast<void>(printCount);

    return 1;
}

// depth is a binary string of open nodes
void printDepth( const std::string& depth, DrawingStyle drawing_style ) {
    for ( std::string::const_iterator it=depth.begin(); it!=depth.end(); ++it ) {
        if ( *it == '1' ) {
            std::cout << vertical_pipe[drawing_style];
        } else {
            std::cout << indent;
        }
    }
}

// recursive function to print resource tree
void printRescTree(
     const std::string&                node_name,
     const std::string&                depth,
     const std::vector<std::string>&   resc_types,
     const std::vector<std::string>&   resc_children,
     const std::map<std::string, int>& resc_map,
     DrawingStyle                      drawing_style ) {

    // get children string
    const std::map<std::string, int>::const_iterator it_resc_map = resc_map.find(node_name);
    if (it_resc_map == resc_map.end()) {
        THROW( SYS_INTERNAL_NULL_INPUT_ERR, std::string("Missing node in resc_map: [") + node_name + "]" );
    }

    const int resc_index = it_resc_map->second;
    const std::string& children_str = resc_children[resc_index];

    // print node name, and type if not UFS
    std::cout << node_name << ":" << resc_types[resc_index] << std::endl;

    if ( children_str.empty() ) {
        return;
    }

    // print children
    irods::children_parser parser;
    parser.set_string( children_str );
    irods::children_parser::const_iterator it;
    const irods::children_parser::const_iterator final_it = --parser.end();
    for ( it=parser.begin(); it!=parser.end(); ++it ) {
        if ( it != final_it ) {
            printDepth( depth, drawing_style);
            std::cout << middle_child_connector[drawing_style];
            printRescTree( it->first, depth + "1", resc_types, resc_children, resc_map, drawing_style );
        } else {
            printDepth( depth, drawing_style );
            std::cout << last_child_connector[drawing_style];
            printRescTree( it->first, depth + "0", resc_types, resc_children, resc_map, drawing_style );
        }
    }
    return;
}

int
parseGenQueryOut(
    int                         offset,
    const nlohmann::json&       rows,
    std::vector<std::string>&   resc_names,
    std::vector<std::string>&   resc_indices,
    std::vector<std::string>&   resc_types,
    std::vector<std::string>&   resc_parents,
    std::map<std::string, int>& resc_map,
    std::vector<std::string>&   roots ) {

    // loop over rows (i.e. for each resource)
    for ( std::size_t i = 0; i < rows.size(); ++i ) {
        const auto& row = rows[i];
        if ( !row.is_array() || row.size() < 4 ) {
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
        // get resource name
        const auto& name_str = row[0].get_ref<const std::string&>();
        if ( name_str.empty() ) {
            // parsing error
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
        resc_names.push_back( name_str );

        // map resource name to row index
        resc_map[resc_names.back()] = static_cast<int>(i) + offset;

        // get resource type
        resc_types.push_back( row[1].get_ref<const std::string&>() );

        // get resource indicies
        resc_indices.push_back( row[2].get_ref<const std::string&>() );

        // check if has parent
        const auto& parent_str = row[3].get_ref<const std::string&>();
        if ( parent_str.empty() ) {
            // another root node
            roots.push_back( resc_names.back() );
            resc_parents.push_back( "" ); // parents must line up with resources
        } else {
            resc_parents.push_back( parent_str );
        }
    }

    bool all_parent_strings_are_names_not_ids_aka_queried_a_pre_42_zone = true;
    for (auto& parent : resc_parents) {
        if (!parent.empty()) {
            auto itr = resc_map.find(parent);
            if (itr == resc_map.end()) {
                all_parent_strings_are_names_not_ids_aka_queried_a_pre_42_zone = false;
                break;
            }
        }
    }

    if (all_parent_strings_are_names_not_ids_aka_queried_a_pre_42_zone) {
        for (auto& parent : resc_parents) {
            auto itr = resc_map.find(parent);
            if (itr != resc_map.end()) {
                parent = resc_indices[itr->second];
            }
        }
    }

    return 0;
}

int build_child_list(
    const std::vector<std::string>&   _resc_names,
    const std::vector<std::string>&   _resc_parents,
    const std::vector<std::string>&   _resc_ids,
    std::vector<std::string>&         _resc_children ) {

    for (size_t idx=0; idx < _resc_names.size(); ++idx ) {
        if (idx >= _resc_parents.size() || _resc_parents[idx].empty()) {
            continue;
        }

        size_t parent_pos = std::find(_resc_ids.begin(), _resc_ids.end(), _resc_parents[idx] ) - _resc_ids.begin();
        if (parent_pos >= _resc_ids.size()) {
            continue;
        }

        if (!_resc_children[parent_pos].empty()) {
            _resc_children[parent_pos] += ";";
        }
        _resc_children[parent_pos] += _resc_names[idx];
        _resc_children[parent_pos] += "{}";

    } // for idx

    return 0;

} // build_child_list


int showRescTree( const char *name, const char *zoneArgument, rcComm_t *Conn , DrawingStyle drawing_style) {
    namespace gq2 = irods::experimental::genquery2;

    // containers for storing query results
    // one vector per attribute
    std::vector<std::string> resc_names;
    std::vector<std::string> resc_indices;
    std::vector<std::string> resc_types;
    std::vector<std::string> resc_children;
    std::vector<std::string> resc_parents;

    // mapping of resource names and row indexes for looking up children
    std::map<std::string, int> resc_map;

    // list of root resources
    std::vector<std::string> roots;

    const std::string_view zone_sv = (zoneArgument && *zoneArgument) ? zoneArgument : "";
    int offset = 0;
    int status = 0;

    while (true) {
        const auto q = gq2::builder::select({"RESC_NAME", "RESC_TYPE_NAME", "RESC_ID", "RESC_PARENT"})
            .where(gq2::builder::col("RESC_NAME") != BUNDLE_RESC)
            .order_by("RESC_NAME")
            .limit(MAX_SQL_ROWS, static_cast<std::size_t>(offset))
            .build();

        nlohmann::json rows;
        status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if (status < 0) {
            if (offset == 0 || status != CAT_NO_ROWS_FOUND) {
                printError(Conn, status, "rc_genquery2");
                return status;
            }
            status = 0;
            break;
        }
        if (rows.empty()) {
            if (offset == 0) {
                return CAT_NO_ROWS_FOUND;
            }
            break;
        }

        status = parseGenQueryOut(offset, rows, resc_names, resc_indices, resc_types, resc_parents, resc_map, roots);
        if (status < 0) {
            return status;
        }
        if (rows.size() < MAX_SQL_ROWS) {
            break;
        }
        offset += static_cast<int>(rows.size());
    }

    // child strings assumed to be initialized by printRescTree
    resc_children.resize( resc_names.size() );

    status =  build_child_list( resc_names, resc_parents, resc_indices, resc_children );
    if( status < 0 ) {
        printError( Conn, status, "build_child_list failed" );
        return status;
    }

    // If target resource was specified print tree for that resource
    if ( name && *name ) {
        // check for invalid resource name
        if ( std::find( resc_names.begin(), resc_names.end(), name ) == resc_names.end() ) {
            std::cout << "Resource " << name << " does not exist." << std::endl;
            return USER_INVALID_RESC_INPUT;
        }

        // print tree
        printRescTree( name, "", resc_types, resc_children, resc_map, drawing_style );
    } else {
        // Otherwise print tree for each root node
        for ( std::vector<std::string>::const_iterator it = roots.begin(); it != roots.end(); ++it ) {
            printRescTree( *it, "", resc_types, resc_children, resc_map, drawing_style );
        }
    }

    return status;
}

int
main( int argc, char **argv ) {
    signal( SIGPIPE, SIG_IGN );
    rodsLogLevel( LOG_ERROR );

    rodsArguments_t myRodsArgs;
    int status = parseCmdLineOpt( argc, argv, "hvVlz:Z", 1, &myRodsArgs );
    if ( status ) {
        printf( "Use -h for help.\n" );
        return 1;
    }

    if ( myRodsArgs.help == True ) {
        usage();
        return 0;
    }

    char zoneArgument[MAX_NAME_LEN + 2] = "";
    if ( myRodsArgs.zone == True ) {
        strncpy( zoneArgument, myRodsArgs.zoneName, MAX_NAME_LEN );
    }

    rodsEnv myEnv;
    status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "main: getRodsEnv error. status = %d", status );
        return 1;
    }

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    irods::api_entry_table&  api_tbl = irods::get_client_api_table();
    init_api_table( api_tbl, pk_tbl );

    rErrMsg_t errMsg;
    rcComm_t *Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );

    const auto disconnect = irods::at_scope_exit{[&Conn] { rcDisconnect(Conn); }};

    try {
        if ( Conn == NULL ) {
            char *mySubName;
            const char *myName = rodsErrorName( errMsg.status, &mySubName );
            rodsLog( LOG_ERROR, "rcConnect failure %s (%s) (%d) %s",
                     myName,
                     mySubName,
                     errMsg.status,
                     errMsg.msg );

            return 2;
        }

        utils::warn_if_connected_to_potentially_incompatible_server(*Conn);

        status = utils::authenticate_client(Conn, myEnv);
        if ( status != 0 ) {
            print_error_stack_to_file(Conn->rError, stderr);
            return 3;
        }

        // tree view
        DrawingStyle drawing_style = DrawingStyle_unicode;
        if ( myRodsArgs.longOption != True ) {
            if ( myRodsArgs.ascii == True ) { // character set for printing tree
                drawing_style = DrawingStyle_ascii;
            }
            status = showRescTree( argv[myRodsArgs.optind], zoneArgument, Conn, drawing_style);
        }
        else { // regular view
            status = showResc( argv[myRodsArgs.optind], myRodsArgs.longOption, zoneArgument, Conn );
        }
    }
    catch ( const irods::exception& e ) {
        rodsLog( LOG_ERROR, "Caught irods::exception\n%s", e.what() );
        status = e.code();
    }

    printErrorStack( Conn->rError );

    if (status < 0) {
        return 1;
    }

    return 0;
}

/*
Print the main usage/help information.
 */
void usage() {
    char *msgs[] = {
        "ilsresc lists iRODS resources",
        "Usage: ilsresc [-lvVh] [--tree] [--ascii] [Name]",
        "If Name is present, list only that resource, ",
        "otherwise list them all ",
        "Options are:",
        " -l Long format - list details",
        " -v verbose",
        " -V Very verbose",
        " -z Zonename  list resources of specified Zone",
        " --tree - tree view",
        " --ascii - use ascii character set to build tree view (ignored without --tree)",
        " -h This help",
        ""
    };
    for ( int i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            break;
        }
        printf( "%s\n", msgs[i] );
    }
    printReleaseInfo( "ilsresc" );
}
