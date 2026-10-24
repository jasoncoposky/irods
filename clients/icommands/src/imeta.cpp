#include "utility.hpp"
#include <irods/filesystem/path.hpp>
#include <irods/genquery2_builder.hpp>
#include <irods/irods_at_scope_exit.hpp>
#include <irods/rcMisc.h>
#include <unistd.h>

#include <irods/rods.h>
#include <irods/rodsClient.h>
#include <irods/rodsError.h>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/rodsPath.h>

#include <fmt/core.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <cstdio>
#include <sstream>
#include <string>
#include <vector>
#include <tuple>
#include <string_view>
#include <algorithm>
#include <filesystem>
#include <optional>

#include <boost/program_options.hpp>

#define MAX_SQL 300
#define BIG_STR 3000
#define MAX_CMD_TOKENS 40

namespace po = boost::program_options;

char cwd[BIG_STR];

int longMode = 0; /* more detailed listing */

char zoneArgument[MAX_NAME_LEN + 2] = "";

rcComm_t *Conn;
rodsEnv myEnv;

int lastCommandStatus = 0;
int printCount = 0;

bool admin_mode = false;

int usage( const char *subOpt );

int display_help(const std::vector<std::string>& _sub_args);

int do_command(const std::string& _cmd, const std::vector<std::string>& _sub_args);

std::tuple<int, std::string> prepare_name(std::string_view object_type, std::string_view name);

po::variables_map parse_sub_args(const std::vector<std::string>& _sub_args,
                                 const po::options_description& _desc,
                                 const po::positional_options_description& _pos);

std::tuple<int, std::string> prepare_name(std::string_view object_type, std::string_view name)
{
    const auto is_path_type = [object_type]() noexcept
    {
        using namespace std::string_view_literals;
        const auto types = {"-d"sv, "-C"sv, "-ld"sv, "-lC"sv};
        return std::any_of(std::begin(types), std::end(types), [object_type](auto type) {
            return type == object_type;
        });
    };

    if (!is_path_type()) {
        return {0, name.data()};
    }

    std::unique_ptr<char[]> temp_path{new char[MAX_NAME_LEN]};

    if (const auto ec = parseRodsPathStr(name.data(), &myEnv, temp_path.get()); ec) {
        return {ec, {}};
    }

    const auto deleter = [](char* p) { std::free(p); };
    std::unique_ptr<char, decltype(deleter)> escaped_path{escape_path(temp_path.get()), deleter};

    return {0, escaped_path.get()};
}

// print the results of a general query.
void printGenQueryResults(rcComm_t *Conn,
                          int status,
                          const nlohmann::json& rows,
                          char *descriptions[])
{
    char localTime[TIME_LEN];
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
                if ( i > 0 ) {
                    printf( "----\n" );
                }

                for ( std::size_t j = 0; j < row.size(); j++ ) {
                    const auto& val = row[j].get_ref<const std::string&>();
                    const char *tResult = val.c_str();
                    if ( *descriptions[j] != '\0' ) {
                        if ( strstr( descriptions[j], "time" ) != 0 ) {
                            getLocalTimeFromRodsTime( tResult, localTime );
                            printf( "%s: %s\n", descriptions[j], localTime );
                        }
                        else {
                            printf( "%s: %s\n", descriptions[j], tResult );
                            printCount++;
                        }
                    }
                }
            }
        }
    }
}

/*
 Via a general query and show the AVUs for a dataobject.
 */
int showDataObj(char* name, char* attrName)
{
    namespace gq2 = irods::experimental::genquery2;
    namespace fs = irods::filesystem;

    char fullName[MAX_NAME_LEN];
    /* Fourth 'time set' column is only used in longMode */
    char *columnNames[] = { "attribute", "value", "units", "time set" };

    printf( "AVUs defined for dataObj %s:\n", name );
    printCount = 0;

    std::vector<std::string> select_cols{
        "META_DATA_ATTR_NAME",
        "META_DATA_ATTR_VALUE",
        "META_DATA_ATTR_UNITS"
    };
    if ( longMode ) {
        select_cols.push_back("META_DATA_MODIFY_TIME");
    }

    if ( *name == '/' ) {
        snprintf( fullName, sizeof( fullName ), "%s", name );
    }
    else {
        snprintf( fullName, sizeof( fullName ), "%s/%s", cwd, name );
    }

    const fs::path full_p{fullName};
    const std::string_view coll_sv = (strstr( name, "/" ) != NULL)
        ? full_p.parent_path_view()
        : std::string_view{cwd};
    const std::string_view data_sv = (strstr( name, "/" ) != NULL)
        ? full_p.object_name_view()
        : std::string_view{name};

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        auto cond = (gq2::builder::col("COLL_NAME") == coll_sv) &&
                    (gq2::builder::col("DATA_NAME") == data_sv);
        if ( attrName != NULL && *attrName != '\0' ) {
            cond = cond && (gq2::builder::col("META_DATA_ATTR_NAME") == attrName);
        }
        else {
            cond = cond && gq2::builder::col("META_DATA_ATTR_ID").is_not_null();
        }

        const auto q = gq2::builder::select(select_cols)
            .where(std::move(cond))
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        int status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            const auto check_q = gq2::builder::select({"DATA_PATH"})
                .where(gq2::builder::col("COLL_NAME") == coll_sv &&
                       gq2::builder::col("DATA_NAME") == data_sv)
                .limit(1)
                .build();
            nlohmann::json check_rows;
            status = gq2::try_execute(*Conn, check_q, check_rows, zone_sv);
            if ( status == 0 && !check_rows.empty() ) {
                printf( "None\n" );
                return 0;
            }
            if ( status == CAT_NO_ROWS_FOUND || (status == 0 && check_rows.empty()) ) {
                lastCommandStatus = CAT_NO_ROWS_FOUND;
                std::cerr << "Error: "
                          << "Dataobject " << name << " does not exist,"
                          << std::endl
                          << "or, if 'strict' access control is enabled, you may not have access."
                          << std::endl;
                return CAT_NO_ROWS_FOUND;
            }
            printGenQueryResults( Conn, status, check_rows, columnNames );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, columnNames );
            }
            break;
        }
        if ( offset > 0 ) {
            printf( "----\n" );
        }
        printGenQueryResults( Conn, status, rows, columnNames );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    return 0;
}

/*
Via a general query, show the AVUs for a collection
*/
int showColl(char* name, char* attrName)
{
    namespace gq2 = irods::experimental::genquery2;

    /* Fourth 'time set' column is only used in longMode */
    char *columnNames[] = { "attribute", "value", "units", "time set" };

    printf( "AVUs defined for collection %s:\n", name );
    printCount = 0;

    std::vector<std::string> select_cols{
        "META_COLL_ATTR_NAME",
        "META_COLL_ATTR_VALUE",
        "META_COLL_ATTR_UNITS"
    };
    if ( longMode ) {
        select_cols.push_back("META_COLL_MODIFY_TIME");
    }

    std::string coll_name = cwd;
    if ( strlen( name ) > 0 ) {
        if ( *name == '/' ) {
            coll_name = name;
        }
        else {
            coll_name += '/';
            coll_name += name;
        }
    }

    if (coll_name.size() >= MAX_NAME_LEN) {
        fmt::print(stderr, "Error: Path length [{}] exceeds max length [{}].\n", coll_name.size(), MAX_NAME_LEN);
    }

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        auto cond = (gq2::builder::col("COLL_NAME") == coll_name);
        if ( attrName != NULL && *attrName != '\0' ) {
            cond = cond && (gq2::builder::col("META_COLL_ATTR_NAME") == attrName);
        }
        else {
            cond = cond && gq2::builder::col("META_COLL_ATTR_ID").is_not_null();
        }

        const auto q = gq2::builder::select(select_cols)
            .where(std::move(cond))
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        int status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            const auto check_q = gq2::builder::select({"COLL_COMMENTS"})
                .where(gq2::builder::col("COLL_NAME") == coll_name)
                .limit(1)
                .build();
            nlohmann::json check_rows;
            status = gq2::try_execute(*Conn, check_q, check_rows, zone_sv);
            if ( status == 0 && !check_rows.empty() ) {
                printf( "None\n" );
                return 0;
            }
            if ( status == CAT_NO_ROWS_FOUND || (status == 0 && check_rows.empty()) ) {
                lastCommandStatus = CAT_NO_ROWS_FOUND;
                std::cerr << "Error: "
                          << "Collection " << name << " does not exist."
                          << std::endl;
                return CAT_NO_ROWS_FOUND;
            }
            printGenQueryResults( Conn, status, check_rows, columnNames );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, columnNames );
            }
            break;
        }
        if ( offset > 0 ) {
            printf( "----\n" );
        }
        printGenQueryResults( Conn, status, rows, columnNames );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    return 0;
}

/*
Via a general query, show the AVUs for a resource
*/
int showResc(char* name, char* attrName)
{
    namespace gq2 = irods::experimental::genquery2;

    /* Fourth 'time set' column is only used in longMode */
    char *columnNames[] = { "attribute", "value", "units", "time set" };

    printf( "AVUs defined for resource %s:\n", name );
    printCount = 0;

    std::vector<std::string> select_cols{
        "META_RESC_ATTR_NAME",
        "META_RESC_ATTR_VALUE",
        "META_RESC_ATTR_UNITS"
    };
    if ( longMode ) {
        select_cols.push_back("META_RESC_MODIFY_TIME");
    }

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        auto cond = (gq2::builder::col("RESC_NAME") == name);
        if ( attrName != NULL && *attrName != '\0' ) {
            cond = cond && (gq2::builder::col("META_RESC_ATTR_NAME") == attrName);
        }
        else {
            cond = cond && gq2::builder::col("META_RESC_ATTR_ID").is_not_null();
        }

        const auto q = gq2::builder::select(select_cols)
            .where(std::move(cond))
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        int status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            const auto check_q = gq2::builder::select({"RESC_INFO"})
                .where(gq2::builder::col("RESC_NAME") == name)
                .limit(1)
                .build();
            nlohmann::json check_rows;
            status = gq2::try_execute(*Conn, check_q, check_rows, zone_sv);
            if ( status == 0 && !check_rows.empty() ) {
                printf( "None\n" );
                return 0;
            }
            if ( status == CAT_NO_ROWS_FOUND || (status == 0 && check_rows.empty()) ) {
                lastCommandStatus = CAT_NO_ROWS_FOUND;
                std::cerr << "Error: "
                          << "Resource " << name << " does not exist."
                          << std::endl;
                return CAT_NO_ROWS_FOUND;
            }
            printGenQueryResults( Conn, status, check_rows, columnNames );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, columnNames );
            }
            break;
        }
        if ( offset > 0 ) {
            printf( "----\n" );
        }
        printGenQueryResults( Conn, status, rows, columnNames );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    return 0;
}

/*
Via a general query, show the AVUs for a user
*/
int showUser(char* name, char* attrName)
{
    namespace gq2 = irods::experimental::genquery2;

    /* Fourth 'time set' column is only used in longMode */
    char *columnNames[] = { "attribute", "value", "units", "time set" };

    char userName[NAME_LEN];
    char userZone[NAME_LEN];

    if ( parseUserName( name, userName, userZone ) ) {
        std::cerr << "Error: "
                  << "Invalid username format"
                  << std::endl;
        return -4;
    }
    if ( userZone[0] == '\0' ) {
        snprintf( userZone, sizeof( userZone ), "%s", myEnv.rodsZone );
    }

    printf( "AVUs defined for user %s#%s:\n", userName, userZone );
    printCount = 0;

    std::vector<std::string> select_cols{
        "META_USER_ATTR_NAME",
        "META_USER_ATTR_VALUE",
        "META_USER_ATTR_UNITS"
    };
    if ( longMode ) {
        select_cols.push_back("META_USER_MODIFY_TIME");
    }

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;

    while ( true ) {
        auto cond = (gq2::builder::col("USER_NAME") == userName) &&
                    (gq2::builder::col("USER_ZONE") == userZone);
        if ( attrName != NULL && *attrName != '\0' ) {
            cond = cond && (gq2::builder::col("META_USER_ATTR_NAME") == attrName);
        }
        else {
            cond = cond && gq2::builder::col("META_USER_ATTR_ID").is_not_null();
        }

        const auto q = gq2::builder::select(select_cols)
            .where(std::move(cond))
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        int status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            const auto check_q = gq2::builder::select({"USER_COMMENT"})
                .where(gq2::builder::col("USER_NAME") == userName)
                .limit(1)
                .build();
            nlohmann::json check_rows;
            status = gq2::try_execute(*Conn, check_q, check_rows, zone_sv);
            if ( status == 0 && !check_rows.empty() ) {
                printf( "None\n" );
                return 0;
            }
            if ( status == CAT_NO_ROWS_FOUND || (status == 0 && check_rows.empty()) ) {
                lastCommandStatus = CAT_NO_ROWS_FOUND;
                std::cerr << "Error: "
                          << "User " << name << " does not exist."
                          << std::endl;
                return CAT_NO_ROWS_FOUND;
            }
            printGenQueryResults( Conn, status, check_rows, columnNames );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, columnNames );
            }
            break;
        }
        if ( offset > 0 ) {
            printf( "----\n" );
        }
        printGenQueryResults( Conn, status, rows, columnNames );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    return 0;
}

enum class object_type
{
    data_object,
    collection
};

auto make_meta_op_cond(std::string_view _col_name, std::string_view _op, std::string_view _val)
    -> irods::experimental::genquery2::builder::condition_builder
{
    namespace gq2 = irods::experimental::genquery2;
    const gq2::builder::col c{_col_name};
    if (_op == "=") {
        return c == _val;
    }
    if (_op == "!=" || _op == "<>") {
        return c != _val;
    }
    if (_op == "<" || _op == "n<") {
        return c < _val;
    }
    if (_op == "<=" || _op == "n<=") {
        return c <= _val;
    }
    if (_op == ">" || _op == "n>") {
        return c > _val;
    }
    if (_op == ">=" || _op == "n>=") {
        return c >= _val;
    }
    if (_op == "like") {
        return c.like(_val);
    }
    if (_op == "not like") {
        return !c.like(_val);
    }
    return c == _val;
}

int find_objects_by_metadata(const char* _cmd_tokens[], object_type _object_type)
{
    namespace gq2 = irods::experimental::genquery2;

    printCount = 0;

    std::vector<std::string> select_cols{"COLL_NAME"};
    if (object_type::data_object == _object_type) {
        select_cols.push_back("DATA_NAME");
    }

    const std::string_view attr_col = (object_type::data_object == _object_type)
        ? "META_DATA_ATTR_NAME"
        : "META_COLL_ATTR_NAME";
    const std::string_view val_col = (object_type::data_object == _object_type)
        ? "META_DATA_ATTR_VALUE"
        : "META_COLL_ATTR_VALUE";

    auto cond = (gq2::builder::col(attr_col) == _cmd_tokens[2]);
    auto val_cond = make_meta_op_cond(val_col, _cmd_tokens[3], _cmd_tokens[4]);

    int token_index = 5;

    while (true) {
        if (std::strcmp(_cmd_tokens[token_index], "and") == 0) {
            cond = cond && val_cond.group();

            const auto* avu_name = _cmd_tokens[++token_index];
            const auto* avu_op = _cmd_tokens[++token_index];
            const auto* avu_value = _cmd_tokens[++token_index];

            cond = cond && (gq2::builder::col(attr_col) == avu_name);
            val_cond = make_meta_op_cond(val_col, avu_op, avu_value);

            ++token_index;
        }
        else if (std::strcmp(_cmd_tokens[token_index], "or") == 0) {
            const auto* avu_op = _cmd_tokens[++token_index];
            const auto* avu_value = _cmd_tokens[++token_index];

            val_cond = val_cond || make_meta_op_cond(val_col, avu_op, avu_value);

            ++token_index;
        }
        else {
            break;
        }
    }

    cond = cond && val_cond.group();

    if (*_cmd_tokens[token_index] != '\0') {
        std::cerr << "Error: Unrecognized input\n";
        return -2;
    }

    std::vector<char*> column_names{"collection"};
    if (object_type::data_object == _object_type) {
        column_names.push_back("dataObj");
    }

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;
    int status = 0;

    while (true) {
        const auto q = gq2::builder::select(select_cols)
            .where(cond)
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if (offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty()))) {
            printGenQueryResults(Conn, CAT_NO_ROWS_FOUND, rows, column_names.data());
            return 0;
        }
        if (status < 0 || rows.empty()) {
            if (status < 0 && status != CAT_NO_ROWS_FOUND) {
                printGenQueryResults(Conn, status, rows, column_names.data());
            }
            break;
        }
        if (offset > 0) {
            printf("----\n");
        }
        printGenQueryResults(Conn, status, rows, column_names.data());
        if (rows.size() < maxRows) {
            break;
        }
        offset += rows.size();
    }

    return status == CAT_NO_ROWS_FOUND ? 0 : status;
}

// Do a query on AVUs for dataobjs and show the results
// attribute op value [AND attribute op value] [REPEAT]
int queryDataObj(const char *cmdToken[])
{
    return find_objects_by_metadata(cmdToken, object_type::data_object);
}

// Do a query on AVUs for collections and show the results
int queryCollection(const char *cmdToken[])
{
    return find_objects_by_metadata(cmdToken, object_type::collection);
}

/*
Do a query on AVUs for resources and show the results
 */
int queryResc( char *attribute, char *op, char *value ) {
    namespace gq2 = irods::experimental::genquery2;
    char *columnNames[] = {"resource"};
    printCount = 0;

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;
    int status = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"RESC_NAME"})
            .where((gq2::builder::col("META_RESC_ATTR_NAME") == attribute) &&
                   make_meta_op_cond("META_RESC_ATTR_VALUE", op, value))
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            printGenQueryResults( Conn, CAT_NO_ROWS_FOUND, rows, columnNames );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, columnNames );
            }
            break;
        }
        if ( offset > 0 ) {
            printf( "----\n" );
        }
        printGenQueryResults( Conn, status, rows, columnNames );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    return status == CAT_NO_ROWS_FOUND ? 0 : status;
}

/*
Do a query on AVUs for users and show the results
 */
int queryUser( char *attribute, char *op, char *value ) {
    namespace gq2 = irods::experimental::genquery2;
    char *columnNames[] = {"user", "zone"};
    printCount = 0;

    const std::string_view zone_sv = (zoneArgument[0] != '\0') ? zoneArgument : "";
    constexpr std::size_t maxRows = 10;
    std::size_t offset = 0;
    int status = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"USER_NAME", "USER_ZONE"})
            .where((gq2::builder::col("META_USER_ATTR_NAME") == attribute) &&
                   make_meta_op_cond("META_USER_ATTR_VALUE", op, value))
            .limit(maxRows, offset)
            .build();

        nlohmann::json rows;
        status = gq2::try_execute(*Conn, q, rows, zone_sv);
        if ( offset == 0 && (status == CAT_NO_ROWS_FOUND || (status == 0 && rows.empty())) ) {
            printGenQueryResults( Conn, CAT_NO_ROWS_FOUND, rows, columnNames );
            return 0;
        }
        if ( status < 0 || rows.empty() ) {
            if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
                printGenQueryResults( Conn, status, rows, columnNames );
            }
            break;
        }
        if ( offset > 0 ) {
            printf( "----\n" );
        }
        printGenQueryResults( Conn, status, rows, columnNames );
        if ( rows.size() < maxRows ) {
            break;
        }
        offset += rows.size();
    }

    return status == CAT_NO_ROWS_FOUND ? 0 : status;
}


/*
 Modify (copy) AVUs
 */
int
modCopyAVUMetadata( char *arg0, char *arg1, char *arg2, char *arg3,
                    char *arg4, char *arg5, char *arg6, char *arg7 )
{
    modAVUMetadataInp_t modAVUMetadataInp{};
    char fullName1[MAX_NAME_LEN];
    char fullName2[MAX_NAME_LEN];

    if ( strcmp( arg1, "-R" ) == 0 || strcmp( arg1, "-r" ) == 0 ||
            strcmp( arg1, "-u" ) == 0 ) {
        snprintf( fullName1, sizeof( fullName1 ), "%s", arg3 );
    }
    else if ( strlen( arg3 ) > 0 ) {
        if ( *arg3 == '/' ) {
            snprintf( fullName1, sizeof( fullName1 ), "%s", arg3 );
        }
        else {
            snprintf( fullName1, sizeof( fullName1 ), "%s/%s", cwd, arg3 );
        }
    }
    else {
        snprintf( fullName1, sizeof( fullName1 ), "%s", cwd );
    }

    if ( strcmp( arg2, "-R" ) == 0 || strcmp( arg2, "-r" ) == 0 ||
            strcmp( arg2, "-u" ) == 0 ) {
        snprintf( fullName2, sizeof( fullName2 ), "%s", arg4 );
    }
    else if ( strlen( arg4 ) > 0 ) {
        if ( *arg4 == '/' ) {
            snprintf( fullName2, sizeof( fullName2 ), "%s", arg4 );
        }
        else {
            snprintf( fullName2, sizeof( fullName2 ), "%s/%s", cwd, arg4 );
        }
    }
    else {
        snprintf( fullName2, sizeof( fullName2 ), "%s", cwd );
    }

    modAVUMetadataInp.arg0 = arg0;
    modAVUMetadataInp.arg1 = arg1;
    modAVUMetadataInp.arg2 = arg2;
    modAVUMetadataInp.arg3 = fullName1;
    modAVUMetadataInp.arg4 = fullName2;
    modAVUMetadataInp.arg5 = arg5;
    modAVUMetadataInp.arg6 = arg6;
    modAVUMetadataInp.arg7 = arg7;
    modAVUMetadataInp.arg8 = "";
    modAVUMetadataInp.arg9 = "";

    if (admin_mode) {
        addKeyVal(&modAVUMetadataInp.condInput, ADMIN_KW, "");
    }

    int status = rcModAVUMetadata( Conn, &modAVUMetadataInp );
    lastCommandStatus = status;

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

    if (status < 0) {
        char *mySubName = NULL;
        const char *myName = rodsErrorName( status, &mySubName );
        rodsLog( LOG_ERROR, "rcModAVUMetadata failed with error %d %s %s",
                 status, myName, mySubName );
        free( mySubName );
    }

    if ( status == CAT_UNKNOWN_FILE ) {
        char tempName[MAX_NAME_LEN] = "/";
        int len;
        int isRemote = 0;
        strncat( tempName, myEnv.rodsZone, MAX_NAME_LEN - strlen( tempName ) );
        len = strlen( tempName );
        if ( strncmp( tempName, fullName1, len ) != 0 ) {
            std::cerr << "Error: "
                      << "Cannot copy metadata from a remote zone."
                      << std::endl;
            isRemote = 1;
        }
        if ( strncmp( tempName, fullName2, len ) != 0 ) {
            std::cerr << "Error: "
                      << "Cannot copy metadata to a remote zone."
                      << std::endl;
            isRemote = 1;
        }
        if ( isRemote ) {
            std::cerr << "Copying of metadata is done via SQL within each ICAT"
                      << std::endl
                      << "for efficiency.  Copying metadata between zones is"
                      << std::endl
                      << "not implemented."
                      << std::endl;
        }
    }
    return status;
}

/*
 Modify (add or remove) AVUs
 */
int
modAVUMetadata(char *arg0, char *arg1, char *arg2, char *arg3,
               char *arg4, char *arg5, char *arg6, char *arg7, char *arg8)
{
    modAVUMetadataInp_t modAVUMetadataInp{};
    char fullName[MAX_NAME_LEN];

    if ( strcmp( arg1, "-R" ) == 0 || strcmp( arg1, "-r" ) == 0 ||
            strcmp( arg1, "-u" ) == 0 ) {
        snprintf( fullName, sizeof( fullName ), "%s", arg2 );
    }
    else if ( strlen( arg2 ) > 0 ) {
        if ( *arg2 == '/' ) {
            snprintf( fullName, sizeof( fullName ), "%s", arg2 );
        }
        else {
            snprintf( fullName, sizeof( fullName ), "%s/%s", cwd, arg2 );
        }
    }
    else {
        snprintf( fullName, sizeof( fullName ), "%s", cwd );
    }

    modAVUMetadataInp.arg0 = arg0;
    modAVUMetadataInp.arg1 = arg1;
    modAVUMetadataInp.arg2 = fullName;
    modAVUMetadataInp.arg3 = arg3;
    modAVUMetadataInp.arg4 = arg4;
    modAVUMetadataInp.arg5 = arg5;
    modAVUMetadataInp.arg6 = arg6;
    modAVUMetadataInp.arg7 = arg7;
    modAVUMetadataInp.arg8 = arg8;
    modAVUMetadataInp.arg9 = "";

    if (admin_mode) {
        addKeyVal(&modAVUMetadataInp.condInput, ADMIN_KW, "");
    }

    const int status = rcModAVUMetadata( Conn, &modAVUMetadataInp );
    lastCommandStatus = status;

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

    if (status < 0) {
        char *mySubName = NULL;
        const char *myName = rodsErrorName( status, &mySubName );
        rodsLog( LOG_ERROR, "rcModAVUMetadata failed with error %d %s %s",
                 status, myName, mySubName );
        free( mySubName );
    }

    return status;
}

std::tuple<po::parsed_options, po::variables_map>
parse_program_options(int _argc,
                      const char** _argv,
                      rodsArguments_t& _rods_args)
{
    po::options_description global("Global options");
    global.add_options()
        ("help,h",                                             "Show imeta help")
        ("verbose,v",                                          "Verbose output")
        ("very_verbose,V",                                     "Very verbose output")
        ("admin_mode,M",                                       "Execute as administrator")
        ("zone_name,z", po::value< std::string >(),            "Work with the specified zone")
        ("command", po::value< std::string >(),                "Command to execute")
        ("subargs", po::value< std::vector< std::string > >(), "Arguments for command");

    po::positional_options_description pos;
    pos.add("command", 1).
        add("subargs", -1);

    try {
        po::parsed_options parsed =
            po::command_line_parser( _argc, _argv ).
            options( global ).
            positional( pos ).
            allow_unregistered().
            run();

        po::variables_map vm;
        po::store( parsed, vm );
        po::notify( vm );

        if ( vm.count( "verbose" ) ) {
            _rods_args.verbose = 1;
        }

        if ( vm.count( "very_verbose" ) ) {
            _rods_args.verbose = 1;
            _rods_args.veryVerbose = 1;
            rodsLogLevel( LOG_NOTICE );
        }

        if ( vm.count( "zone_name" ) ) {
            _rods_args.zone = 1;
            _rods_args.zoneName = (char*) vm["zone_name"].as<std::string>().c_str();
            rstrcpy(zoneArgument, _rods_args.zoneName, sizeof(zoneArgument));
        }

        if (vm.count("admin_mode")) {
            admin_mode = true;
        }

        if ( vm.count( "help" ) ) {
            usage( "" );
            exit( 0 );
        }

        return std::make_tuple(parsed, vm);
    }
    catch (const po::error& _e) {
        std::cerr << "Error: "
                  << _e.what()
                  << std::endl;
        THROW( SYS_INVALID_INPUT_PARAM, _e.what() );
    }
}

std::vector<std::string> reorder_arguments(
    const std::string& _attribute,
    const std::string& _value,
    const std::string& _unit) {
    std::vector<std::string> args;
    if(!_attribute.empty()) {
        args.push_back(_attribute);
    }
    if(!_value.empty()) {
        args.push_back(_value);
    }
    if(!_unit.empty()) {
        args.push_back(_unit);
    }

    while(args.size() < 3) {
        args.push_back("");
    }

    return args;
}

int do_command(const std::string& _cmd, const std::vector<std::string>& _sub_args) {

    if ( _sub_args.size() >= MAX_CMD_TOKENS ) {
        std::cerr << "Error: "
                  << "Unrecognized input, too many input tokens"
                  << std::endl;
        rcDisconnect(Conn);
        exit( 4 );
    }

    if (_cmd == "add") {
        // Add new AVU triple
        std::string units = "";

        po::options_description add_desc( "add options" );
        add_desc.add_options()
            ("object_type", po::value<std::string>(), "Object type (-d/-C/-R/-u)")
            ("name", po::value<std::string>(),        "Object name")
            ("attribute", po::value<std::string>(),   "AVU attribute")
            ("value", po::value<std::string>(),       "AVU value")
            ("units", po::value<std::string>(),       "AVU units [optional]");

        po::positional_options_description add_pos;
        add_pos.add("object_type", 1).
            add("name", 1).
            add("attribute", 1).
            add("value", 1).
            add("units", 1);

        po::variables_map sub_vm;
        try {
            sub_vm = parse_sub_args(_sub_args, add_desc, add_pos);
        } catch ( const irods::exception& e ) {
            return e.code();
        }

        if ( !sub_vm.count( "object_type" ) ||
            ( sub_vm["object_type"].as<std::string>() != "-d" &&
             sub_vm["object_type"].as<std::string>() != "-C" &&
             sub_vm["object_type"].as<std::string>() != "-R" &&
             sub_vm["object_type"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "value" ) ) {
            std::cerr << "Error: "
                      << "Not enough arguments provided to "
                      << _cmd
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( sub_vm.count( "units" ) ) {
            units = sub_vm["units"].as<std::string>();
        }

        auto [status, name] = prepare_name(sub_vm["object_type"].as<std::string>(),
                                           sub_vm["name"].as<std::string>());

        if (status) {
            return status;
        }

        status = modAVUMetadata((char*) _cmd.c_str(),
                                (char*) sub_vm["object_type"].as<std::string>().c_str(),
                                name.data(),
                                (char*) sub_vm["attribute"].as<std::string>().c_str(),
                                (char*) sub_vm["value"].as<std::string>().c_str(),
                                (char*) units.c_str(),
                                "",
                                "",
                                "" );

        if ( status < 0 ) {
            return status;
        }

        return 0;
    }
    else if (_cmd == "rm") {
        // Remove AVU triple
        std::string units = "";

        po::options_description rm_desc( "rm options" );
        rm_desc.add_options()
            ("object_type", po::value<std::string>(), "Object type (-d/-C/-R/-u)")
            ("name", po::value<std::string>(),      "Object name")
            ("attribute", po::value<std::string>(), "AVU attribute")
            ("value", po::value<std::string>(),     "AVU value")
            ("units", po::value<std::string>(),     "AVU units [optional]");

        po::positional_options_description rm_pos;
        rm_pos.add("object_type", 1).
            add("name", 1).
            add("attribute", 1).
            add("value", 1).
            add("units", 1);

        po::variables_map sub_vm;
        try {
            sub_vm = parse_sub_args(_sub_args, rm_desc, rm_pos);
        } catch ( const irods::exception& e ) {
            return e.code();
        }

        if ( !sub_vm.count( "object_type" ) ||
            ( sub_vm["object_type"].as<std::string>() != "-d" &&
             sub_vm["object_type"].as<std::string>() != "-C" &&
             sub_vm["object_type"].as<std::string>() != "-R" &&
             sub_vm["object_type"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "value" ) ) {
            std::cerr << "Error: "
                      << "Not enough arguments provided to "
                      << _cmd
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( sub_vm.count( "units" ) ) {
            units = sub_vm["units"].as<std::string>();
        }

        auto [status, name] = prepare_name(sub_vm["object_type"].as<std::string>(),
                                           sub_vm["name"].as<std::string>());

        if (status) {
            return status;
        }

        status = modAVUMetadata((char*) _cmd.c_str(),
                                (char*) sub_vm["object_type"].as<std::string>().c_str(),
                                name.data(),
                                (char*) sub_vm["attribute"].as<std::string>().c_str(),
                                (char*) sub_vm["value"].as<std::string>().c_str(),
                                (char*) units.c_str(),
                                "",
                                "",
                                "" );

        return status < 0 ? status : 0;
    }
    else if (_cmd == "rmi") {
        // Remove AVU triple by metadata ID
        po::options_description rmi_desc( "rm options" );
        rmi_desc.add_options()
            ("object_type", po::value<std::string>(), "Object type (-d/-C/-R/-u)")
            ("name", po::value<std::string>(),        "Object name")
            ("metadata_id", po::value<std::string>(), "Metadata ID");

        po::positional_options_description rmi_pos;
        rmi_pos.add("object_type", 1).
            add("name", 1).
            add("metadata_id", 1);

        po::variables_map sub_vm;
        try {
            sub_vm = parse_sub_args(_sub_args, rmi_desc, rmi_pos);
        } catch ( const irods::exception& e ) {
            return e.code();
        }

        if ( !sub_vm.count( "object_type" ) ||
            ( sub_vm["object_type"].as<std::string>() != "-d" &&
             sub_vm["object_type"].as<std::string>() != "-C" &&
             sub_vm["object_type"].as<std::string>() != "-R" &&
             sub_vm["object_type"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "metadata_id" ) ) {
            std::cerr << "Error: "
                      << "Not enough arguments provided to "
                      << _cmd
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        auto [status, name] = prepare_name(sub_vm["object_type"].as<std::string>(),
                                           sub_vm["name"].as<std::string>());

        if (status) {
            return status;
        }

        status = modAVUMetadata((char*) _cmd.c_str(),
                                (char*) sub_vm["object_type"].as<std::string>().c_str(),
                                name.data(),
                                (char*) sub_vm["metadata_id"].as<std::string>().c_str(),
                                "",
                                "",
                                "",
                                "",
                                "" );

        return status < 0 ? status : 0;
    }
    else if (_cmd == "mod") {
        // Modify existing AVU triple
        std::string units = "";
        std::string new_attribute = "";
        std::string new_value = "";
        std::string new_units = "";

        po::options_description mod_desc( "mod options" );
        mod_desc.add_options()
            ("object_type", po::value<std::string>(), "Object type (-d/-C/-R/-u)")
            ("name", po::value<std::string>(),      "Object name")
            ("attribute", po::value<std::string>(), "AVU attribute")
            ("value", po::value<std::string>(),     "AVU value")
            ("opt_1", po::value<std::string>(),     "AVU units [optional]")
            ("opt_2", po::value<std::string>(),     "New AVU attribute [optional]")
            ("opt_3", po::value<std::string>(),     "New AVU value [optional]")
            ("opt_4", po::value<std::string>(),     "New AVU units [optional]");

        po::positional_options_description mod_pos;
        mod_pos.add("object_type", 1).
            add("name", 1).
            add("attribute", 1).
            add("value", 1).
            add("opt_1", 1).
            add("opt_2", 1).
            add("opt_3", 1).
            add("opt_4", 1);

        po::variables_map sub_vm;
        try {
            sub_vm = parse_sub_args(_sub_args, mod_desc, mod_pos);
        } catch ( const irods::exception& e ) {
            return e.code();
        }

        if ( !sub_vm.count( "object_type" ) ||
            ( sub_vm["object_type"].as<std::string>() != "-d" &&
             sub_vm["object_type"].as<std::string>() != "-C" &&
             sub_vm["object_type"].as<std::string>() != "-R" &&
             sub_vm["object_type"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "opt_1" ) ) {
            std::cerr << "Error: "
                      << "Not enough arguments provided to "
                      << _cmd
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        } else {

            // Issue 3788 - Reject duplicate new attr, val, or unit
            bool new_attribute_set = false;
            bool new_value_set = false;
            bool new_unit_set = false;

            std::vector<std::string> new_argument_identifier_list = { "opt_1", "opt_2", "opt_3", "opt_4" };

            for (const std::string& option_identifier : new_argument_identifier_list) {

                if ( sub_vm.count( option_identifier ) ) {
                    std::string temp = sub_vm[option_identifier].as<std::string>();
                    std::string label = temp.substr(0,2);
                    if("n:" == label) {
                        if (new_attribute_set) {
                            std::cerr << "Error: "
                                << "New attribute specified more than once"
                                << std::endl;
                            return SYS_INVALID_INPUT_PARAM;

                        } else {
                            new_attribute_set = true;
                        }
                    } else if("v:" == label) {
                        if (new_value_set) {
                            std::cerr << "Error: "
                                << "New value specified more than once"
                                << std::endl;
                            return SYS_INVALID_INPUT_PARAM;
                        } else {
                            new_value_set = true;
                        }
                    } else if("u:" == label) {
                        if (new_unit_set) {
                            std::cerr << "Error: "
                                << "New unit specified more than once"
                                << std::endl;
                            return SYS_INVALID_INPUT_PARAM;
                        } else {
                            new_unit_set = true;
                        }
                    }
                }
            }
            // End Issue 3788 changes

            // Need to check optional arguments to see if the string represents
            //  units, new_attribute, new_value, or new_units
            std::string temp = sub_vm["opt_1"].as<std::string>();
            std::string label = temp.substr(0,2);

            if("n:" == label) {
                new_attribute = temp;
            } else if("v:" == label) {
                new_value = temp;
            } else if("u:" == label) {
                new_units = temp;
            } else {
                units = temp;
            }
        }

        if ( sub_vm.count( "opt_2" ) ) {
            std::string temp = sub_vm["opt_2"].as<std::string>();
            std::string label = temp.substr(0,2);

            if("n:" == label) {
                new_attribute = temp;
            } else if("v:" == label) {
                new_value = temp;
            } else if("u:" == label) {
                new_units = temp;
            } else {
                units = temp;
            }
        }

        if ( sub_vm.count( "opt_3" ) ) {
            std::string temp = sub_vm["opt_3"].as<std::string>();
            std::string label = temp.substr(0,2);

            if("n:" == label) {
                new_attribute = temp;
            } else if("v:" == label) {
                new_value = temp;
            } else if("u:" == label) {
                new_units = temp;
            } else {
                units = temp;
            }
        }

        if ( sub_vm.count( "opt_4" ) ) {
            std::string temp = sub_vm["opt_4"].as<std::string>();
            std::string label = temp.substr(0,2);

            if("n:" == label) {
                new_attribute = temp;
            } else if("v:" == label) {
                new_value = temp;
            } else if("u:" == label) {
                new_units = temp;
            } else {
                units = temp;
            }
        }

        auto [status, name] = prepare_name(sub_vm["object_type"].as<std::string>(),
                                           sub_vm["name"].as<std::string>());

        if (status) {
            return status;
        }

        std::vector<std::string> new_args = reorder_arguments(new_attribute, new_value, new_units);
        status = modAVUMetadata((char*) _cmd.c_str(),
                                (char*) sub_vm["object_type"].as<std::string>().c_str(),
                                name.data(),
                                (char*) sub_vm["attribute"].as<std::string>().c_str(),
                                (char*) sub_vm["value"].as<std::string>().c_str(),
                                (char*) units.c_str(),
                                (char*) new_args[0].c_str(),
                                (char*) new_args[1].c_str(),
                                (char*) new_args[2].c_str() );

        return status < 0 ? status : 0;
    }
    else if (_cmd == "set") {
        // Assign a new value to an existing AVU triple
        std::string new_units = "";

        po::options_description set_desc( "set options" );
        set_desc.add_options()
            ("object_type", po::value<std::string>(), "Object type (-d/-C/-R/-u)")
            ("name", po::value<std::string>(),      "Object name")
            ("attribute", po::value<std::string>(), "Attribute name")
            ("new_value", po::value<std::string>(), "New attribute value")
            ("new_units", po::value<std::string>(), "New attribute units [optional]");

        po::positional_options_description set_pos;
        set_pos.add("object_type", 1).
            add("name", 1).
            add("attribute", 1).
            add("new_value", 1).
            add("new_units", 1);

        po::variables_map sub_vm;
        try {
            sub_vm = parse_sub_args(_sub_args, set_desc, set_pos);
        } catch ( const irods::exception& e ) {
            return e.code();
        }

        if ( !sub_vm.count( "object_type" ) ||
            ( sub_vm["object_type"].as<std::string>() != "-d" &&
             sub_vm["object_type"].as<std::string>() != "-C" &&
             sub_vm["object_type"].as<std::string>() != "-R" &&
             sub_vm["object_type"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "new_value" ) ) {
            std::cerr << "Error: "
                      << "Not enough arguments provided to "
                      << _cmd
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( sub_vm.count( "new_units" ) ) {
            new_units = sub_vm["new_units"].as<std::string>();
        }

        auto [status, name] = prepare_name(sub_vm["object_type"].as<std::string>(),
                                           sub_vm["name"].as<std::string>());

        if (status) {
            return status;
        }

        status = modAVUMetadata((char*) _cmd.c_str(),
                                (char*) sub_vm["object_type"].as<std::string>().c_str(),
                                name.data(),
                                (char*) sub_vm["attribute"].as<std::string>().c_str(),
                                (char*) sub_vm["new_value"].as<std::string>().c_str(),
                                (char*) new_units.c_str(),
                                "",
                                "",
                                "" );

        return status < 0 ? status : 0;
    }
    else if (_cmd == "ls") {
        // List existing AVUs on an iRODS object
        std::string attribute = "";

        po::options_description ls_desc( "ls options" );
        ls_desc.add_options()
            ("object_type", po::value<std::string>(), "Object type (-d/-C/-R/-u)")
            ("name", po::value<std::string>(),      "Object name")
            ("attribute", po::value<std::string>(), "Attribute name");

        po::positional_options_description ls_pos;
        ls_pos.add("object_type", 1).
            add("name", 1).
            add("attribute", 1);

        po::variables_map sub_vm;
        try {
            sub_vm = parse_sub_args(_sub_args, ls_desc, ls_pos);
        } catch ( const irods::exception& e ) {
            return e.code();
        }

        if ( !sub_vm.count( "object_type" ) ||
            ( sub_vm["object_type"].as<std::string>() != "-ld" &&
             sub_vm["object_type"].as<std::string>() != "-d" &&
             sub_vm["object_type"].as<std::string>() != "-lC" &&
             sub_vm["object_type"].as<std::string>() != "-C" &&
             sub_vm["object_type"].as<std::string>() != "-lR" &&
             sub_vm["object_type"].as<std::string>() != "-R" &&
             sub_vm["object_type"].as<std::string>() != "-lu" &&
             sub_vm["object_type"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "name" ) ) {
            std::cerr << "Error: "
                      << "Not enough arguments provided to "
                      << _cmd
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( sub_vm.count( "attribute" ) ) {
            attribute = sub_vm["attribute"].as<std::string>();
        }

        auto [status, name] = prepare_name(sub_vm["object_type"].as<std::string>(),
                                           sub_vm["name"].as<std::string>());

        if (status) {
            return status;
        }

        std::string obj_type = sub_vm["object_type"].as<std::string>();

        if ( obj_type == "-ld") {
            longMode = 1;
            return showDataObj(name.data(), (char*) attribute.c_str());
        }
        else if ( obj_type == "-d" ) {
            return showDataObj(name.data(), (char*) attribute.c_str());
        }
        else if ( obj_type == "-lC" ) {
            longMode = 1;
            return showColl(name.data(), (char*) attribute.c_str());
        }
        else if ( obj_type == "-C" ) {
            return showColl(name.data(), (char*) attribute.c_str());
        }
        else if ( obj_type == "-lR" ) {
            longMode = 1;
            return showResc(name.data(), (char*) attribute.c_str());
        }
        else if ( obj_type == "-R" ) {
            return showResc(name.data(), (char*) attribute.c_str());
        }
        else if ( obj_type == "-lu" ) {
            longMode = 1;
            return showUser(name.data(), (char*) attribute.c_str());
        }
        else {
            return showUser(name.data(), (char*) attribute.c_str());
        }

        return 0;
    }
    else if ( _cmd == "cp" ) {
        // Copy AVUs from one iRODS object to another
        po::options_description cp_desc( "cp options" );
        cp_desc.add_options()
            ("object_type_1", po::value<std::string>(), "First object type (-d/-C/-R/-u)")
            ("object_type_2", po::value<std::string>(), "Second object type (-d/-C/-R/-u")
            ("name_1", po::value<std::string>(),        "First object name")
            ("name_2", po::value<std::string>(),        "Second object name");

        po::positional_options_description cp_pos;
        cp_pos.add("object_type_1", 1).
            add("object_type_2", 1).
            add("name_1", 1).
            add("name_2", 1);

        po::variables_map sub_vm;
        try {
            sub_vm = parse_sub_args(_sub_args, cp_desc, cp_pos);
        } catch ( const irods::exception& e ) {
            return e.code();
        }

        if ( !sub_vm.count( "object_type_1" ) ||
            ( sub_vm["object_type_1"].as<std::string>() != "-d" &&
             sub_vm["object_type_1"].as<std::string>() != "-C" &&
             sub_vm["object_type_1"].as<std::string>() != "-R" &&
             sub_vm["object_type_1"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No first object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "object_type_2" ) ||
            ( sub_vm["object_type_2"].as<std::string>() != "-d" &&
             sub_vm["object_type_2"].as<std::string>() != "-C" &&
             sub_vm["object_type_2"].as<std::string>() != "-R" &&
             sub_vm["object_type_2"].as<std::string>() != "-u" ) ) {
            std::cerr << "Error: "
                      << "No second object type descriptor (-d/C/R/u) specified"
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        if ( !sub_vm.count( "name_2" ) ) {
            std::cerr << "Error: "
                      << "Not enough arguments provided to "
                      << _cmd
                      << std::endl;
            return SYS_INVALID_INPUT_PARAM;
        }

        auto [status_1, name_1] = prepare_name(sub_vm["object_type_1"].as<std::string>(),
                                               sub_vm["name_1"].as<std::string>());

        if (status_1) {
            return status_1;
        }

        auto [status_2, name_2] = prepare_name(sub_vm["object_type_2"].as<std::string>(),
                                               sub_vm["name_2"].as<std::string>());

        if (status_2) {
            return status_2;
        }

        int status = modCopyAVUMetadata( (char*) _cmd.c_str(),
                        (char*) sub_vm["object_type_1"].as<std::string>().c_str(),
                        (char*) sub_vm["object_type_2"].as<std::string>().c_str(),
                        name_1.data(),
                        name_2.data(),
                        "",
                        "",
                        "" );

        return status < 0 ? status : 0;
    } else if ( _cmd == "help" ||
                _cmd == "h" ) {
        return display_help(_sub_args);
    } else if ( _cmd == "quit" ||
                _cmd == "q" ) {
        return -1;
    } else if ( _cmd.empty() ) {
        return -3;
    } else {
        std::cerr << "Unrecognized subcommand '"
                  << _cmd
                  << "', try 'imeta help'"
                  << std::endl;
        return -2;
    }

    return 0;
}

int display_help(const std::vector<std::string>& _sub_args) {
    // Display help for a specified command
    po::options_description help_desc( "help options" );
    help_desc.add_options()
        ("help_command", po::value<std::string>(), "Which command is help for");

    po::positional_options_description help_pos;
    help_pos.add("help_command", 1);

    po::variables_map sub_vm;
    try {
        sub_vm = parse_sub_args(_sub_args, help_desc, help_pos);
    } catch ( const irods::exception& e ) {
        return e.code();
    }

    usage( sub_vm.count( "help_command" ) ? sub_vm["help_command"].as<std::string>().c_str() : "" );
    return 0;
}

int main( int argc, const char **argv )
{
    signal( SIGPIPE, SIG_IGN );

    utils::set_ips_display_name(std::filesystem::path{argv[0]}.filename().c_str());

    rodsLogLevel(LOG_ERROR);

    rodsArguments_t myRodsArgs;
    std::optional<std::tuple<po::parsed_options, po::variables_map>> options_and_map;
    try {
        options_and_map = parse_program_options(argc, argv, myRodsArgs);
    }
    catch (const irods::exception&) {
        std::cout << "Use -h for help\n";
        return 1;
    }

    po::parsed_options parsed = std::get<po::parsed_options>(*options_and_map);
    po::variables_map vm = std::get<po::variables_map>(*options_and_map);

    const bool missing_command = !vm.count( "command" );

    std::vector< std::string > command_to_be_parsed;
    if (missing_command) {
        std::cout << "Error: No command specified.\n";
        usage("");
        return 1;
    }
    else {
        command_to_be_parsed = po::collect_unrecognized( parsed.options, po::include_positional );
    }

    if (command_to_be_parsed.size() > 0 && (command_to_be_parsed[0] == "h" || command_to_be_parsed[0] == "help")) {
        return display_help({++command_to_be_parsed.begin(), command_to_be_parsed.end()});
    }

    int status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "main: getRodsEnv error. status = %d", status );
        return 1;
    }

    strncpy( cwd, myEnv.rodsCwd, BIG_STR );
    if ( strlen( cwd ) == 0 ) {
        strcpy( cwd, "/" );
    }

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::api_entry_table&  api_tbl = irods::get_client_api_table();
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    init_api_table( api_tbl, pk_tbl );

    rErrMsg_t errMsg;
    Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );

    if ( Conn == NULL ) {
        char *mySubName = NULL;
        const char *myName = rodsErrorName( errMsg.status, &mySubName );
        rodsLog( LOG_ERROR, "rcConnect failure %s (%s) (%d) %s", myName, mySubName, errMsg.status, errMsg.msg );
        free( mySubName );

        return 2;
    }

    const auto disconnect = irods::at_scope_exit{[] { rcDisconnect(Conn); }};

    utils::warn_if_connected_to_potentially_incompatible_server(*Conn);

    if (utils::authenticate_client(Conn, myEnv) != 0) {
        print_error_stack_to_file(Conn->rError, stderr);
        return 3;
    }

    lastCommandStatus = do_command( command_to_be_parsed[0], { ++command_to_be_parsed.begin(), command_to_be_parsed.end() });

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
        "Usage: imeta [-vVMhz] [command]",
        " -v verbose",
        " -V Very verbose",
        " -M Admin mode",
        " -z Zonename  work with the specified Zone",
        " -h This help",
        " ",
        "Commands are:",
        " add  -d|C|R|u Name AttName AttValue [AttUnits] (Add new AVU triple)",
        " rm   -d|C|R|u Name AttName AttValue [AttUnits] (Remove AVU)",
        " rmi  -d|C|R|u Name MetadataID (Remove AVU by MetadataID)",
        " mod  -d|C|R|u Name AttName AttValue [AttUnits] [n:Name] [v:Value] [u:Units]",
        "      (modify AVU; new name (n:), value(v:), and/or units(u:)",
        " set  -d|C|R|u Name AttName newValue [newUnits] (Assign a single value)",
        " ls   -[l]d|C|R|u Name [AttName] (List existing AVUs for item Name)",
        " cp   -d|C|R|u -d|C|R|u Name1 Name2 (Copy AVUs from item Name1 to Name2)",
        " ",
        "Metadata attribute-value-units triples (AVUs) consist of an Attribute-Name,",
        "Attribute-Value, and an optional Attribute-Units.  They can be added",
        "via the 'add' command (and in other ways), and",
        "then queried to find matching objects.",
        " ",
        "For each command, -d, -C, -R, or -u is used to specify which type of",
        "object to work with: dataobjs (iRODS files), collections, resources,",
        "or users.",
        " ",
        "A user must be a rodsadmin to set or modify AVUs on resources and users.",
        " ",
        "Fields represented with upper case, such as Name, are entered values.  For",
        "example, 'Name' is the name of a dataobject, collection, resource,",
        "or user.",
        " ",
        "Single or double quotes can be used to enter items with blanks.",
        " ",
        "Entered usernames are of the form username[#zone].  If #zone is not",
        "provided, the zone from your irods_environment.json is assumed.",
        " ",
        "The appropriate zone (local or remote) is determined from the path names",
        "or via -z Zonename (when working with resources).",
        " ",
        "If you are an iRODS administrator, you can include the -M option to run",
        "in administrator mode and add, remove, or set metadata on any collection,",
        "data object, user, or resource.",
        " ",
        "Try 'help command' for more help on a specific command.",
        ""
    };
    int i;
    for ( i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            break;
        }
        printf( "%s\n", msgs[i] );
    }
    printReleaseInfo( "imeta" );
}

/*
Print either main usage/help information, or some more specific
information on particular commands.
 */
int
usage( const char *subOpt ) {
    int i;
    if ( *subOpt == '\0' ) {
        usageMain();
    }
    else {
        if ( strcmp( subOpt, "add" ) == 0 ) {
            char *msgs[] = {
                " add -d|C|R|u Name AttName AttValue [AttUnits]  (Add new AVU triple)",
                "Add an AVU to a dataobj (-d), collection(-C), resource(-R), ",
                "or user(-u)",
                "Example: add -d file1 distance 12 miles",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "rm" ) == 0 ) {
            char *msgs[] = {
                " rm  -d|C|R|u Name AttName AttValue [AttUnits] (Remove AVU)",
                "Remove an AVU from a dataobj (-d), collection(-C), resource(-R),",
                "or user(-u)",
                "Example: rm -d file1 distance 12 miles",
                "An AttUnits value must be included if it was when the AVU was added.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "rmi" ) == 0 ) {
            char *msgs[] = {
                " rmi -d|C|R|u Name MetadataID (Remove AVU by MetadataID)",
                "Remove an AVU from a dataobj (-d), collection(-C), resource(-R),",
                "or user(-u) by name and metadataID from the catalog.  Intended for",
                "expert use.  There is no matching done to confirm correctness of",
                "a request (but permissions are still checked).  Use with caution.",
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
                " mod -d|C|R|u Name AttName AttValue [AttUnits] [n:Name] [v:Value] [u:Units]",
                "      (modify AVU; new name (n:), value(v:), and/or units(u:)",
                "Modify a defined AVU for the specified item (object)",
                "Example: mod -d file1 distance 14 miles v:27",
                " ",
                "Only the AVU associated with this object is modified.  Internally, the system",
                "removes the old AVU from the object and creates a new one (ensuring",
                "consistency), and performs a single 'commit' if all is valid.",
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
                " ls -d|C|R|u Name [AttName] (List existing AVUs for item Name)",
                " ls -ld Name [AttName]        (List in long format)",
                "List defined AVUs for the specified item",
                "Example: ls -d file1",
                "If the optional AttName is included, it is the attribute name",
                "you wish to list and only those will be listed.",
                "If the optional -l is used on dataObjects (-ld), the long format will",
                "be displayed which includes the time the AVU was set.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "set" ) == 0 ) {
            char *msgs[] = {
                " set -d|C|R|u Name AttName newValue [newUnits]  (assign a single value)",
                "Set the newValue (and newUnit) of an AVU of a dataobj (-d), collection(-C),",
                "     resource(-R) or user(-u).",
                " ",
                "'set' modifies an AVU if it exists, or creates one if it does not.",
                "If the AttName does not exist, or is used by multiple objects, the AVU for",
                "this object is added.",
                "If the AttName is used only by this one object, the AVU (row) is modified",
                "with the new values, reducing the database overhead (unused rows).",
                "Example: set -d file1 distance 12",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "cp" ) == 0 ) {
            char *msgs[] = {
                " cp -d|C|R|u -d|C|R|u Name1 Name2 (Copy AVUs from item Name1 to Name2)",
                "Example: cp -d -C file1 dir1",
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

po::variables_map parse_sub_args(const std::vector<std::string>& _sub_args,
                                 const po::options_description& _desc,
                                 const po::positional_options_description& _pos)
{
    try {
        po::variables_map sub_vm;
        po::store(
            po::command_line_parser(_sub_args)
                .options(_desc)
                .positional(_pos)
                .style(po::command_line_style::unix_style ^ po::command_line_style::allow_short)
                .allow_unregistered()
                .run(),
            sub_vm);
        po::notify(sub_vm);

        return sub_vm;
    }
    catch (po::error& _e) {
        std::cerr << "Error: "
                  << _e.what()
                  << std::endl;
        THROW( SYS_INVALID_INPUT_PARAM, _e.what() );
    }
}
