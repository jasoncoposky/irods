#include "irods/rcMisc.h"
#include "irods/genQuery.h"
#include "irods/genquery1_ast_adapter.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/icatHighLevelRoutines.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/irods_lexical_cast.hpp"
#include "irods/irods_resource_manager.hpp"
#include "irods/irods_rs_comm_query.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/miscServerFunct.hpp"
#include "irods/miscUtil.h"
#include "irods/private/genquery2_sql.hpp"
#include "irods/rodsErrorTable.h"
#include "irods/rodsGenQueryNames.h"
#include "irods/rsGenQuery.hpp"
#include "irods/rsGetRescQuota.hpp"
#include "irods/rsGlobalExtern.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <regex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace {
    using log_api = irods::experimental::log::api;

    std::string
    get_column_name(int j) {
        const int n = sizeof(columnNames)/sizeof(columnNames[0]);
        for (int i=0; i<n; ++i) {
            if (columnNames[i].columnId == j) {
                return std::string(columnNames[i].columnName);
            }
        }

        std::stringstream ss;
        ss << j;
        return std::string("COLUMN_NAME_NOT_FOUND_") + ss.str();
    }

    struct option_element{
        int key;
        const char* cpp_macro;
        const char* token;
    };

    option_element queryWideOptionsMap[] = {
        {RETURN_TOTAL_ROW_COUNT, "RETURN_TOTAL_ROW_COUNT", "return_total_row_count"},
        {NO_DISTINCT,            "NO_DISTINCT",            "no_distinct"},
        {QUOTA_QUERY,            "QUOTA_QUERY",            "quota_query"},
        {AUTO_CLOSE,             "AUTO_CLOSE",             "auto_close"},
        {UPPER_CASE_WHERE,       "UPPER_CASE_WHERE",       "upper_case_where"}
    };

    option_element selectInpOptionsMap[] = {
        {ORDER_BY,      "ORDER_BY",      "order"},
        {ORDER_BY_DESC, "ORDER_BY_DESC", "order_desc"}
    };

    option_element selectInpFunctionMap[] = {
        {SELECT_MIN,   "SELECT_MIN",   "min"},
        {SELECT_MAX,   "SELECT_MAX",   "max"},
        {SELECT_SUM,   "SELECT_SUM",   "sum"},
        {SELECT_AVG,   "SELECT_AVG",   "avg"},
        {SELECT_COUNT, "SELECT_COUNT", "count"}
    };

    std::string
    format_selected_column(int columnIndex, int columnOption) {
        std::string ret = get_column_name(columnIndex);
        if (columnOption == 0 || columnOption == 1) {
            return ret;
        }

        for (size_t i=0; i<sizeof(selectInpOptionsMap)/sizeof(selectInpOptionsMap[0]); ++i) {
            if (columnOption == selectInpOptionsMap[i].key) {
                ret = std::string(selectInpOptionsMap[i].token) + "(" + ret + ")";
                return ret;
            }
        }
        for (size_t i=0; i<sizeof(selectInpFunctionMap)/sizeof(selectInpFunctionMap[0]); ++i) {
            if (columnOption == selectInpFunctionMap[i].key) {
                ret = std::string(selectInpFunctionMap[i].token) + "(" + ret + ")";
                return ret;
            }
        }

        std::stringstream ss;
        ss << columnOption;
        ret = std::string("combo_func_") + ss.str() + "(" + ret + ")";
        return ret;
    }

    template <typename OutStream>
    void
    insert_genquery_inp_into_stream(const genQueryInp_t *genQueryInp, OutStream& f) {
        f << "maxRows: " << genQueryInp->maxRows << " continueInx: " << genQueryInp->continueInx << " rowOffset: " << genQueryInp->rowOffset << '\n';
        f << "options: " << genQueryInp->options;
        for (size_t i=0; i<sizeof(queryWideOptionsMap)/sizeof(queryWideOptionsMap[0]); ++i) {
            if (genQueryInp->options & queryWideOptionsMap[i].key) {
                f << " " << queryWideOptionsMap[i].cpp_macro;
            }
        }
        f << '\n';

        f << "selectInp.len: " << genQueryInp->selectInp.len << '\n';
        for (int i=0; i<genQueryInp->selectInp.len; ++i) {
            f << "    column: " << genQueryInp->selectInp.inx[i] << " " << get_column_name(genQueryInp->selectInp.inx[i]) << '\n';
            f << "    options: " << genQueryInp->selectInp.value[i];
            for (size_t j=0; j<sizeof(selectInpOptionsMap)/sizeof(selectInpOptionsMap[0]); ++j) {
                if (genQueryInp->selectInp.value[i] & selectInpOptionsMap[j].key) {
                    f << " " << selectInpOptionsMap[j].cpp_macro;
                }
            }
            for (size_t j=0; j<sizeof(selectInpFunctionMap)/sizeof(selectInpFunctionMap[0]); ++j) {
                if ((genQueryInp->selectInp.value[i] & 0x7) == selectInpFunctionMap[j].key) {
                    f << " " << selectInpFunctionMap[j].cpp_macro;
                }
            }
            f << '\n';
        }

        f << "sqlCondInp.len: " << genQueryInp->sqlCondInp.len << '\n';
        for (int i=0; i<genQueryInp->sqlCondInp.len; ++i) {
            f << "    column: " << genQueryInp->sqlCondInp.inx[i] << " " << get_column_name(genQueryInp->sqlCondInp.inx[i]) << '\n';
            f << "    condition: " << genQueryInp->sqlCondInp.value[i] << '\n';
        }
        f << '\n';
    }

    auto translate_single_data_resc_hier_condition_to_resc_id(const std::string& _cond) -> std::string
    {
        // The default return string will yield 0 results when run in a query because RESC_ID is never '0'.
        constexpr const char* default_condition_str = "='0'";

        const auto open_quote_pos = _cond.find_first_of('\'');
        const auto close_quote_pos = _cond.find_last_of('\'');
        if (close_quote_pos == open_quote_pos) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_INVALID_INPUT_PARAM, fmt::format("Invalid condition: [{}]", _cond));
        }

        const std::string op = _cond.substr(0, open_quote_pos);

        // Only allow =, !=, LIKE, and NOT LIKE to be specified. Check = first because it is used in some
        // critical path operations and will only match 1 result, so it should be quick.
        static const std::regex equal_regex{R"_(^\s*=\s*$)_"};
        static const std::regex like_regex{R"_(^\s*(LIKE|like)\s*$)_"};
        static const std::regex not_like_regex{R"_(^\s*(NOT|not)\s+(LIKE|like)\s*$)_"};
        static const std::regex not_equal_regex{R"_(^\s*!=\s*$)_"};
        bool equal_op = false;
        bool like_op = false;
        bool not_like_op = false;
        bool not_equal_op = false;

        if (std::regex_match(op.c_str(), equal_regex)) {
            equal_op = true;
        }
        else if (std::regex_match(op.c_str(), like_regex)) {
            like_op = true;
        }
        else if (std::regex_match(op.c_str(), not_like_regex)) {
            not_like_op = true;
        }
        else if (std::regex_match(op.c_str(), not_equal_regex)) {
            not_equal_op = true;
        }

        if (!equal_op && !like_op && !not_like_op && !not_equal_op) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(CAT_INVALID_ARGUMENT, fmt::format("Invalid condition: [{}]", _cond));
        }

        // Generate a regex by replacing GenQuery wildcards with regex wildcards.
        std::string hier_regex_str = _cond.substr(open_quote_pos + 1, close_quote_pos - open_quote_pos - 1);
        if (!equal_op && !not_equal_op) {
            auto wildcard_pos = hier_regex_str.find_first_of('%');
            while (std::string::npos != wildcard_pos) {
                hier_regex_str.replace(wildcard_pos, 1, ".*");
                wildcard_pos = hier_regex_str.find_first_of('%');
            }
        }
        const std::regex hier_regex{hier_regex_str};

        // If LIKE or = are used, this means that the WHERE clause is looking for the string to match. If a resource
        // matches the regex and we want to include resources which match, then that resource ID is included in the
        // list of leaf resource IDs. If neither LIKE nor = are used, this means that the WHERE clause is looking for
        // strings which do NOT match. If a resource does NOT match the regex and we want to include resources which
        // do NOT match, then that resource ID is included in the list of leaf resource IDs.
        const auto want_to_match = like_op || equal_op;
        std::vector<std::string> leaf_ids;
        try {
            for (const auto& hier : resc_mgr.get_all_resc_hierarchies()) {
                const auto match_found = std::regex_match(hier, hier_regex);
                if ((match_found && want_to_match) || (!match_found && !want_to_match)) {
                    // Pre-format IDs such that they are ready to use with the GenQuery IN clause.
                    leaf_ids.push_back(fmt::format("'{}'", resc_mgr.hier_to_leaf_id(hier)));
                }
            }
        }
        catch (const irods::exception& e) {
            // Need to handle irods::exceptions here because get_all_resc_hierarchies throws when a structured file is
            // being operated upon. Somehow, a resource with ID -1 is found in the resource map with structured files.
            // Historically, this has not been treated as an error in this case, so just return the default condition,
            // which will produce no results when the query is run.
            log_api::debug("[{}:{}] - irods::exception occurred: [{}]", __func__, __LINE__, e.client_display_what());
            return default_condition_str;
        }

        if (leaf_ids.empty()) {
            return default_condition_str;
        }

        return fmt::format("IN ({})", fmt::join(leaf_ids, ","));
    } // translate_single_data_resc_hier_condition_to_resc_id

    auto translate_data_resc_hier_where_clause_to_resc_id(const std::string& _condition) -> std::string
    {
        std::string condition_str;

        // For each || operator found, take the condition immediately preceding and translate it from the DATA_RESC_HIER
        // condition to the equivalent RESC_ID condition.
        auto previous_or_position = std::string_view::size_type{};
        for (auto or_position = _condition.find("||", previous_or_position); std::string_view::npos != or_position;
             or_position = _condition.find("||", previous_or_position))
        {
            condition_str += translate_single_data_resc_hier_condition_to_resc_id(
                _condition.substr(previous_or_position, or_position));
            condition_str += " || ";

            // Advance the previous position tracker to the current || position, plus 2 so that the next condition to
            // translate does not include the || itself.
            previous_or_position = or_position + 2;
        }

        // There should be one condition found after the last || operator has been found which will need to be
        // translated because the loop above is translating conditions BEFORE the || operator. || is a binary operation,
        // so there will necessarily be one left over at the end.
        return condition_str +=
               translate_single_data_resc_hier_condition_to_resc_id(_condition.substr(previous_or_position));
    } // translate_data_resc_hier_where_clause_to_resc_id
} // anonymous namespace

std::string
genquery_inp_to_diagnostic_string(const genQueryInp_t *q) {
    std::stringstream ss;
    insert_genquery_inp_into_stream(q, ss);
    return ss.str();
}

std::string
genquery_inp_to_iquest_string(const genQueryInp_t *q) {
    // TODO: handle queryWideOptionsMap
    std::stringstream ss;
    ss << "select ";
    {
        const int n = q->selectInp.len;
        if (n<=0) {
            ss << "ERROR: 0 columns selected, printing query contents instead of iquest string\n";
            insert_genquery_inp_into_stream(q, ss);
            return ss.str();
        }

        ss << format_selected_column(q->selectInp.inx[0], q->selectInp.value[0]);
        for (int i=1; i<n; ++i) {
            ss << ", ";
            ss << format_selected_column(q->selectInp.inx[i], q->selectInp.value[i]);
        }
    }

    {
        const int n = q->sqlCondInp.len;
        if (n>0) {
            ss << " where ";
            ss << get_column_name(q->sqlCondInp.inx[0]) << " " << q->sqlCondInp.value[0];
            for (int i=1; i<n; ++i) {
                ss << " and ";
                ss << get_column_name(q->sqlCondInp.inx[i]) << " " << q->sqlCondInp.value[i];
            }
        }
    }
    return ss.str();
}

irods::GenQueryInpWrapper::GenQueryInpWrapper(void) {
    memset(&genquery_inp_, 0, sizeof(genquery_inp_));
}
irods::GenQueryInpWrapper::~GenQueryInpWrapper(void) {
    clearGenQueryInp(&genquery_inp_);
}
genQueryInp_t& irods::GenQueryInpWrapper::get(void) {
    return genquery_inp_;
}
irods::GenQueryOutPtrWrapper::GenQueryOutPtrWrapper(void) {
    genquery_out_ptr_ = nullptr;
}
irods::GenQueryOutPtrWrapper::~GenQueryOutPtrWrapper(void) {
    freeGenQueryOut(&genquery_out_ptr_);
}
genQueryOut_t*& irods::GenQueryOutPtrWrapper::get(void) {
    return genquery_out_ptr_;
}


static
irods::error strip_new_query_terms(
    genQueryInp_t* _inp ) {
    // =-=-=-=-=-=-=-
    // cache pointers to the incoming inxIvalPair
    inxIvalPair_t tmp;
    tmp.len   = _inp->selectInp.len;
    tmp.inx   = _inp->selectInp.inx;
    tmp.value = _inp->selectInp.value;

    // =-=-=-=-=-=-=-
    // zero out the selectInp to copy
    // fresh community indices and values
    std::memset(&_inp->selectInp, 0, sizeof(_inp->selectInp));

    // =-=-=-=-=-=-=-
    // iterate over the tmp and only copy community values
    for ( int i = 0; i < tmp.len; ++i ) {
        if ( tmp.inx[ i ] == COL_R_RESC_CHILDREN ||
                tmp.inx[ i ] == COL_R_RESC_CONTEXT  ||
                tmp.inx[ i ] == COL_R_RESC_PARENT   ||
                tmp.inx[ i ] == COL_R_RESC_PARENT_CONTEXT   ||
                tmp.inx[ i ] == COL_D_RESC_HIER ) {
            continue;
        }
        else {
            addInxIval( &_inp->selectInp, tmp.inx[ i ], tmp.value[ i ] );
        }

    } // for i

    return SUCCESS();

} // strip_new_query_terms


static
irods::error strip_resc_grp_name_from_query_inp( genQueryInp_t* _inp, int& _pos ) {

    const int COL_D_RESC_GROUP_NAME  = 408;

    // sanity check
    if ( !_inp ) {
        return CODE( SYS_INTERNAL_NULL_INPUT_ERR );
    }

    // =-=-=-=-=-=-=-
    // cache pointers to the incoming inxIvalPair
    inxIvalPair_t tmp;
    tmp.len   = _inp->selectInp.len;
    tmp.inx   = _inp->selectInp.inx;
    tmp.value = _inp->selectInp.value;

    // =-=-=-=-=-=-=-
    // zero out the selectInp to copy
    // fresh indices and values
    std::memset(&_inp->selectInp, 0, sizeof(_inp->selectInp));

    // =-=-=-=-=-=-=-
    // iterate over tmp and replace resource group with resource name
    for ( int i = 0; i < tmp.len; ++i ) {
        if ( tmp.inx[i] == COL_D_RESC_GROUP_NAME ) {
            addInxIval( &_inp->selectInp, COL_D_RESC_NAME, tmp.value[i] );
            _pos = i;
        }
        else {
            addInxIval( &_inp->selectInp, tmp.inx[i], tmp.value[i] );
        }
    } // for i

    // cleanup
    if ( tmp.inx ) { free( tmp.inx ); }
    if ( tmp.value ) { free( tmp.value ); }

    return SUCCESS();

} // strip_resc_grp_name_from_query_inp


static
irods::error add_resc_grp_name_to_query_out( genQueryOut_t *_out, int& _pos ) {

    const int COL_D_RESC_GROUP_NAME  = 408;

    // =-=-=-=-=-=-=-
    // Sanity checks
    if ( !_out ) {
        return CODE( SYS_INTERNAL_NULL_INPUT_ERR );
    }

    if ( _pos < 0 || _pos > MAX_SQL_ATTR - 1 ) {
        return CODE( SYS_INVALID_INPUT_PARAM );
    }

    sqlResult_t *sqlResult = &_out->sqlResult[_pos];
    if ( !sqlResult || sqlResult->attriInx != COL_D_RESC_NAME ) {
        return CODE( SYS_INTERNAL_ERR );
    }

    // =-=-=-=-=-=-=-
    // Swap attribute indices back
    sqlResult->attriInx = COL_D_RESC_GROUP_NAME;

    return SUCCESS();

} // add_resc_grp_name_to_query_out

static
irods::error strip_resc_hier_name_from_query_inp( genQueryInp_t* _inp, int& _pos ) {
    // sanity check
    if ( !_inp ) {
        return CODE( SYS_INTERNAL_NULL_INPUT_ERR );
    }

    // =-=-=-=-=-=-=-
    // cache pointers to the incoming inxIvalPair
    inxIvalPair_t tmpI;
    tmpI.len   = _inp->selectInp.len;
    tmpI.inx   = _inp->selectInp.inx;
    tmpI.value = _inp->selectInp.value;

    // =-=-=-=-=-=-=-
    // zero out the selectInp to copy
    // fresh indices and values
    std::memset(&_inp->selectInp, 0, sizeof(_inp->selectInp));

    // =-=-=-=-=-=-=-
    // iterate over tmp and replace resource group with resource name
    for ( int i = 0; i < tmpI.len; ++i ) {
        if ( tmpI.inx[i] == COL_D_RESC_HIER ) {
            addInxIval( &_inp->selectInp, COL_D_RESC_ID, tmpI.value[i] );
            _pos = i;
        }
        else {
            addInxIval( &_inp->selectInp, tmpI.inx[i], tmpI.value[i] );
        }
    } // for i

    // cleanup
    if ( tmpI.inx ) { free( tmpI.inx ); }
    if ( tmpI.value ) { free( tmpI.value ); }


    // =-=-=-=-=-=-=-
    // cache pointers to the incoming inxIvalPair
    inxValPair_t tmpV;
    const auto cleanup = irods::at_scope_exit{[&tmpV] { clearInxVal(&tmpV); }};
    tmpV.len   = _inp->sqlCondInp.len;
    tmpV.inx   = _inp->sqlCondInp.inx;
    tmpV.value = _inp->sqlCondInp.value;

    const auto inxs = std::span{tmpV.inx, static_cast<std::size_t>(tmpV.len)};
    const auto vals = std::span{tmpV.value, static_cast<std::size_t>(tmpV.len)};

    // =-=-=-=-=-=-=-
    // zero out the selectInp to copy
    // fresh indices and values
    std::memset(&_inp->sqlCondInp, 0, sizeof(_inp->selectInp));

    try {
        // iterate over tmp and replace resource group with resource name
        for (int i = 0; i < tmpV.len; ++i) {
            const int inx = inxs[i];
            const char* val = vals[i];
            if (inx == COL_D_RESC_HIER) {
                const auto new_cond = translate_data_resc_hier_where_clause_to_resc_id(val);
                addInxVal(&_inp->sqlCondInp, COL_D_RESC_ID, new_cond.empty() ? "='0'" : new_cond.c_str());
            }
            else {
                addInxVal(&_inp->sqlCondInp, inx, val);
            }
        }
    }
    catch (const irods::exception& e) {
        return ERROR(e.code(), e.client_display_what());
    }

    return SUCCESS();
} // strip_resc_hier_name_from_query_inp


static
irods::error add_resc_hier_name_to_query_out( genQueryOut_t *_out, int& _pos ) {
    // =-=-=-=-=-=-=-
    // Sanity checks
    if ( !_out ) {
        return CODE( SYS_INTERNAL_NULL_INPUT_ERR );
    }

    if ( _pos < 0 || _pos > MAX_SQL_ATTR - 1 ) {
        return CODE( SYS_INVALID_INPUT_PARAM );
    }

    sqlResult_t *sqlResult = &_out->sqlResult[_pos];
    if ( !sqlResult || sqlResult->attriInx != COL_D_RESC_ID ) {
        return CODE( SYS_INTERNAL_ERR );
    }

    // =-=-=-=-=-=-=-
    // Swap attribute indices back
    sqlResult->attriInx = COL_D_RESC_HIER;

    // =-=-=-=-=-=-=-
    // cache hier strings for leaf id
    size_t max_len = 0;
    std::vector<std::string> resc_hiers(_out->rowCnt);
    for ( int i = 0; i < _out->rowCnt; ++i ) {
        char* leaf_id_str = &sqlResult->value[i*sqlResult->len];

        rodsLong_t leaf_id = 0;
        irods::error ret = irods::lexical_cast<rodsLong_t>(
                               leaf_id_str,
                               leaf_id);
        if(!ret.ok()) {
            irods::log(PASS(ret));
            continue;
        }

        std::string hier;
        ret = resc_mgr.leaf_id_to_hier( leaf_id, resc_hiers[i] );
        if(!ret.ok()) {
            irods::log(PASS(ret));
            continue;
        }

        if(resc_hiers[i].size() > max_len ) {
            max_len = resc_hiers[i].size();
        }

    } // for i

    free( sqlResult->value );

    // =-=-=-=-=-=-=-
    // craft new result string with the hiers
    sqlResult->len = max_len+1;
    sqlResult->value = (char*)malloc(sqlResult->len*_out->rowCnt);
    for( std::size_t i = 0; i < resc_hiers.size(); ++i ) {
        snprintf(
            &sqlResult->value[i*sqlResult->len],
            sqlResult->len,
            "%s",
            resc_hiers[i].c_str() );
    }

    return SUCCESS();

} // add_resc_hier_name_to_query_out

static auto extract_zone_from_hint(std::string_view _zone_hint) -> std::string_view
{
    const auto start = _zone_hint.find_first_not_of('/');
    if (start == std::string_view::npos) {
        return {};
    }
    const auto end = _zone_hint.find('/', start);
    return _zone_hint.substr(start, end - start);
}

static auto iequals(std::string_view _lhs, std::string_view _rhs) -> bool
{
    return std::equal(_lhs.begin(), _lhs.end(), _rhs.begin(), _rhs.end(), [](unsigned char a, unsigned char b) {
        return std::tolower(a) == std::tolower(b);
    });
}

static bool is_zone_name_valid(const std::string& _zone_hint)
{
    const auto zone_name = extract_zone_from_hint(_zone_hint);
    if (zone_name.empty()) {
        return false;
    }

    for (const auto* tmp_zone = ZoneInfoHead; tmp_zone; tmp_zone = tmp_zone->next) {
        if (iequals(zone_name, tmp_zone->zoneName)) {
            return true;
        }
    }

    return false;
} // is_zone_name_valid

static
irods::error process_query_terms_for_pre_irods4_server(const std::string& _zone_hint,
                                                       genQueryInp_t* _inp)
{
    const auto zone_name = extract_zone_from_hint(_zone_hint);
    if (zone_name.empty()) {
        return ERROR(SYS_INVALID_ZONE_NAME, "No zone name parsed from zone hint");
    }

    const zoneInfo_t* tmp_zone = ZoneInfoHead;

    // grind through the zones and find the match to the kw
    while (tmp_zone) {
        // clang-format off
        if (iequals(zone_name, tmp_zone->zoneName) &&
            tmp_zone->primaryServerHost->conn &&
            tmp_zone->primaryServerHost->conn->svrVersion &&
            tmp_zone->primaryServerHost->conn->svrVersion->cookie < 301)
        // clang-format on
        {
            return strip_new_query_terms(_inp);
        }

        tmp_zone = tmp_zone->next;
    }

    return SUCCESS();
} // process_query_terms_for_pre_irods4_server

#if 0 // debug code
int
print_gen_query_input(
    genQueryInp_t *genQueryInp ) {
    if(!genQueryInp) return 0;

    int i = 0, len = 0;
    int *ip1 = 0, *ip2 = 0;
    char *cp = 0;
    char **cpp = 0;

    printf( "maxRows=%d\n", genQueryInp->maxRows );

    len = genQueryInp->selectInp.len;
    printf( "sel len=%d\n", len );

    ip1 = genQueryInp->selectInp.inx;
    ip2 = genQueryInp->selectInp.value;
    if( ip1 && ip2 ) {
            for ( i = 0; i < len; i++ ) {
                printf( "sel inx [%d]=%d\n", i, *ip1 );
                printf( "sel val [%d]=%d\n", i, *ip2 );
                ip1++;
                ip2++;
            }
    }

    len = genQueryInp->sqlCondInp.len;
    printf( "sqlCond len=%d\n", len );
    ip1 = genQueryInp->sqlCondInp.inx;
    cpp = genQueryInp->sqlCondInp.value;
    if(cpp) {
        cp = *cpp;
    }
    if( ip1 && cp ) {
            for ( i = 0; i < len; i++ ) {
                printf( "sel inx [%d]=%d\n", i, *ip1 );
                printf( "sel val [%d]=:%s:\n", i, cp );
                ip1++;
                cpp++;
                cp = *cpp;
            }
    }

    return 0;
}
#endif

/* can be used for debug: */
/* extern int printGenQI( genQueryInp_t *genQueryInp); */

int
rsGenQuery( rsComm_t *rsComm, genQueryInp_t *genQueryInp,
            genQueryOut_t **genQueryOut ) {

    rodsServerHost_t *rodsServerHost;
    int status;
    char *zoneHint;
    zoneHint = getZoneHintForGenQuery(genQueryInp);

    std::string zone_hint_str;
    if (zoneHint != nullptr) {
        zone_hint_str = zoneHint;

        // clean up path separator(s) and trailing '
        if (zone_hint_str.starts_with('/')) {
            zone_hint_str.erase(zone_hint_str.cbegin());

            const std::string::size_type pos = zone_hint_str.find_first_of('/');
            if (std::string::npos != pos) {
                zone_hint_str.erase(pos, zone_hint_str.size() - pos);
            }
        }
        if (zone_hint_str.ends_with('\'')) {
            zone_hint_str.pop_back();
        }
    }

    status = getAndConnRcatHost(rsComm, SECONDARY_RCAT, zone_hint_str.c_str(), &rodsServerHost);

    if ( status < 0 ) {
        return status;
    }

    if (!zone_hint_str.empty()) {
        // If the ZONE_KW is set, that means the client targeted a specific zone.
        // The zone name check only applies to this case. Checking the validity of
        // the zone name must be skipped when getZoneHintForGenQuery() derives it.
        if (getValByKey(&genQueryInp->condInput, ZONE_KW)) {
            if (!is_zone_name_valid(zone_hint_str)) {
                log_api::error("[{}:{}] - Unknown zone name [{}].", __func__, __LINE__, zone_hint_str.c_str());
                return SYS_INVALID_ZONE_NAME;
            }
        }

        // Handle connections with iRODS 3 or earlier.
        irods::error ret = process_query_terms_for_pre_irods4_server( zone_hint_str, genQueryInp );
        if (!ret.ok()) {
            irods::log(PASS(ret));
        }
    }

    if ( rodsServerHost->localFlag == LOCAL_HOST ) {
        std::string svc_role;
        irods::error ret = get_catalog_service_role(svc_role);
        if(!ret.ok()) {
            irods::log(PASS(ret));
            return ret.code();
        }

        if( irods::KW_CFG_SERVICE_ROLE_PROVIDER == svc_role ) {
            status = _rsGenQuery( rsComm, genQueryInp, genQueryOut );
        } else if( irods::KW_CFG_SERVICE_ROLE_CONSUMER == svc_role ) {
            rodsLog( LOG_NOTICE,
                     "rsGenQuery error. RCAT is not configured on this host" );
            return SYS_NO_RCAT_SERVER_ERR;
        } else {
            rodsLog(
                LOG_ERROR,
                "role not supported [%s]",
                svc_role.c_str() );
            status = SYS_SERVICE_ROLE_NOT_SUPPORTED;
        }
    }
    else {
        // =-=-=-=-=-=-=-
        // strip disable strict acl flag if the agent conn flag is missing
        char* dis_kw = getValByKey( &genQueryInp->condInput, DISABLE_STRICT_ACL_KW );
        if ( dis_kw ) {
            if (!irods::server_property_exists(irods::AGENT_CONN_KW)) {
                rmKeyVal( &genQueryInp->condInput, DISABLE_STRICT_ACL_KW );
            }
        } // if dis_kw

        status = rcGenQuery( rodsServerHost->conn,
                             genQueryInp, genQueryOut );
    }
    if ( status < 0  && status != CAT_NO_ROWS_FOUND ) {
        std::string prefix = ( rodsServerHost->localFlag == LOCAL_HOST ) ? "_rs" : "rc";
        rodsLog( LOG_NOTICE,
                 "rsGenQuery: %sGenQuery failed, status = %d", prefix.c_str(), status );
    }
    return status;
}

namespace {
    struct continuation_state
    {
        std::size_t next_offset = 0;
        int total_row_count = 0;
    };

    std::mutex g_continuation_mutex;
    std::unordered_map<int, continuation_state> g_continuations;
    int g_next_continue_inx = 1;

    auto allocate_continue_inx(std::size_t _next_offset, int _total_row_count) -> int
    {
        std::lock_guard lock{g_continuation_mutex};
        int inx = g_next_continue_inx++;
        if (g_next_continue_inx <= 0) {
            g_next_continue_inx = 1;
        }
        g_continuations[inx] = continuation_state{_next_offset, _total_row_count};
        return inx;
    }

    auto get_continuation(int _inx) -> std::optional<continuation_state>
    {
        std::lock_guard lock{g_continuation_mutex};
        if (const auto it = g_continuations.find(_inx); it != g_continuations.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    void update_continuation(int _inx, std::size_t _next_offset, int _total_row_count)
    {
        std::lock_guard lock{g_continuation_mutex};
        g_continuations[_inx] = continuation_state{_next_offset, _total_row_count};
    }

    void erase_continuation(int _inx)
    {
        std::lock_guard lock{g_continuation_mutex};
        g_continuations.erase(_inx);
    }

    auto extract_quota_literal(std::string_view _cond) -> std::string
    {
        auto trim = [](std::string_view _s) noexcept {
            while (!_s.empty() && std::isspace(static_cast<unsigned char>(_s.front()))) {
                _s.remove_prefix(1);
            }
            while (!_s.empty() && std::isspace(static_cast<unsigned char>(_s.back()))) {
                _s.remove_suffix(1);
            }
            return _s;
        };

        auto s = trim(_cond);
        if (!s.empty() && s.front() == '=') {
            s.remove_prefix(1);
            s = trim(s);
        }
        if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
            s = s.substr(1, s.size() - 2);
        }
        return std::string{s};
    }

    auto check_cond_input_access(
        rsComm_t& _comm,
        const genQueryInp_t& _inp,
        const nlohmann::json& _rows) -> int
    {
        if (_inp.condInput.len <= 0) {
            return 0;
        }

        if (getValByKey(&_inp.condInput, ADMIN_KW) != nullptr) {
            return 0;
        }

        if (_inp.condInput.len == 1 &&
            _inp.condInput.keyWord[0] != nullptr &&
            std::strcmp(_inp.condInput.keyWord[0], ZONE_KW) == 0)
        {
            return 0;
        }

        const char* user_name = getValByKey(&_inp.condInput, USER_NAME_CLIENT_KW);
        const char* user_zone = getValByKey(&_inp.condInput, RODS_ZONE_CLIENT_KW);
        const char* access_perm = getValByKey(&_inp.condInput, ACCESS_PERMISSION_KW);

        if (user_name == nullptr || user_zone == nullptr || access_perm == nullptr) {
            if (getValByKey(&_inp.condInput, DISABLE_STRICT_ACL_KW) != nullptr) {
                return 0;
            }
            return CAT_INVALID_ARGUMENT;
        }

        int data_ix = -1;
        for (int i = 0; i < _inp.selectInp.len; ++i) {
            if (_inp.selectInp.inx[i] == COL_D_DATA_ID) {
                data_ix = i;
                break;
            }
        }

        if (data_ix < 0) {
            return CAT_INVALID_ARGUMENT;
        }

        std::string resolved_zone = user_zone;
        if (resolved_zone.empty()) {
            chlGetLocalZone(resolved_zone);
        }

        rsComm_t check_comm = _comm;
        std::snprintf(check_comm.clientUser.userName, sizeof(check_comm.clientUser.userName), "%s", user_name);
        std::snprintf(check_comm.clientUser.rodsZone, sizeof(check_comm.clientUser.rodsZone), "%s", resolved_zone.c_str());

        std::string prev_data_id;
        for (const auto& row : _rows) {
            if (!row.is_array() || static_cast<std::size_t>(data_ix) >= row.size() || !row[data_ix].is_string()) {
                return CAT_INVALID_ARGUMENT;
            }
            const auto& data_id_str = row[data_ix].get_ref<const std::string&>();
            if (!prev_data_id.empty() && data_id_str == prev_data_id) {
                continue;
            }
            prev_data_id = data_id_str;

            rodsLong_t data_id = 0;
            const auto [ptr, ec] = std::from_chars(
                data_id_str.data(), data_id_str.data() + data_id_str.size(), data_id);
            if (ec != std::errc{}) {
                return CAT_INVALID_ARGUMENT;
            }

            if (const int status = chl_check_data_object_access(check_comm, data_id, access_perm); status != 0) {
                return status;
            }
        }

        return 0;
    }
} // anonymous namespace

int
_rsGenQuery( rsComm_t *rsComm, genQueryInp_t *genQueryInp,
             genQueryOut_t **genQueryOut ) {
    int status = 0;
    int resc_grp_attr_pos = -1;
    int resc_hier_attr_pos = -1;

    static int ruleExecuted = 0;

    static int PrePostProcForGenQueryFlag = -2;
    int i, argc;
    ruleExecInfo_t rei2;
    const char *args[MAX_NUM_OF_ARGS_IN_ACTION];

    // =-=-=-=-=-=-=-
    // handle queries from older clients
    std::string client_rel_version = rsComm->cliVersion.relVersion;
    std::string local_rel_version = RODS_REL_VERSION;
    if ( client_rel_version != local_rel_version ) {	// skip if version strings match
        irods::error err = strip_resc_grp_name_from_query_inp( genQueryInp, resc_grp_attr_pos );
        if ( !err.ok() ) {
            irods::log( PASS( err ) );
        }
    }

    irods::error err = strip_resc_hier_name_from_query_inp( genQueryInp, resc_hier_attr_pos );
    if ( !err.ok() ) {
        irods::log( PASS( err ) );
        // All irods::error::code values returned here are constructed from error codes created in the iRODS error
        // table, which are all ints. Ignore narrowing conversion warnings because the values are not actually narrowed.
        // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
        return err.code();
    }

    if ( PrePostProcForGenQueryFlag < 0 ) {
        if ( getenv( "PREPOSTPROCFORGENQUERYFLAG" ) != NULL ) {
            PrePostProcForGenQueryFlag = 1;
        }
        else {
            PrePostProcForGenQueryFlag = 0;
        }
    }

    memset( ( char* )&rei2, 0, sizeof( ruleExecInfo_t ) );
    rei2.rsComm = rsComm;

    *genQueryOut = static_cast<genQueryOut_t*>(std::calloc(1, sizeof(genQueryOut_t)));
    if (*genQueryOut == nullptr) {
        return SYS_MALLOC_ERR;
    }

    if (ruleExecuted == 0) {
        int prop_status{};
        // set a strict acl prop
        try {
            irods::set_server_property<std::string>(irods::STRICT_ACL_KW, "on");
        }
        catch (irods::exception& e) {
            irods::log(e);
            // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
            prop_status = e.code();
        }
        if (prop_status == 0) {
            ruleExecuted = 1; /* No need to retry next time since it
                                   succeeded.  Since this is called at
                                   startup, the Rule Engine may not be
                                   initialized yet, in which case the
                                   default setting is fine and we should
                                   retry next time. */
        }
    }

    const bool agent_conn_flag = irods::server_property_exists(irods::AGENT_CONN_KW);
    char* dis_kw = getValByKey( &genQueryInp->condInput, DISABLE_STRICT_ACL_KW );
    const bool disable_strict_acl = (agent_conn_flag && dis_kw != nullptr);

    if ( PrePostProcForGenQueryFlag == 1 ) {
        std::string arg = fmt::format( "{}", ( long )genQueryInp );
        args[0] = arg.c_str();
        argc = 1;
        i =  applyRuleArg( "acPreProcForGenQuery", args, argc, &rei2, NO_SAVE_REI );
        if ( i < 0 ) {
            rodsLog( LOG_ERROR,
                     "rsGenQuery:acPreProcForGenQuery error,stat=%d", i );
            if ( i != NO_MICROSERVICE_FOUND_ERR ) {
                return i;
            }
        }
    }
    /**  June 1 2009 for pre-post processing rule hooks **/

    auto run_query = [&]() -> int {
        namespace gq2 = irods::experimental::genquery2;

        // Close an open continuation cursor when continueInx > 0 and maxRows <= 0.
        if (genQueryInp->continueInx > 0 && genQueryInp->maxRows <= 0) {
            erase_continuation(genQueryInp->continueInx);
            return 0;
        }

        // Delegate QUOTA_QUERY to getQuotaByResc.
        if ((genQueryInp->options & QUOTA_QUERY) != 0) {
            std::string quota_user;
            std::string quota_resc;
            for (int k = 0; k < genQueryInp->sqlCondInp.len; ++k) {
                if (genQueryInp->sqlCondInp.value[k] == nullptr) {
                    continue;
                }
                if (genQueryInp->sqlCondInp.inx[k] == COL_USER_NAME) {
                    quota_user = extract_quota_literal(genQueryInp->sqlCondInp.value[k]);
                }
                else if (genQueryInp->sqlCondInp.inx[k] == COL_R_RESC_NAME) {
                    quota_resc = extract_quota_literal(genQueryInp->sqlCondInp.value[k]);
                }
            }

            if (quota_user.empty()) {
                quota_user = rsComm->clientUser.userName;
            }

            std::free(*genQueryOut);
            *genQueryOut = nullptr;

            const int q_status = getQuotaByResc(
                rsComm,
                quota_user.data(),
                quota_resc.empty() ? nullptr : quota_resc.data(),
                genQueryOut);

            if (*genQueryOut == nullptr) {
                *genQueryOut = static_cast<genQueryOut_t*>(std::calloc(1, sizeof(genQueryOut_t)));
                if (*genQueryOut == nullptr) {
                    return SYS_MALLOC_ERR;
                }
            }

            if (q_status < 0) {
                return q_status;
            }
            if ((*genQueryOut)->rowCnt == 0) {
                return CAT_NO_ROWS_FOUND;
            }
            return 0;
        }

        std::size_t current_offset = static_cast<std::size_t>(std::max(0, genQueryInp->rowOffset));
        int total_row_count = 0;

        if (genQueryInp->continueInx > 0) {
            const auto state = get_continuation(genQueryInp->continueInx);
            if (!state) {
                return CAT_INVALID_ARGUMENT;
            }
            current_offset = state->next_offset;
            total_row_count = state->total_row_count;
        }

        gq2::select select_ast;
        try {
            select_ast = gq2::genquery1_to_genquery2_ast(*genQueryInp);
        }
        catch (const irods::exception& e) {
            log_api::error("_rsGenQuery: AST translation failed: {}", e.client_display_what());
            // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
            return e.code();
        }
        catch (const std::exception& e) {
            log_api::error("_rsGenQuery: AST translation failed: {}", e.what());
            return CAT_INVALID_ARGUMENT;
        }

        std::string db_tech;
        {
            const auto handle = irods::server_properties::instance().map();
            const auto& config = handle.get_json();
            const auto& db = config.at(nlohmann::json::json_pointer{"/plugin_configuration/database/technology"});
            db_tech = db.get_ref<const std::string&>();
        }

        gq2::options sql_opts{};
        sql_opts.database = db_tech;
        sql_opts.user_name = rsComm->clientUser.userName;
        sql_opts.user_zone = rsComm->clientUser.rodsZone;
        sql_opts.admin_mode = irods::is_privileged_client(*rsComm) || disable_strict_acl;
        sql_opts.default_number_of_rows = MAX_SQL_ROWS;

        // Compute total row count when requested on the initial page.
        if (genQueryInp->continueInx <= 0 && (genQueryInp->options & RETURN_TOTAL_ROW_COUNT) != 0) {
            auto count_ast = select_ast;
            count_ast.range = {};
            try {
                auto [count_sql, count_values] = gq2::to_sql(count_ast, sql_opts);
                char* count_out_str = nullptr;
                irods::at_scope_exit free_count_out{[&count_out_str] { std::free(count_out_str); }};
                const int count_ec = chl_execute_genquery2_sql(*rsComm, count_sql.c_str(), &count_values, &count_out_str);
                if (count_ec == 0 && count_out_str != nullptr) {
                    const auto count_rows = nlohmann::json::parse(count_out_str);
                    if (count_rows.is_array()) {
                        total_row_count = static_cast<int>(count_rows.size());
                    }
                }
            }
            catch (const irods::exception& e) {
                log_api::error("_rsGenQuery: count SQL generation failed: {}", e.client_display_what());
                // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
                return e.code();
            }
            catch (const std::exception& e) {
                log_api::error("_rsGenQuery: count SQL execution failed: {}", e.what());
                return SYS_INTERNAL_ERR;
            }
        }

        // If maxRows <= 0 on initial call, return 0 rows with totalRowCount populated.
        if (genQueryInp->maxRows <= 0) {
            (*genQueryOut)->totalRowCount = total_row_count;
            return total_row_count == 0 ? CAT_NO_ROWS_FOUND : 0;
        }

        const auto requested_rows = static_cast<std::size_t>(genQueryInp->maxRows);
        select_ast.range.offset = std::to_string(current_offset);
        select_ast.range.number_of_rows = std::to_string(requested_rows + 1);

        std::string sql;
        std::vector<std::string> bind_values;
        try {
            std::tie(sql, bind_values) = gq2::to_sql(select_ast, sql_opts);
        }
        catch (const irods::exception& e) {
            log_api::error("_rsGenQuery: SQL generation failed: {}", e.client_display_what());
            // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
            return e.code();
        }
        catch (const std::exception& e) {
            log_api::error("_rsGenQuery: SQL generation failed: {}", e.what());
            return CAT_INVALID_ARGUMENT;
        }

        char* sql_out_str = nullptr;
        irods::at_scope_exit free_sql_out{[&sql_out_str] { std::free(sql_out_str); }};
        if (const int ec = chl_execute_genquery2_sql(*rsComm, sql.c_str(), &bind_values, &sql_out_str); ec < 0) {
            return ec;
        }

        if (sql_out_str == nullptr) {
            return SYS_INTERNAL_ERR;
        }

        nlohmann::json rows;
        try {
            rows = nlohmann::json::parse(sql_out_str);
        }
        catch (const std::exception& e) {
            log_api::error("_rsGenQuery: failed to parse JSON result: {}", e.what());
            return SYS_INTERNAL_ERR;
        }

        if (!rows.is_array() || rows.empty()) {
            if (genQueryInp->continueInx > 0) {
                erase_continuation(genQueryInp->continueInx);
            }
            (*genQueryOut)->totalRowCount = total_row_count;
            return CAT_NO_ROWS_FOUND;
        }

        const bool has_more = rows.size() > requested_rows;
        if (has_more) {
            rows.get_ref<nlohmann::json::array_t&>().resize(requested_rows);
        }

        if (const int access_ec = check_cond_input_access(*rsComm, *genQueryInp, rows); access_ec != 0) {
            if (genQueryInp->continueInx > 0) {
                erase_continuation(genQueryInp->continueInx);
            }
            return access_ec;
        }

        int out_continue_inx = 0;
        const std::size_t next_offset = current_offset + rows.size();
        if (has_more) {
            if ((genQueryInp->options & AUTO_CLOSE) != 0) {
                if (genQueryInp->continueInx > 0) {
                    erase_continuation(genQueryInp->continueInx);
                }
                out_continue_inx = -1;
            }
            else if (genQueryInp->continueInx > 0) {
                update_continuation(genQueryInp->continueInx, next_offset, total_row_count);
                out_continue_inx = genQueryInp->continueInx;
            }
            else {
                out_continue_inx = allocate_continue_inx(next_offset, total_row_count);
            }
        }
        else if (genQueryInp->continueInx > 0) {
            erase_continuation(genQueryInp->continueInx);
        }

        std::vector<int> col_indices;
        col_indices.reserve(static_cast<std::size_t>(std::max(0, genQueryInp->selectInp.len)));
        for (int k = 0; k < genQueryInp->selectInp.len; ++k) {
            col_indices.push_back(genQueryInp->selectInp.inx[k]);
        }

        genQueryOut_t* populated_out = nullptr;
        if (const int pack_ec = gq2::rows_to_genquery_out(rows, col_indices, &populated_out, out_continue_inx); pack_ec < 0) {
            return pack_ec;
        }

        std::free(*genQueryOut);
        *genQueryOut = populated_out;
        if ((genQueryInp->options & RETURN_TOTAL_ROW_COUNT) != 0) {
            (*genQueryOut)->totalRowCount = total_row_count;
        }

        return 0;
    };

    status = run_query();

    /**  June 1 2009 for pre-post processing rule hooks **/
    if ( PrePostProcForGenQueryFlag == 1 ) {
        std::string in_string = fmt::format( "{}", ( long )genQueryInp );
        std::string out_string = fmt::format( "{}", ( long )genQueryOut );
        std::string status_string = fmt::format( "{}", ( long )status );
        args[0] = in_string.c_str();
        args[1] = out_string.c_str();
        args[2] = status_string.c_str();
        argc = 3;
        i =  applyRuleArg( "acPostProcForGenQuery", args, argc, &rei2, NO_SAVE_REI );
        if ( i < 0 ) {
            rodsLog( LOG_ERROR,
                     "rsGenQuery:acPostProcForGenQuery error,stat=%d", i );
            if ( i != NO_MICROSERVICE_FOUND_ERR ) {
                return i;
            }
        }
    }
    /**  June 1 2009 for pre-post processing rule hooks **/

    // =-=-=-=-=-=-=-
    // handle queries from older clients
    if ( status >= 0 && resc_grp_attr_pos >= 0 ) {
        irods::error err = add_resc_grp_name_to_query_out( *genQueryOut, resc_grp_attr_pos );
        if ( !err.ok() ) {
            irods::log( PASS( err ) );
        }
    }

    if ( status >= 0 && resc_hier_attr_pos >= 0 ) {
        // restore COL_D_RESC_HIER in select index
        genQueryInp->selectInp.inx[resc_hier_attr_pos] = COL_D_RESC_HIER;

        // replace resc ids with resc hier strings in output
        irods::error err = add_resc_hier_name_to_query_out( *genQueryOut, resc_hier_attr_pos );
        if ( !err.ok() ) {
            irods::log( PASS( err ) );
        }
    }

    if ( status < 0 ) {
        if ( status != CAT_NO_ROWS_FOUND ) {
            rodsLog( LOG_NOTICE,
                     "_rsGenQuery: genQuery status = %d", status );
        }
        return status;
    }
    return status;
}
