#include <iostream>

#include "genquery_ast_types.hpp"
#include "table_column_key_maps.hpp"

#include "irods_logger.hpp"
#include "irods_exception.hpp"

namespace irods::experimental::api::genquery {

    using log = irods::experimental::log;

    template <typename Iterable>
    std::string
    to_string(Iterable const& iterable) {
        std::string v{"( "};
        for (auto&& element: iterable) {
            v += "'";
            v += element;
            v += "', ";
        }
        v.erase(v.find_last_of(","));
        v += " ) ";
        return v;
    }

    struct sql_visitor: public boost::static_visitor<std::string> {
        template <typename T>
        std::string operator()(const T& arg) const {
            return sql(arg);
        }
    };

    std::vector<std::string> columns{};
    std::vector<std::string> tables{};
    std::vector<std::string> from_aliases{};
    std::vector<std::string> where_aliases{};
    std::vector<std::string> processed_tables{};

    std::string
    sql(const Column& column) {
        const auto& key = column.name;

        if(column_table_alias_map.find(key) == column_table_alias_map.end()) {
            THROW(SYS_INVALID_INPUT_PARAM,
                  fmt::format("failed to find column named [{}]", key));
        }

        const auto& tup = column_table_alias_map.at(key);
        const auto& tbl = std::get<0>(tup);
        const auto& col = std::get<1>(tup);

log::api::info("pushing back table {} and column {}", tbl, col);
        tables.push_back(tbl);
        columns.push_back(col);


        return tbl + "." + col;
    }

    static uint8_t emit_second_paren{};

    std::string
    sql(const SelectFunction& select_function) {
        std::string ret{};
        ret += select_function.name;
        ret += "(";
        emit_second_paren = 1;

        return ret;
    }

    std::string
    sql(const Selections& selections) {
        tables.clear();

        if(selections.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM, "selections are empty");
        }

        std::string ret{};
        for (auto&& selection: selections) {
            auto sel = boost::apply_visitor(sql_visitor{}, selection);
            ret.append(sel);

            if(emit_second_paren >= 2) {
                ret += "), ";
                emit_second_paren = 0;
            }
            else if(emit_second_paren > 0) {
                ++emit_second_paren;
            }
            else {
                ret += ", ";
            }
        } // for selection


        if(ret.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM, "selection string is empty");
        }

        if(std::string::npos != ret.find_last_of(",")) {
            ret.erase(ret.find_last_of(","));
        }

        return ret;
    }

    std::string
    sql(const ConditionOperator_Or& op_or) {
        std::string ret{};
        ret += boost::apply_visitor(sql_visitor(), op_or.left);
        ret += " || ";
        ret += boost::apply_visitor(sql_visitor(), op_or.right);
        return ret;
    }

    std::string
    sql(const ConditionOperator_And& op_and) {
        std::string ret = boost::apply_visitor(sql_visitor(), op_and.left);
        ret += " && ";
        ret += boost::apply_visitor(sql_visitor(), op_and.right);
        return ret;
    }

    std::string
    sql(const ConditionOperator_Not& op_not) {
        std::string ret{" NOT "};
        ret += boost::apply_visitor(sql_visitor(), op_not.expression);
        return ret;
    }

    std::string
    sql(const ConditionNotEqual& not_equal) {
        std::string ret{" != "};
        ret += "'";
        ret += not_equal.string_literal;
        ret += "'";
        return ret;
    }

    std::string
    sql(const ConditionEqual& equal) {
        std::string ret{" = "};
        ret += "'";
        ret += equal.string_literal;
        ret += "'";
        return ret;
    }

    std::string
    sql(const ConditionLessThan& less_than) {
        std::string ret{" < "};
        ret += "'";
        ret += less_than.string_literal;
        ret += "'";
        return ret;
    }

    std::string
    sql(const ConditionLessThanOrEqualTo& less_than_or_equal_to) {
        std::string ret{" <= "};
        ret += "'";
        ret += less_than_or_equal_to.string_literal;
        ret += "'";
        return ret;
    }

    std::string
    sql(const ConditionGreaterThan& greater_than) {
        std::string ret{" > "};
        ret += "'";
        ret += greater_than.string_literal;
        ret += "'";
        return ret;
    }

    std::string
    sql(const ConditionGreaterThanOrEqualTo& greater_than_or_equal_to) {
        std::string ret{" >= "};
        ret += greater_than_or_equal_to.string_literal;
        return ret;
    }

    std::string
    sql(const ConditionBetween& between) {
        std::string ret{" BETWEEN '"};
        ret += between.low;
        ret += "' AND '";
        ret += between.high;
        ret += "'";
        return ret;
    }

    std::string
    sql(const ConditionIn& in) {
        std::string ret{" IN "};
        ret += to_string(in.list_of_string_literals);
        return ret;
    }

    std::string
    sql(const ConditionLike& like) {
        std::string ret{" LIKE '"};
        ret += like.string_literal;
        ret += "'";
        return ret;
    }

    std::string
    sql(const ConditionParentOf& parent_of) {
        std::string ret{"parent_of"};
        ret += parent_of.string_literal;
        return ret;
    }

    std::string
    sql(const ConditionBeginningOf& beginning_of) {
        std::string ret{"beginning_of"};
        ret += beginning_of.string_literal;
        return ret;
    }

    std::string
    sql(const Condition& condition) {
        std::string ret{sql(condition.column)};
        ret += boost::apply_visitor(sql_visitor(), condition.expression);
        return ret;
    }

    std::string
    sql(const Conditions& conditions) {
        std::string ret{};

        size_t i{};
        for (auto&& condition: conditions) {
            ret += sql(condition);
            if(i < conditions.size()-1) { ret += " AND "; }
            ++i;
        }

        return ret;
    }

    // =-=-=-=-=-=-=-=-=-=-
    using link_type = std::tuple<const std::string, const std::string>;
    using link_vector_type = std::vector<link_type>;

    auto find_fklinks_for_table1(const std::string& _src) -> link_vector_type
    {
        std::vector<std::tuple<const std::string, const std::string>> v{};

        for(const auto& t : foreign_key_link_map) {
            const auto& t1 = std::get<0>(t);
            if(t1 == _src) {
                v.push_back(std::make_tuple(std::get<1>(t), std::get<2>(t)));
            }
        }

        return v;

    } // find_fklinks_for_table1

    auto find_fklinks_for_table2(const std::string& _src) -> link_vector_type
    {
        //link_vector_type v{};
        std::vector<std::tuple<const std::string, const std::string>> v{};

        for(const auto& t : foreign_key_link_map) {
            const auto& t2 = std::get<1>(t);
            if(t2 == _src) {
                v.push_back(std::make_tuple(std::get<0>(t), std::get<2>(t)));
            }
        }

        return v;

    } // find_fklinks_for_table2

    auto get_table_alias(const std::string& _t) -> std::string
    {
        const auto& tacm = table_alias_cycler_map;
        if(tacm.find(_t) != tacm.end()) {
            return std::get<0>(tacm.at(_t));
        }

        THROW(SYS_INVALID_INPUT_PARAM,
              fmt::format("Table does not exist [{}]", _t));

    } // get_table_alias

    auto get_table_cycle_flag(const std::string& _t) -> int
    {
        const auto& tacm = table_alias_cycler_map;
        if(tacm.find(_t) != tacm.end()) {
            const auto& tup = tacm.at(_t);
            auto r = std::get<1>(tup);
            return r;
        }

        THROW(SYS_INVALID_INPUT_PARAM,
              fmt::format("Table does not exist [{}]", _t));

    } // get_table_cycle_flag

    auto prime_from_aliases() -> void
    {
        for(auto&& t : tables) {
            auto a = get_table_alias(t);
            log::api::info("priming table alias {} -> '{}'", t, a);
            from_aliases.push_back(a);
        }

    } // prime_from_aliases

    auto annotate_redundant_table_aliases() -> void
    {
        log::api::info("IMPLEMENT ME {}", __FUNCTION__);
    }

    auto from_contains_table_alias(const std::string& _t) -> bool
    {
        log::api::info("searching for table {} alias", _t);

        //const auto space{" "};

        // table aliases are either a plane table name or
        // of the form 'table alias'.  we need to attempt
        // to match the alias after the space
        for(const auto& a : from_aliases) {
#if 0
            const auto pos = a.find(space);

            if(pos != std::string::npos) {
                const auto s = a.substr(pos+1);
                log::api::info("---- matching {} to {} alias", s, _t);
                if(_t == s) {
                    log::api::info("---- found table {} alias", _t);
                    return true;
                }
            }
            else if(a == _t) {
                log::api::info("---- found table {}", _t);
                return true;
            }
#else
            if(a == _t) {
            log::api::info("---- matched alias {} to _t {}", a, _t);
                log::api::info("---- found table {}", _t);
                return true;
            }
#endif
        } // for aliases

        return false;

    } // from_contains_table_alias

    auto process_table_linkage(const std::string& _t, const link_type& _l) -> void
    {
        log::api::info("processing where for table {} : {}", _t, std::get<1>(_l));
        where_aliases.push_back(std::get<1>(_l));

        //if(from_contains_table_alias(get_table_alias(std::get<0>(_l)))) {
        if(from_contains_table_alias(_t)) {
            log::api::info("processing from alias for table {} : {}", _t, std::get<0>(_l));
            from_aliases.push_back(get_table_alias(std::get<0>(_l)));
        }

    } // process_table_linkage

    auto table_has_been_processed(const std::string& _t) -> bool
    {
        return std::find(processed_tables.begin(),
                         processed_tables.end(),
                         _t) != processed_tables.end();
        
    } // table_has_been_processed

    auto compute_table_linkage(const std::string& _t1, const std::string& _t2) -> bool
    {
        log::api::info("computing table linkage for table {} -> {}", _t1, _t2);

        // if there is a potential cycle in table linkage
        // then break the recursion
        if(get_table_cycle_flag(_t1) > 0) {
            log::api::info("---- found cycle flag for table {}", _t1);
            return false;
        }

        // we have already processed this table
        if(table_has_been_processed(_t1)) {
            log::api::info("---- table has been processed {}, searching from_tables for {}", _t1, _t2);
            if(_t2.empty()) {
                return from_contains_table_alias(get_table_alias(_t1));
            }

            return from_contains_table_alias(get_table_alias(_t2));
        }

        processed_tables.push_back(_t1);

        // process all forward linkages for this table
        log::api::info("---- computing forward linkage for table {}", _t1);

        const auto t1_links = find_fklinks_for_table1(_t1);
        for(const auto& l : t1_links) {
            log::api::info("---- processing forward link for table {} to {}:{}", _t1, std::get<0>(l), std::get<1>(l));
            if(compute_table_linkage(std::get<0>(l), _t1)) {
                process_table_linkage(_t1, l);
                return true;
            }
        } // for l

        // process all reverse linkages for this table
        log::api::info("---- computing reverse linkage for table {}", _t1);

        const auto t2_links = find_fklinks_for_table2(_t1);
        for(const auto& l : t2_links) {
            log::api::info("---- processing reverse link for table {} to {}:{}", _t1, std::get<0>(l), std::get<1>(l));
            if(compute_table_linkage(std::get<0>(l), _t1)) {
                process_table_linkage(_t1, l);
                return true;
            }
        } // for l

        return false;

    } // compute_table_linkage

    auto build_from_clause() -> std::string
    {
        auto from = std::string{" FROM "};
        for(auto&& a : from_aliases) {
            from += a +  ", ";
        }

        if(std::string::npos != from.find_last_of(",")) {
            from.erase(from.find_last_of(","));
        }

        return from;

    } // build_from_clause 
    auto build_where_clause() -> std::string
    {
        const std::string space{" "};
        const std::string conn{"AND"};

        auto where = std::string{};
        for(auto&& a : where_aliases) {
            where += a +  space + conn + space;
        }

        auto p = where.find_last_of(conn);
        if(std::string::npos != p) {
            where.erase(where.find_last_of(conn) - 3);
        }

        return where;

    } // build_where_clause

    auto uniquify_tables() -> void
    {
        std::sort(tables.begin(), tables.end());
        auto last = std::unique(tables.begin(), tables.end());
        tables.erase(last, tables.end());

    } // uniquify_tables

    std::string
    sql(const Select& select) {
        std::string root{"SELECT "};
        auto sel = sql(select.selections);
        if(sel.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM,
                  "error : no columns selected");
        }

        auto con = sql(select.conditions);
        if(con.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM,
                  "error : no conditions provided");
        }

        if(tables.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM, "from tables is empty");
        }

        prime_from_aliases();

        annotate_redundant_table_aliases();

        uniquify_tables();
    
        compute_table_linkage(get_table_alias(tables[0]), {});

        root += sel + build_from_clause() + " WHERE ";
        root += con + " AND " + build_where_clause();

        return root;
    }

//  "SELECT DATA_NAME WHERE META_DATA_ATTR_NAME = 'a0' AND META_DATA_ATTR_NAME = 'a1'"


// SELECT R_DATA_MAIN.data_name
//     FROM R_DATA_MAIN, R_META_MAIN r_data_meta_main, R_OBJT_METAMAP r_data_metamap
//
//     WHERE r_data_meta_main.meta_attr_name = 'a0' AND
//           r_data_metamap.meta_id = r_data_meta_main.meta_id AND
//           r_data_metamap.meta_id = r_data_meta_main.meta_id AND
//           R_DATA_MAIN.data_id = r_data_metamap.object_id;"
//
// select distinct R_DATA_MAIN.data_name
//     from  R_DATA_MAIN , R_OBJT_METAMAP r_data_metamap , R_META_MAIN r_data_meta_main , R_OBJT_METAMAP r_data_metamap2, R_META_MAIN r_data_meta_mn02
//
//     where r_data_meta_main.meta_attr_name = ?  AND r_data_meta_mn02.meta_attr_name = ?  AND
//
//           R_DATA_MAIN.data_id     = r_data_metamap.object_id  AND
//           r_data_metamap.meta_id  = r_data_meta_main.meta_id AND
//           r_data_metamap2.meta_id = r_data_meta_mn02.meta_id AND
//           R_DATA_MAIN.data_id     = r_data_metamap2.object_id
//
//     order by R_DATA_MAIN.data_name",


//SELECT R_DATA_MAIN.data_name from R_DATA_MAIN, r_data_meta_main WHERE
//    r_data_meta_main.meta_attr_name = 'a0' AND r_data_meta_main.meta_attr_name = 'a1' AND
//    R_DATA_MAIN.data_id = r_data_meta_main.object_id;",

// SELECT R_DATA_MAIN.data_name from R_DATA_MAIN, r_data_meta_main WHERE
//     r_data_meta_main.meta_attr_name = 'a0' AND r_data_meta_main.meta_attr_name = 'a1' AND
//     R_DATA_MAIN.data_id = r_data_meta_main.object_id;",

// SELECT R_DATA_MAIN.data_name from R_DATA_MAIN, r_data_meta_main WHERE r_data_meta_main.meta_attr_name = 'a0' AND r_data_meta_main.meta_attr_name = 'a1';",





} // namespace irods::experimental::api::genquery
