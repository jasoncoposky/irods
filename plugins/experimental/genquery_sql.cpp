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
    std::vector<std::string> where_clauses{};
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

//log::api::info("pushing back table {} and column {}", tbl, col);
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
            auto cond = sql(condition);

            where_clauses.push_back(cond);

            ret += cond;

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
            //log::api::info("priming table alias {} -> '{}'", t, a);
            from_aliases.push_back(a);
        }

    } // prime_from_aliases


    auto from_table_is_aliased(const std::string& _t) -> bool
    {
        return _t.find(" ") != std::string::npos;

    } // from_table_is_aliased


    auto annotate_where_clause(std::string& _clause, const uint32_t _counter) -> void
    {
        const auto p0 = _clause.find('.');
        if(std::string::npos == p0) {
            return;
        }

        _clause.insert(p0, std::string{"_"}+std::to_string(_counter));

        // the . moved forward after the insert
        const auto p1 = _clause.find('.');
        const auto p2 = _clause.find('.', p1+1);

        if(std::string::npos != p2) {
            _clause.insert(p2, std::string{"_"}+std::to_string(_counter));
        }
    } // annotate_where_clause


    auto annotate_redundant_table_aliases() -> void
    {
        std::map<std::string, uint32_t> alias_counter;

        for(auto& t : from_aliases) {
            if(from_table_is_aliased(t)) {
                auto& ctr = alias_counter[t]; 
                if(ctr > 0) {
                    t += "_" + std::to_string(ctr);
                    //log::api::info("---- annotated from table {}", t);
                }
                ++ctr;
            }
        }

        alias_counter.clear();

        for(auto& c : where_clauses) {
            std::string key{};

            const auto p = c.find(' ');
            if(std::string::npos == p) {
                key = c;
            }
            else {
                key = c.substr(0, p); 
            }

            auto& ctr = alias_counter[key]; 
            if(ctr > 0) {
                annotate_where_clause(c, ctr);
                //log::api::info("---- annotated where clause {}", c);
            }

            ++ctr;
        }

    } // annotate_redundant_table_aliases


    auto count_aliases_in_from_table(const std::string& _t) -> uint8_t
    {
        //log::api::info("searching for table {} alias in FROM tables", _t);

        uint8_t ctr{};

        for(const auto& a : from_aliases) {
            if(a == _t) {
                ++ctr;
            }
        } // for aliases

        //log::api::info("---- found {} aliases", ctr);

        return ctr;

    } // count_aliases_in_from_table


    auto count_aliases_in_where_clauses(const std::string& _t) -> uint8_t
    {
        //log::api::info("searching for table {} alias in WHERE clauses", _t);

        uint8_t ctr{};

        for(const auto& a : where_clauses) {
            if(a.find(_t) != std::string::npos) {
                ++ctr;
            }
        } // for aliases

        //log::api::info("---- found {} aliases", ctr);

        return ctr;

    } // count_aliases_in_where_clauses


    auto process_table_linkage(const std::string& _t, const link_type& _l) -> void
    {
        const auto& t2 = std::get<0>(_l);
        const auto& lk = std::get<1>(_l);

        //log::api::info("processing table linkage for {} to {}", _t, t2);

        // --> We are here for a reason, linkage is needed.
        //
        // if fc_t1 and wc_w1 are non-zero and equal then it is satisfied
        // if fc_t1 and wc_t1 are non-zero and unequal then we are missing linkage
        // if fc_t1 and wc_t1 are both zero we are missing linkage -> need a 1:1 mapping
        // we can fix up both here
        // consider case where t2 = w2 but t1 -> t2 clause does not exist
        // e.g. R_DATA_MAIN -> r_data_metamap :: do we need to match the full link clause? (just the one)
        //
        // t0, w0 should explore the link clause
        // if t0 & w0 are 0 then we need a 1:1 mapping to t2, w2
        auto fc_t1 = count_aliases_in_from_table(get_table_alias(_t));
        auto wc_t1 = count_aliases_in_from_table(get_table_alias(_t));
        auto fc_t2 = count_aliases_in_from_table(get_table_alias(t2));
        auto wc_t2 = count_aliases_in_where_clauses(t2);

        //log::api::info("counts from t1 {} where t1 {}, from t2 {} where t2 {}", fc_t1, wc_t1, fc_t2, wc_t2);

        // always need at least one where clause
        //log::api::info("adding WHERE clause for table {} : {}", _t, t2);
        where_clauses.push_back(lk);

        const auto t2_satisfied   = fc_t2 == wc_t2;
        const auto one_to_one_map = !fc_t1 && !wc_t1;
        const auto cnt = std::max(fc_t2, wc_t2);

        //log::api::info("XXXX - t2_satisfied {}", t2_satisfied);

        // fix-up the from-where disparity for table 2
        if(!t2_satisfied) {
            //log::api::info("Table 2 {} from-where is not satisfied", t2);

            if(fc_t2 < wc_t2) {
                const auto cnt = wc_t2 - fc_t2;
                for(auto i = 0; i < cnt; ++i) {
                    const auto& a = get_table_alias(t2);
                    //log::api::info("fix-up :: adding from alias {} for table {}", a, t2);
                    from_aliases.push_back(a);
                }
            }
            else {
                const auto cnt = fc_t2 - wc_t2;
                for(auto i = 0; i < cnt; ++i) {
                    //log::api::info("fix-up :: adding where clause for table {}", t2);
                    where_clauses.push_back(lk);
                }
            }
        }

        if(one_to_one_map && t2_satisfied) {
            // add additional where clauses to match the from clauses
            for(auto i = 0; i < cnt-1; ++i) {
                //log::api::info("adding WHERE clause for table {} : {}", _t, lk);
                where_clauses.push_back(lk);
            }

            for(auto i = 0; i < cnt; ++i) {
                const auto& a = get_table_alias(_t);
                //log::api::info("adding from alias for table {} : {} to list", _t, a);
                from_aliases.push_back(a);
            }
        }

    } // process_table_linkage


    auto table_has_been_processed(const std::string& _t) -> bool
    {
        return std::find(processed_tables.begin(),
                         processed_tables.end(),
                         _t) != processed_tables.end();
        
    } // table_has_been_processed


    auto linkage_is_applicable_for_table(const std::string& _t) -> bool
    {
        //log::api::info("-------- linkage is applicable for table {}", _t);

        return count_aliases_in_from_table(get_table_alias(_t)) > 0;
    } // linkage_is_applicable_for_table


    auto compute_table_linkage(const std::string& _t) -> bool;
    auto process_fklinks(const std::string& _t, const link_vector_type& _flk) -> bool
    {
        for(const auto& l : _flk) {
            const auto& t2 = std::get<0>(l);
            //const auto& lk = std::get<1>(l);

            //log::api::info("---- computing forward link for table {} to {}:{}", _t, t2, lk);

            if(compute_table_linkage(t2)) {
                //log::api::info("--- forward linkage has been computed for {}", t2);
                process_table_linkage(_t, l);
                return true;
            }
            // second link table may be terminal, determine if it should be processed
            // due to existance in the FROM clause
            else if(linkage_is_applicable_for_table(t2)) {
                //log::api::info("--- forward linkage is applicable for {} from {}", t2, _t);
                process_table_linkage(_t, l);
                return true;
            }

        } // for l

        return false;

    } // process_fklinks


    auto compute_table_linkage(const std::string& _t) -> bool
    {
        //log::api::info("computing table linkage for table {}", _t);

        if(get_table_cycle_flag(_t) > 0) {
            //log::api::info("---- found cycle flag for table {}, breaking", _t);
            return false;
        }

        if(table_has_been_processed(_t)) {
            //log::api::info("---- table has been processed {}", _t);
            return false;
        }

        processed_tables.push_back(_t);

        //log::api::info("---- computing forward linkage for table {}", _t);

        const auto t1l = find_fklinks_for_table1(_t);
        if(auto r = process_fklinks(_t, t1l); r) {
            return true;
        }

        //log::api::info("---- computing reverse linkage for table {}", _t);

        const auto t2l = find_fklinks_for_table2(_t);
        if(auto r = process_fklinks(_t, t2l); r) {
            return true;
        }

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
        for(auto&& a : where_clauses) {
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
        // TODO :: only want to uniquify tables without an alias
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


//        uniquify_tables();

        compute_table_linkage(get_table_alias(tables[0]));

        annotate_redundant_table_aliases();

        root += sel + build_from_clause() + " WHERE ";
        //root += con + " AND " + build_where_clause();
        root += build_where_clause();

        return root;
    }

#if 0
=======================================================================================
ORIGINAL GENQUERY

select distinct R_DATA_MAIN.data_name
    from  R_DATA_MAIN,
          R_OBJT_METAMAP r_data_metamap,
          R_META_MAIN    r_data_meta_main,
          R_OBJT_METAMAP r_data_metamap2,
          R_META_MAIN    r_data_meta_mn02

    where r_data_meta_main.meta_attr_name = ? AND
          r_data_meta_mn02.meta_attr_name = ? AND
          R_DATA_MAIN.data_id     = r_data_metamap.object_id AND
          r_data_metamap.meta_id  = r_data_meta_main.meta_id AND
          r_data_metamap2.meta_id = r_data_meta_mn02.meta_id AND
          R_DATA_MAIN.data_id     = r_data_metamap2.object_id

    order by R_DATA_MAIN.data_name",

=======================================================================================
#endif
} // namespace irods::experimental::api::genquery
