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

    std::vector<std::string> tables{};

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

        tables.push_back(tbl);

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

    auto find_fklink_for_columns(
        const std::string& src
      , const std::string& dst) -> std::tuple<std::string, std::string>
    {
        for(auto& e : foreign_key_link_map) {
            const auto& key = std::get<0>(e);
            if(key == src) {
                const auto& tup = std::get<1>(e);
                const auto& col = std::get<0>(tup);
                if(dst == col) {
                    return tup;
                }
            }
        }

        return {{}, {}};

    } // find_fklink_for_columns

    auto compute_join_constraints() -> std::string
    {
        // guarantee a unique list of tables to join
        std::sort(tables.begin(), tables.end());
        auto last = std::unique(tables.begin(), tables.end());
        tables.erase(last, tables.end());

        std::string cons{};
        for(const auto& src : tables) {
            for(const auto& dst : tables) {
                if(src == dst) {
                    continue;
                }

                const auto& tup = find_fklink_for_columns(src, dst);
                const auto& col = std::get<0>(tup); // matched dst column
                if(!col.empty()) {
                    const auto& fkl = std::get<1>(tup);
                    cons += " AND " + fkl;
                }
            } // for dst
        } // for src

        return cons;

    } // compute_join_constraints

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

        // guarantee a unique list of tables to join
        std::sort(tables.begin(), tables.end());
        auto last = std::unique(tables.begin(), tables.end());
        tables.erase(last, tables.end());

        std::string from{" from "};
        for(auto&& t : tables) {
            from += t +  ", ";
        }

        if(std::string::npos != from.find_last_of(",")) {
            from.erase(from.find_last_of(","));
        }

        root += sel + from + " WHERE ";
        root += con + compute_join_constraints();

        return root;
    }

//  "SELECT DATA_NAME WHERE META_DATA_ATTR_NAME = 'a0' AND META_DATA_ATTR_NAME = 'a1'"



// select distinct R_DATA_MAIN.data_name  from  R_DATA_MAIN , R_OBJT_METAMAP r_data_metamap , R_META_MAIN r_data_meta_main , R_OBJT_METAMAP r_data_metamap2, R_META_MAIN r_data_meta_mn02  where r_data_meta_main.meta_attr_name = ?  AND r_data_meta_mn02.meta_attr_name = ?  AND R_DATA_MAIN.data_id = r_data_metamap.object_id  AND r_data_metamap.meta_id = r_data_meta_main.meta_id  AND r_data_metamap2.meta_id = r_data_meta_mn02.meta_id AND R_DATA_MAIN.data_id = r_data_metamap2.object_id  order by R_DATA_MAIN.data_name",



// SELECT R_DATA_MAIN.data_name from R_DATA_MAIN, r_data_meta_main WHERE r_data_meta_main.meta_attr_name = 'a0' AND r_data_meta_main.meta_attr_name = 'a1';",





} // namespace irods::experimental::api::genquery
