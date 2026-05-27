#ifndef IRODS_GENQUERY2_JSON_DESERIALIZATION_HPP
#define IRODS_GENQUERY2_JSON_DESERIALIZATION_HPP

#include "irods/private/genquery2_ast_types.hpp"
#include <nlohmann/json.hpp>
#include <boost/variant/recursive_variant.hpp>

namespace irods::experimental::genquery2 {

    // Forward declarations
    void from_json(const nlohmann::json& j, column& c);
    void from_json(const nlohmann::json& j, function& f);
    void from_json(const nlohmann::json& j, condition& c);
    void from_json(const nlohmann::json& j, select& s);

    inline void from_json(const nlohmann::json& j, column& c) {
        c.name = j.at("name").get<std::string>();
        c.type_name = j.at("type_name").get<std::string>();
    }

    inline void from_json(const nlohmann::json& j, function& f) {
        f.name = j.at("name").get<std::string>();
        f.distinct = j.at("distinct").get<bool>();
        for (const auto& arg : j.at("arguments")) {
            if (arg.is_string()) f.arguments.push_back(arg.get<std::string>());
            else if (arg.contains("type_name")) { column c; from_json(arg, c); f.arguments.push_back(c); }
            else { function sub_f; from_json(arg, sub_f); f.arguments.push_back(sub_f); }
        }
    }

    // Helper for condition_expression
    inline condition_expression deserialize_condition_expression(const nlohmann::json& j) {
        const std::string type = j.at("type").get<std::string>();
        if (type == "like") return condition_like{j.at("value").get<std::string>()};
        if (type == "in") return condition_in{j.at("values").get<std::vector<std::string>>()};
        if (type == "between") return condition_between{j.at("low").get<std::string>(), j.at("high").get<std::string>()};
        if (type == "=") return condition_equal{j.at("value").get<std::string>()};
        if (type == "!=") return condition_not_equal{j.at("value").get<std::string>()};
        if (type == "<") return condition_less_than{j.at("value").get<std::string>()};
        if (type == "<=") return condition_less_than_or_equal_to{j.at("value").get<std::string>()};
        if (type == ">") return condition_greater_than{j.at("value").get<std::string>()};
        if (type == ">=") return condition_greater_than_or_equal_to{j.at("value").get<std::string>()};
        if (type == "is_null") return condition_is_null{};
        if (type == "is_not_null") return condition_is_not_null{};
        if (type == "not") {
            if (j.contains("expression")) return condition_operator_not{deserialize_condition_expression(j.at("expression"))};
        }
        throw std::runtime_error("Unknown condition expression type: " + type);
    }

    // Helper for condition_wrapper
    inline condition_wrapper deserialize_condition_wrapper(const nlohmann::json& j) {
        if (j.contains("lhs")) {
            condition c;
            from_json(j, c);
            return c;
        }
        const std::string type = j.at("type").get<std::string>();
        if (type == "and") {
            logical_and l;
            for (const auto& sub : j.at("conditions")) l.condition.push_back(deserialize_condition_wrapper(sub));
            return l;
        }
        if (type == "or") {
            logical_or l;
            for (const auto& sub : j.at("conditions")) l.condition.push_back(deserialize_condition_wrapper(sub));
            return l;
        }
        if (type == "not") {
            logical_not l;
            for (const auto& sub : j.at("conditions")) l.condition.push_back(deserialize_condition_wrapper(sub));
            return l;
        }
        if (type == "group") {
            logical_grouping l;
            for (const auto& sub : j.at("conditions")) l.conditions.push_back(deserialize_condition_wrapper(sub));
            return l;
        }
        throw std::runtime_error("Unknown condition wrapper type: " + type);
    }

    inline void from_json(const nlohmann::json& j, condition& c) {
        if (j.at("lhs").at("node_type").get<std::string>() == "column") {
            column col; from_json(j.at("lhs"), col); c.lhs = col;
        } else {
            function f; from_json(j.at("lhs"), f); c.lhs = f;
        }
        c.expression = deserialize_condition_expression(j.at("expression"));
    }

    inline void from_json(const nlohmann::json& j, select& s) {
        s.distinct = j.at("distinct").get<bool>();
        for (const auto& p : j.at("projections")) {
            if (p.contains("type_name")) { column c; from_json(p, c); s.projections.push_back(c); }
            else { function f; from_json(p, f); s.projections.push_back(f); }
        }
        for (const auto& c : j.at("conditions")) s.conditions.push_back(deserialize_condition_wrapper(c));
        
        for (const auto& g : j.at("group_by")) {
            if (g.at("node_type").get<std::string>() == "column") { column c; from_json(g, c); s.group_by.expressions.push_back(c); }
            else { function f; from_json(g, f); s.group_by.expressions.push_back(f); }
        }

        for (const auto& o : j.at("order_by")) {
            sort_expression se;
            se.ascending_order = o.at("ascending").get<bool>();
            if (o.at("expression").at("node_type").get<std::string>() == "column") { column c; from_json(o.at("expression"), c); se.expr = c; }
            else { function f; from_json(o.at("expression"), f); se.expr = f; }
            s.order_by.sort_expressions.push_back(se);
        }

        s.range.offset = j.at("range").at("offset").get<std::string>();
        s.range.number_of_rows = j.at("range").at("limit").get<std::string>();
    }

} // namespace irods::experimental::genquery2

#endif // IRODS_GENQUERY2_JSON_DESERIALIZATION_HPP
