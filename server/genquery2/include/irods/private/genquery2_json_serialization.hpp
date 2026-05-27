#ifndef IRODS_GENQUERY2_JSON_SERIALIZATION_HPP
#define IRODS_GENQUERY2_JSON_SERIALIZATION_HPP

#include "irods/private/genquery2_ast_types.hpp"
#include <nlohmann/json.hpp>
#include <boost/variant/static_visitor.hpp>

namespace irods::experimental::genquery2 {

    // Forward declarations
    void to_json(nlohmann::json& j, const column& c);
    void to_json(nlohmann::json& j, const function& f);
    void to_json(nlohmann::json& j, const condition& c);
    void to_json(nlohmann::json& j, const select& s);

    // Visitors
    struct projection_visitor : public boost::static_visitor<nlohmann::json> {
        nlohmann::json operator()(const column& c) const { nlohmann::json j; to_json(j, c); return j; }
        nlohmann::json operator()(const function& f) const { nlohmann::json j; to_json(j, f); return j; }
    };

    struct argument_visitor {
        nlohmann::json operator()(const std::string& s) const { return s; }
        nlohmann::json operator()(const column& c) const { nlohmann::json j; to_json(j, c); return j; }
        nlohmann::json operator()(const function& f) const { nlohmann::json j; to_json(j, f); return j; }
    };

    struct condition_expression_visitor : public boost::static_visitor<nlohmann::json> {
        nlohmann::json operator()(const condition_like& c) const { return {{"type", "like"}, {"value", c.string_literal}}; }
        nlohmann::json operator()(const condition_in& c) const { return {{"type", "in"}, {"values", c.list_of_string_literals}}; }
        nlohmann::json operator()(const condition_between& c) const { return {{"type", "between"}, {"low", c.low}, {"high", c.high}}; }
        nlohmann::json operator()(const condition_equal& c) const { return {{"type", "="}, {"value", c.string_literal}}; }
        nlohmann::json operator()(const condition_not_equal& c) const { return {{"type", "!="}, {"value", c.string_literal}}; }
        nlohmann::json operator()(const condition_less_than& c) const { return {{"type", "<"}, {"value", c.string_literal}}; }
        nlohmann::json operator()(const condition_less_than_or_equal_to& c) const { return {{"type", "<="}, {"value", c.string_literal}}; }
        nlohmann::json operator()(const condition_greater_than& c) const { return {{"type", ">"}, {"value", c.string_literal}}; }
        nlohmann::json operator()(const condition_greater_than_or_equal_to& c) const { return {{"type", ">="}, {"value", c.string_literal}}; }
        nlohmann::json operator()(const condition_is_null&) const { return {{"type", "is_null"}}; }
        nlohmann::json operator()(const condition_is_not_null&) const { return {{"type", "is_not_null"}}; }
        nlohmann::json operator()(const condition_operator_not& c) const { 
            return {{"type", "not"}, {"expression", boost::apply_visitor(*this, c.expression)}}; 
        }
    };

    struct condition_wrapper_visitor : public boost::static_visitor<nlohmann::json> {
        nlohmann::json operator()(const logical_and& l) const { 
            nlohmann::json j = nlohmann::json::array();
            for (const auto& c : l.condition) j.push_back(boost::apply_visitor(*this, c));
            return {{"type", "and"}, {"conditions", j}};
        }
        nlohmann::json operator()(const logical_or& l) const { 
            nlohmann::json j = nlohmann::json::array();
            for (const auto& c : l.condition) j.push_back(boost::apply_visitor(*this, c));
            return {{"type", "or"}, {"conditions", j}};
        }
        nlohmann::json operator()(const logical_not& l) const { 
            nlohmann::json j = nlohmann::json::array();
            for (const auto& c : l.condition) j.push_back(boost::apply_visitor(*this, c));
            return {{"type", "not"}, {"conditions", j}};
        }
        nlohmann::json operator()(const logical_grouping& l) const { 
            nlohmann::json j = nlohmann::json::array();
            for (const auto& c : l.conditions) j.push_back(boost::apply_visitor(*this, c));
            return {{"type", "group"}, {"conditions", j}};
        }
        nlohmann::json operator()(const condition& c) const { nlohmann::json j; to_json(j, c); return j; }
    };

    // Implementations
    inline void to_json(nlohmann::json& j, const column& c) {
        j = {{"name", c.name}, {"type_name", c.type_name}};
    }

    inline void to_json(nlohmann::json& j, const function& f) {
        nlohmann::json args = nlohmann::json::array();
        for (const auto& arg : f.arguments) args.push_back(std::visit(argument_visitor{}, arg));
        j = {{"name", f.name}, {"arguments", args}, {"distinct", f.distinct}};
    }

    inline void to_json(nlohmann::json& j, const condition& c) {
        nlohmann::json lhs;
        if (std::holds_alternative<column>(c.lhs)) {
            to_json(lhs, std::get<column>(c.lhs));
            lhs["node_type"] = "column";
        } else {
            to_json(lhs, std::get<function>(c.lhs));
            lhs["node_type"] = "function";
        }
        j = {{"lhs", lhs}, {"expression", boost::apply_visitor(condition_expression_visitor{}, c.expression)}};
    }

    inline void to_json(nlohmann::json& j, const select& s) {
        nlohmann::json projections = nlohmann::json::array();
        for (const auto& p : s.projections) projections.push_back(boost::apply_visitor(projection_visitor{}, p));
        
        nlohmann::json conditions = nlohmann::json::array();
        for (const auto& c : s.conditions) conditions.push_back(boost::apply_visitor(condition_wrapper_visitor{}, c));

        nlohmann::json group_by = nlohmann::json::array();
        for (const auto& e : s.group_by.expressions) {
            std::visit(overloaded{
                [&group_by](const column& c) { nlohmann::json j; to_json(j, c); j["node_type"] = "column"; group_by.push_back(j); },
                [&group_by](const function& f) { nlohmann::json j; to_json(j, f); j["node_type"] = "function"; group_by.push_back(j); }
            }, e);
        }

        nlohmann::json order_by = nlohmann::json::array();
        for (const auto& se : s.order_by.sort_expressions) {
            nlohmann::json j_se;
            std::visit(overloaded{
                [&j_se](const column& c) { to_json(j_se, c); j_se["node_type"] = "column"; },
                [&j_se](const function& f) { to_json(j_se, f); j_se["node_type"] = "function"; }
            }, se.expr);
            order_by.push_back({{"expression", j_se}, {"ascending", se.ascending_order}});
        }

        j = {
            {"distinct", s.distinct},
            {"projections", projections},
            {"conditions", conditions},
            {"group_by", group_by},
            {"order_by", order_by},
            {"range", {{"offset", s.range.offset}, {"limit", s.range.number_of_rows}}}
        };
    }

} // namespace irods::experimental::genquery2

#endif // IRODS_GENQUERY2_JSON_SERIALIZATION_HPP
