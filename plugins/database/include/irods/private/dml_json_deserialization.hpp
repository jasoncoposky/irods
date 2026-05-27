#ifndef IRODS_DML_JSON_DESERIALIZATION_HPP
#define IRODS_DML_JSON_DESERIALIZATION_HPP

#include "irods/atomic_apply_database_operations.hpp"
#include <nlohmann/json.hpp>

namespace irods::experimental::dml {

    inline void from_json(const nlohmann::json& j, column& c) {
        // column is immutable, so we use a trick or just use a helper
        const_cast<std::string&>(c.name) = j.at("name").get<std::string>();
        const_cast<std::string&>(c.value) = j.at("value").get<std::string>();
    }

    inline void from_json(const nlohmann::json& j, condition& c) {
        const_cast<std::string&>(c.column) = j.at("column").get<std::string>();
        const_cast<std::string&>(c.op) = j.at("op").get<std::string>();
        const_cast<std::vector<std::string>&>(c.values) = j.at("values").get<std::vector<std::string>>();
    }

    inline void from_json(const nlohmann::json& j, insert_op& op) {
        const_cast<std::string&>(op.table) = j.at("table").get<std::string>();
        const_cast<std::vector<column>&>(op.data) = j.at("data").get<std::vector<column>>();
    }

    inline void from_json(const nlohmann::json& j, update_op& op) {
        const_cast<std::string&>(op.table) = j.at("table").get<std::string>();
        const_cast<std::vector<column>&>(op.data) = j.at("data").get<std::vector<column>>();
        const_cast<std::vector<condition>&>(op.conditions) = j.at("conditions").get<std::vector<condition>>();
    }

    inline void from_json(const nlohmann::json& j, delete_op& op) {
        const_cast<std::string&>(op.table) = j.at("table").get<std::string>();
        const_cast<std::vector<condition>&>(op.conditions) = j.at("conditions").get<std::vector<condition>>();
    }

} // namespace irods::experimental::dml

#endif // IRODS_DML_JSON_DESERIALIZATION_HPP
