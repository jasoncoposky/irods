#ifndef IRODS_DML_JSON_SERIALIZATION_HPP
#define IRODS_DML_JSON_SERIALIZATION_HPP

#include "irods/atomic_apply_database_operations.hpp"
#include <nlohmann/json.hpp>

namespace irods::experimental::dml {

    inline void to_json(nlohmann::json& j, const column& c) {
        j = nlohmann::json{{"name", c.name}, {"value", c.value}};
    }

    inline void to_json(nlohmann::json& j, const condition& c) {
        j = nlohmann::json{{"column", c.column}, {"op", c.op}, {"values", c.values}};
    }

    inline void to_json(nlohmann::json& j, const insert_op& op) {
        j = nlohmann::json{{"type", "insert"}, {"table", op.table}, {"data", op.data}};
    }

    inline void to_json(nlohmann::json& j, const update_op& op) {
        j = nlohmann::json{{"type", "update"}, {"table", op.table}, {"data", op.data}, {"conditions", op.conditions}};
    }

    inline void to_json(nlohmann::json& j, const delete_op& op) {
        j = nlohmann::json{{"type", "delete"}, {"table", op.table}, {"conditions", op.conditions}};
    }

    inline void to_json(nlohmann::json& j, const operation_type& op) {
        std::visit([&j](auto&& arg) {
            to_json(j, arg);
        }, op);
    }

} // namespace irods::experimental::dml

#endif // IRODS_DML_JSON_SERIALIZATION_HPP
