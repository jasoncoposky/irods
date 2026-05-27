#include "irods/atomic_apply_database_operations.hpp"

#include "irods/rodsErrorTable.h"
#include "irods/irods_exception.hpp"
#include "irods/irods_logger.hpp"
#include "irods/irods_database_factory.hpp"
#include "irods/irods_database_manager.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/dml_json_serialization.hpp"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

namespace
{
    using log_db = irods::experimental::log::database;
} // anonymous namespace

namespace irods::experimental
{
    auto atomic_apply_database_operations(const std::vector<dml::operation_type>& _ops) -> int
    {
        if (_ops.empty()) {
            return 0;
        }

        try {
            const auto& server_config = irods::server_properties::instance().map().get_json();
            const auto& db_config = server_config.at(irods::KW_CFG_PLUGIN_CONFIGURATION).at(irods::KW_CFG_PLUGIN_TYPE_DATABASE);
            
            std::string db_type;
            if (db_config.contains(irods::KW_CFG_DB_TECHNOLOGY)) {
                db_type = db_config.at(irods::KW_CFG_DB_TECHNOLOGY).get<std::string>();
            } else {
                for (auto it = db_config.begin(); it != db_config.end(); ++it) {
                    if (it.value().is_object() && it.value().contains(irods::KW_CFG_DB_TECHNOLOGY)) {
                        db_type = it.value().at(irods::KW_CFG_DB_TECHNOLOGY).get<std::string>();
                        break;
                    }
                }
            }

            irods::database_ptr db;
            if (const auto ret = irods::db_mgr.resolve(db_type, db); !ret.ok()) {
                log_db::error("{}: Failed to resolve database plugin [{}]: {}", __func__, db_type, ret.result());
                return CAT_CONNECT_ERR;
            }

            // Serialize operations to JSON
            nlohmann::json j_ops = _ops;
            const std::string json_str = j_ops.dump();

            // Dispatch to plugin
            if (const auto ret = db->call_without_policy(nullptr, irods::DATABASE_OP_ATOMIC_APPLY, nullptr, json_str.c_str()); !ret.ok()) {
                log_db::error("{}: Atomic apply via plugin failed: {}", __func__, ret.result());
                return ret.code();
            }

            return 0;
        }
        catch (const std::exception& e) {
            log_db::error("{} :: Caught exception: {}", __func__, e.what());
            return SYS_INTERNAL_ERR;
        }
    } // atomic_apply_database_operations

    auto atomic_apply_database_operations(const std::vector<dml::operation_type>& _ops,
                                          std::error_code& _ec) -> void
    {
        try {
            const auto ec = atomic_apply_database_operations(_ops);
            _ec.assign(ec, std::generic_category());
        }
        catch (...) {
            _ec.assign(SYS_UNKNOWN_ERROR, std::generic_category());
        }
    } // atomic_apply_database_operations
} // namespace irods::experimental
