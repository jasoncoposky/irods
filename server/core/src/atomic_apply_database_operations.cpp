#include "irods/atomic_apply_database_operations.hpp"

#include "irods/icatHighLevelRoutines.hpp"
#include "irods/irods_exception.hpp"
#include "irods/irods_logger.hpp"
#include "irods/rodsErrorTable.h"

#include <exception>
#include <system_error>
#include <vector>

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
            return chl_atomic_apply_operations(nullptr, _ops);
        }
        catch (const irods::exception& e) {
            log_db::error("{} :: Caught exception: {}", __func__, e.what());
            // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
            return e.code();
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
