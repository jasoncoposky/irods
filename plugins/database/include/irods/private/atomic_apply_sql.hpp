#ifndef IRODS_ATOMIC_APPLY_SQL_HPP
#define IRODS_ATOMIC_APPLY_SQL_HPP

#include "irods/atomic_apply_database_operations.hpp"
#include <nanodbc/nanodbc.h>
#include <string_view>

namespace irods::experimental::catalog {

    auto apply_atomic_operations(nanodbc::connection& _db_conn,
                                 const std::string_view _db_instance_name,
                                 const std::vector<dml::operation_type>& _ops) -> int;

} // namespace irods::experimental::catalog

#endif // IRODS_ATOMIC_APPLY_SQL_HPP
