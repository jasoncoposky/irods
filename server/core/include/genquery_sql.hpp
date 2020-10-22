#ifndef IRODS_GENQUERY_SQL_HPP
#define IRODS_GENQUERY_SQL_HPP

#include <string>
#include "genquery_ast_types.hpp"

namespace irods::experimental::api::genquery {
    std::string sql(const Select&);
}
#endif // IRODS_GENQUERY_SQL_HPP
