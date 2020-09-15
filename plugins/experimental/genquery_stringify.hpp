#ifndef GENQUERY_STRINGIFY_HPP
#define GENQUERY_STRINGIFY_HPP

#include <string>
#include "genquery_ast_types.hpp"

namespace irods::experimental::api::genquery {
    std::string stringify(const Select&);
}
#endif // GENQUERY_STRINGIFY_HPP
