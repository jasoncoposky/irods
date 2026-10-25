#ifndef IRODS_GENQUERY1_AST_ADAPTER_HPP
#define IRODS_GENQUERY1_AST_ADAPTER_HPP

#include "irods/genquery2_ast_types.hpp"
#include "irods/rodsGenQuery.h"

#include <string_view>

namespace irods::experimental::genquery2
{
    auto genquery1_column_id_to_name(int _col_id) -> std::string_view;

    auto genquery1_column_name_to_id(std::string_view _name) -> int;

    auto genquery1_to_genquery2_ast(const genQueryInp_t& _inp) -> select;
} // namespace irods::experimental::genquery2

#endif // IRODS_GENQUERY1_AST_ADAPTER_HPP
