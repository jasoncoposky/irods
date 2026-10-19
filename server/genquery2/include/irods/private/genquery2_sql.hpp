#ifndef IRODS_GENQUERY2_SQL_HPP
#define IRODS_GENQUERY2_SQL_HPP

#include "irods/private/genquery2_ast_types.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace irods::experimental::genquery2
{
    struct options
    {
        std::string_view user_name;
        std::string_view user_zone;
        std::string_view database;
        std::uint16_t default_number_of_rows = 16;
        bool admin_mode = false;
    }; // struct options

    auto to_sql(const select& _select, const options& _opts) -> std::tuple<std::string, std::vector<std::string>>;
    auto to_sql(const insert& _ins, const options& _opts) -> std::tuple<std::string, std::vector<std::string>>;
    auto to_sql(const update& _upd, const options& _opts) -> std::tuple<std::string, std::vector<std::string>>;
    auto to_sql(const remove& _rem, const options& _opts) -> std::tuple<std::string, std::vector<std::string>>;
    auto to_sql(const statement& _stmt, const options& _opts) -> std::tuple<std::string, std::vector<std::string>>;
} // namespace irods::experimental::genquery2

#endif // IRODS_GENQUERY2_SQL_HPP
