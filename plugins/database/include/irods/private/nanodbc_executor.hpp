#ifndef IRODS_NANODBC_EXECUTOR_HPP
#define IRODS_NANODBC_EXECUTOR_HPP

#include <nanodbc/nanodbc.h>

#include "irods/private/genquery2_ast_types.hpp"
#include "irods/private/genquery2_sql.hpp"

#include <cstddef>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace irods::experimental::catalog
{
    class statement_cache
    {
    public:
        explicit statement_cache(std::size_t _max_capacity = 128);

        auto get(nanodbc::connection& _conn, std::string_view _sql) -> nanodbc::statement;
        auto put_for_testing(std::string_view _sql, nanodbc::statement _stmt) -> void;
        [[nodiscard]] auto has(std::string_view _sql) const noexcept -> bool;
        [[nodiscard]] auto size() const noexcept -> std::size_t;
        [[nodiscard]] auto capacity() const noexcept -> std::size_t;
        auto clear() noexcept -> void;

    private:
        std::size_t capacity_;
        std::list<std::string> lru_list_;
        std::unordered_map<std::string, std::pair<nanodbc::statement, std::list<std::string>::iterator>> cache_;
    };

    class nanodbc_executor
    {
    public:
        explicit nanodbc_executor(std::size_t _cache_capacity = 128);

        auto execute_query(
            nanodbc::connection& _conn,
            std::string_view _sql,
            const std::vector<std::string>& _params = {}) -> nanodbc::result;

        auto execute_dml(
            nanodbc::connection& _conn,
            std::string_view _sql,
            const std::vector<std::string>& _params = {}) -> std::size_t;

        auto transaction(nanodbc::connection& _conn) -> nanodbc::transaction;

        auto get_next_sequence_value(
            nanodbc::connection& _conn,
            std::string_view _seq_name = "R_OBJECTID",
            int _db_type = 0) -> int64_t;

        auto get_current_sequence_value(
            nanodbc::connection& _conn,
            std::string_view _seq_name = "R_OBJECTID",
            int _db_type = 0) -> int64_t;

        auto query_integer(
            nanodbc::connection& _conn,
            std::string_view _sql,
            const std::vector<std::string>& _params = {}) -> std::optional<int64_t>;

        auto query_string(
            nanodbc::connection& _conn,
            std::string_view _sql,
            const std::vector<std::string>& _params = {}) -> std::optional<std::string>;

        auto query_strings(
            nanodbc::connection& _conn,
            std::string_view _sql,
            const std::vector<std::string>& _params = {}) -> std::vector<std::string>;

        [[nodiscard]] auto cache() noexcept -> statement_cache&;

        auto bind_parameters(nanodbc::statement& _stmt, const std::vector<std::string>& _params) -> void;

    private:
        statement_cache cache_;
    };

    auto get_next_sequence_sql(std::string_view _seq_name, std::string_view _dbms = "") -> std::string;
    auto get_next_sequence_sql(std::string_view _seq_name, int _db_type) -> std::string;
    auto get_current_sequence_sql(std::string_view _seq_name, std::string_view _dbms = "") -> std::string;
    auto get_current_sequence_sql(std::string_view _seq_name, int _db_type) -> std::string;

    struct execution_result
    {
        std::size_t affected_rows = 0;
        std::optional<nanodbc::result> query_result;
    };

    auto execute(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        const genquery2::statement& _stmt,
        const genquery2::options& _opts = {}) -> execution_result;

    inline auto execute_catalog(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        const genquery2::statement& _stmt) -> execution_result
    {
        static constexpr genquery2::options catalog_admin_opts{.admin_mode = true};
        return execute(_exec, _conn, _stmt, catalog_admin_opts);
    }

    inline auto query_catalog_integer(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        const genquery2::statement& _stmt) -> std::optional<int64_t>
    {
        auto res = execute_catalog(_exec, _conn, _stmt);
        if (res.query_result && res.query_result->next()) {
            if (res.query_result->is_null(0)) {
                return std::nullopt;
            }
            return res.query_result->get<int64_t>(0);
        }
        return std::nullopt;
    }

    inline auto query_catalog_string(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        const genquery2::statement& _stmt) -> std::optional<std::string>
    {
        auto res = execute_catalog(_exec, _conn, _stmt);
        if (res.query_result && res.query_result->next()) {
            if (res.query_result->is_null(0)) {
                return std::nullopt;
            }
            return res.query_result->get<std::string>(0);
        }
        return std::nullopt;
    }

    inline auto query_catalog_strings(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        const genquery2::statement& _stmt) -> std::vector<std::string>
    {
        auto res = execute_catalog(_exec, _conn, _stmt);
        std::vector<std::string> results;
        if (res.query_result) {
            while (res.query_result->next()) {
                results.push_back(res.query_result->is_null(0) ? "" : res.query_result->get<std::string>(0));
            }
        }
        return results;
    }
} // namespace irods::experimental::catalog

#endif // IRODS_NANODBC_EXECUTOR_HPP
