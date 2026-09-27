#include "irods/private/nanodbc_executor.hpp"
#include "irods/private/db_flavor_table.hpp"

#include "irods/irods_exception.hpp"
#include "irods/rodsErrorTable.h"

#include <fmt/format.h>

#include <sql.h>
#include <sqlext.h>

#include <algorithm>
#include <dlfcn.h>
#include <type_traits>
#include <variant>

namespace
{
    auto close_cursor_safely(void* _handle) noexcept -> void
    {
        if (_handle == nullptr) {
            return;
        }
        using SQLFreeStmtFunc = short(*)(void*, short);
        static auto* sql_free_stmt = reinterpret_cast<SQLFreeStmtFunc>(dlsym(RTLD_DEFAULT, "SQLFreeStmt"));
        if (sql_free_stmt) {
            sql_free_stmt(_handle, SQL_CLOSE);
        }
    }
} // anonymous namespace

namespace irods::experimental::catalog
{
    statement_cache::statement_cache(std::size_t _max_capacity)
        : capacity_{_max_capacity}
    {
    }

    auto statement_cache::get(nanodbc::connection& _conn, std::string_view _sql) -> nanodbc::statement
    {
        const std::string sql_str{_sql};
        auto it = cache_.find(sql_str);

        if (it != cache_.end()) {
            auto& cached_stmt = it->second.first;
            if (cached_stmt.connected() && cached_stmt.open() &&
                cached_stmt.connection().native_dbc_handle() == _conn.native_dbc_handle()) {
                lru_list_.erase(it->second.second);
                lru_list_.push_front(sql_str);
                it->second.second = lru_list_.begin();
                return cached_stmt;
            }
            lru_list_.erase(it->second.second);
            cache_.erase(it);
        }

        nanodbc::statement stmt{_conn};
        nanodbc::prepare(stmt, sql_str);

        if (cache_.size() >= capacity_ && !lru_list_.empty()) {
            const auto& oldest = lru_list_.back();
            cache_.erase(oldest);
            lru_list_.pop_back();
        }

        lru_list_.push_front(sql_str);
        cache_.emplace(sql_str, std::make_pair(stmt, lru_list_.begin()));

        return stmt;
    }

    auto statement_cache::put_for_testing(std::string_view _sql, nanodbc::statement _stmt) -> void
    {
        const std::string sql_str{_sql};
        auto it = cache_.find(sql_str);

        if (it != cache_.end()) {
            lru_list_.erase(it->second.second);
            lru_list_.push_front(sql_str);
            it->second.second = lru_list_.begin();
            it->second.first = std::move(_stmt);
            return;
        }

        if (cache_.size() >= capacity_ && !lru_list_.empty()) {
            const auto& oldest = lru_list_.back();
            cache_.erase(oldest);
            lru_list_.pop_back();
        }

        lru_list_.push_front(sql_str);
        cache_.emplace(sql_str, std::make_pair(std::move(_stmt), lru_list_.begin()));
    }

    auto statement_cache::has(std::string_view _sql) const noexcept -> bool
    {
        return cache_.find(std::string{_sql}) != cache_.end();
    }

    auto statement_cache::size() const noexcept -> std::size_t
    {
        return cache_.size();
    }

    auto statement_cache::capacity() const noexcept -> std::size_t
    {
        return capacity_;
    }

    auto statement_cache::clear() noexcept -> void
    {
        cache_.clear();
        lru_list_.clear();
    }

    nanodbc_executor::nanodbc_executor(std::size_t _cache_capacity)
        : cache_{_cache_capacity}
    {
    }

    auto nanodbc_executor::cache() noexcept -> statement_cache&
    {
        return cache_;
    }

    auto nanodbc_executor::transaction(nanodbc::connection& _conn) -> nanodbc::transaction
    {
        return nanodbc::transaction{_conn};
    }

    auto nanodbc_executor::bind_parameters(nanodbc::statement& _stmt, const std::vector<std::string>& _params) -> void
    {
        close_cursor_safely(_stmt.native_statement_handle());
        _stmt.reset_parameters();

        if (_params.empty()) {
            return;
        }

        std::vector<short> idx;
        std::vector<short> type;
        std::vector<unsigned long> size;
        std::vector<short> scale;

        idx.reserve(_params.size());
        type.reserve(_params.size());
        size.reserve(_params.size());
        scale.reserve(_params.size());

        for (std::vector<std::string>::size_type i = 0; i < _params.size(); ++i) {
            idx.push_back(static_cast<short>(i));
            type.push_back(SQL_VARCHAR);
            size.push_back(std::max<unsigned long>(_params[i].size(), 1UL));
            scale.push_back(0);
        }

        _stmt.describe_parameters(idx, type, size, scale);

        for (std::vector<std::string>::size_type i = 0; i < _params.size(); ++i) {
            _stmt.bind(static_cast<short>(i), _params[i].c_str());
        }
    }

    auto nanodbc_executor::execute_query(
        nanodbc::connection& _conn,
        std::string_view _sql,
        const std::vector<std::string>& _params) -> nanodbc::result
    {
        auto stmt = cache_.get(_conn, _sql);
        bind_parameters(stmt, _params);
        return nanodbc::execute(stmt);
    }

    auto nanodbc_executor::execute_dml(
        nanodbc::connection& _conn,
        std::string_view _sql,
        const std::vector<std::string>& _params) -> std::size_t
    {
        auto stmt = cache_.get(_conn, _sql);
        bind_parameters(stmt, _params);
        const auto result = nanodbc::execute(stmt);
        return static_cast<std::size_t>(result.affected_rows());
    }

    auto get_next_sequence_sql(std::string_view _seq_name, int _db_type) -> std::string
    {
        return fmt::format(fmt::runtime(get_db_flavor(_db_type).next_sequence_sql), _seq_name);
    }

    auto get_current_sequence_sql(std::string_view _seq_name, int _db_type) -> std::string
    {
        return fmt::format(fmt::runtime(get_db_flavor(_db_type).current_sequence_sql), _seq_name);
    }

    auto get_next_sequence_sql(std::string_view _seq_name, std::string_view _dbms) -> std::string
    {
        return get_next_sequence_sql(_seq_name, get_db_type_from_name(_dbms));
    }

    auto get_current_sequence_sql(std::string_view _seq_name, std::string_view _dbms) -> std::string
    {
        return get_current_sequence_sql(_seq_name, get_db_type_from_name(_dbms));
    }

    auto nanodbc_executor::get_next_sequence_value(
        nanodbc::connection& _conn,
        std::string_view _seq_name,
        int _db_type) -> int64_t
    {
        std::string sql;
        if (_db_type > 0) {
            sql = get_next_sequence_sql(_seq_name, _db_type);
        }
        else {
            std::string dbms;
            try {
                if (_conn.connected()) {
                    dbms = _conn.dbms_name();
                }
            }
            catch (...) {
            }
            sql = get_next_sequence_sql(_seq_name, dbms);
        }

        auto res = execute_query(_conn, sql);
        if (res.next()) {
            return res.get<int64_t>(0);
        }
        throw std::runtime_error(fmt::format("Failed to get next sequence value for [{}]", _seq_name));
    }

    auto nanodbc_executor::get_current_sequence_value(
        nanodbc::connection& _conn,
        std::string_view _seq_name,
        int _db_type) -> int64_t
    {
        std::string sql;
        if (_db_type > 0) {
            sql = get_current_sequence_sql(_seq_name, _db_type);
        }
        else {
            std::string dbms;
            try {
                if (_conn.connected()) {
                    dbms = _conn.dbms_name();
                }
            }
            catch (...) {
            }
            sql = get_current_sequence_sql(_seq_name, dbms);
        }

        auto res = execute_query(_conn, sql);
        if (res.next()) {
            return res.get<int64_t>(0);
        }
        throw std::runtime_error(fmt::format("Failed to get current sequence value for [{}]", _seq_name));
    }

    auto nanodbc_executor::query_integer(
        nanodbc::connection& _conn,
        std::string_view _sql,
        const std::vector<std::string>& _params) -> std::optional<int64_t>
    {
        auto res = execute_query(_conn, _sql, _params);
        if (res.next()) {
            if (res.is_null(0)) {
                return std::nullopt;
            }
            return res.get<int64_t>(0);
        }
        return std::nullopt;
    }

    auto nanodbc_executor::query_string(
        nanodbc::connection& _conn,
        std::string_view _sql,
        const std::vector<std::string>& _params) -> std::optional<std::string>
    {
        auto res = execute_query(_conn, _sql, _params);
        if (res.next()) {
            if (res.is_null(0)) {
                return std::nullopt;
            }
            return res.get<std::string>(0);
        }
        return std::nullopt;
    }

    auto nanodbc_executor::query_strings(
        nanodbc::connection& _conn,
        std::string_view _sql,
        const std::vector<std::string>& _params) -> std::vector<std::string>
    {
        auto res = execute_query(_conn, _sql, _params);
        std::vector<std::string> results;
        while (res.next()) {
            results.push_back(res.is_null(0) ? "" : res.get<std::string>(0));
        }
        return results;
    }

    auto execute(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        const genquery2::statement& _stmt,
        const genquery2::options& _opts) -> execution_result
    {
        if (!std::holds_alternative<genquery2::select>(_stmt) && !_opts.admin_mode) {
            THROW(CAT_INSUFFICIENT_PRIVILEGE_LEVEL,
                  "Execution of GenQuery2 modification statements requires rodsadmin privileges");
        }

        auto [sql, params] = genquery2::to_sql(_stmt, _opts);

        return std::visit(
            [&](const auto& s) -> execution_result {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, genquery2::select>) {
                    return execution_result{0, _exec.execute_query(_conn, sql, params)};
                }
                else {
                    return execution_result{_exec.execute_dml(_conn, sql, params), std::nullopt};
                }
            },
            _stmt);
    }
} // namespace irods::experimental::catalog
