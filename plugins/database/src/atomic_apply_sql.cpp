#include "irods/private/atomic_apply_sql.hpp"

#include "irods/rodsErrorTable.h"
#include "irods/irods_exception.hpp"
#include "irods/catalog.hpp"
#include "irods/irods_logger.hpp"
#include "irods/irods_erasure_coding_error_codes.hpp"

#include <boost/iterator/function_input_iterator.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nanodbc/nanodbc.h>

#include <cctype>
#include <string>
#include <string_view>
#include <iterator>
#include <tuple>
#include <array>
#include <vector>
#include <algorithm>
#include <chrono>

namespace irods::experimental::catalog
{
    namespace
    {
        namespace dml = irods::experimental::dml;
        namespace ec_err = irods::erasurecoding::error_codes;

        using log_db            = irods::experimental::log::database;
        using db_operation_type = std::tuple<std::string_view, std::string_view>;
        using db_column_type    = std::tuple<std::string_view, std::string_view>;

        // Helpers for visitor pattern.
        template <typename ...Ts> struct overloaded : Ts... { using Ts::operator()...; };
        template <typename ...Ts> overloaded(Ts...) -> overloaded<Ts...>;

        template <typename T, typename ...Args>
        constexpr auto make_array(Args&&... _args)
        {
            return std::array<T, sizeof...(Args)>{std::forward<Args>(_args)...};
        }

        constexpr auto db_columns = make_array<db_column_type>(
            db_column_type{"r_data_main", "data_id"},
            db_column_type{"r_data_main", "coll_id"},
            db_column_type{"r_data_main", "data_name"},
            db_column_type{"r_data_main", "data_repl_num"},
            db_column_type{"r_data_main", "data_version"},
            db_column_type{"r_data_main", "data_type_name"},
            db_column_type{"r_data_main", "data_size"},
            db_column_type{"r_data_main", "resc_group_name"},
            db_column_type{"r_data_main", "resc_name"},
            db_column_type{"r_data_main", "data_path"},
            db_column_type{"r_data_main", "data_owner_name"},
            db_column_type{"r_data_main", "data_owner_zone"},
            db_column_type{"r_data_main", "data_is_dirty"},
            db_column_type{"r_data_main", "data_status"},
            db_column_type{"r_data_main", "data_checksum"},
            db_column_type{"r_data_main", "data_expiry_ts"},
            db_column_type{"r_data_main", "data_map_id"},
            db_column_type{"r_data_main", "data_mode"},
            db_column_type{"r_data_main", "r_comment"},
            db_column_type{"r_data_main", "create_ts"},
            db_column_type{"r_data_main", "modify_ts"},
            db_column_type{"r_data_main", "resc_hier"},
            db_column_type{"r_data_main", "resc_id"},

            db_column_type{"r_coll_main", "coll_id"},
            db_column_type{"r_coll_main", "parent_coll_name"},
            db_column_type{"r_coll_main", "coll_name"},
            db_column_type{"r_coll_main", "coll_owner_name"},
            db_column_type{"r_coll_main", "coll_owner_zone"},
            db_column_type{"r_coll_main", "coll_map_id"},
            db_column_type{"r_coll_main", "coll_inheritance"},
            db_column_type{"r_coll_main", "coll_type"},
            db_column_type{"r_coll_main", "coll_info1"},
            db_column_type{"r_coll_main", "coll_info2"},
            db_column_type{"r_coll_main", "coll_expiry_ts"},
            db_column_type{"r_coll_main", "r_comment"},
            db_column_type{"r_coll_main", "create_ts"},
            db_column_type{"r_coll_main", "modify_ts"},

            db_column_type{"r_meta_main", "meta_id"},
            db_column_type{"r_meta_main", "meta_namespace"},
            db_column_type{"r_meta_main", "meta_attr_name"},
            db_column_type{"r_meta_main", "meta_attr_value"},
            db_column_type{"r_meta_main", "meta_attr_unit"},
            db_column_type{"r_meta_main", "r_comment"},
            db_column_type{"r_meta_main", "create_ts"},
            db_column_type{"r_meta_main", "modify_ts"},

            db_column_type{"r_objt_metamap", "object_id"},
            db_column_type{"r_objt_metamap", "meta_id"},
            db_column_type{"r_objt_metamap", "create_ts"},
            db_column_type{"r_objt_metamap", "modify_ts"},

            db_column_type{"r_resc_main", "resc_id"},
            db_column_type{"r_resc_main", "resc_name"},
            db_column_type{"r_resc_main", "zone_name"},
            db_column_type{"r_resc_main", "resc_type_name"},
            db_column_type{"r_resc_main", "resc_class_name"},
            db_column_type{"r_resc_main", "resc_net"},
            db_column_type{"r_resc_main", "resc_def_path"},
            db_column_type{"r_resc_main", "free_space"},
            db_column_type{"r_resc_main", "free_space_ts"},
            db_column_type{"r_resc_main", "resc_info"},
            db_column_type{"r_resc_main", "r_comment"},
            db_column_type{"r_resc_main", "resc_status"},
            db_column_type{"r_resc_main", "create_ts"},
            db_column_type{"r_resc_main", "modify_ts"},
            db_column_type{"r_resc_main", "resc_children"},
            db_column_type{"r_resc_main", "resc_context"},
            db_column_type{"r_resc_main", "resc_parent"},
            db_column_type{"r_resc_main", "resc_objcount"},
            db_column_type{"r_resc_main", "resc_parent_context"},

            db_column_type{"r_ticket_allowed_groups", "ticket_id"},
            db_column_type{"r_ticket_allowed_groups", "group_name"},

            db_column_type{"r_ticket_allowed_hosts", "ticket_id"},
            db_column_type{"r_ticket_allowed_hosts", "host"},

            db_column_type{"r_ticket_allowed_users", "ticket_id"},
            db_column_type{"r_ticket_allowed_users", "user_name"},

            db_column_type{"r_ticket_main", "ticket_id"},
            db_column_type{"r_ticket_main", "ticket_string"},
            db_column_type{"r_ticket_main", "ticket_type"},
            db_column_type{"r_ticket_main", "user_id"},
            db_column_type{"r_ticket_main", "object_id"},
            db_column_type{"r_ticket_main", "object_type"},
            db_column_type{"r_ticket_main", "uses_limit"},
            db_column_type{"r_ticket_main", "uses_count"},
            db_column_type{"r_ticket_main", "write_file_limit"},
            db_column_type{"r_ticket_main", "write_file_count"},
            db_column_type{"r_ticket_main", "write_byte_limit"},
            db_column_type{"r_ticket_main", "write_byte_count"},
            db_column_type{"r_ticket_main", "ticket_expiry_ts"},
            db_column_type{"r_ticket_main", "restrictions"},
            db_column_type{"r_ticket_main", "create_ts"},
            db_column_type{"r_ticket_main", "modify_ts"}
        );

        constexpr std::array<std::string_view, 7> supported_operators{"=", "!=", ">", ">=", "<", "<=", "in"};

        constexpr auto supported_operations = make_array<db_operation_type>(
            db_operation_type{"insert", "r_coll_main"},
            db_operation_type{"insert", "r_data_main"},
            db_operation_type{"insert", "r_meta_map"},
            db_operation_type{"insert", "r_objt_metamap"},
            db_operation_type{"insert", "r_resc_main"},
            db_operation_type{"insert", "r_ticket_allowed_groups"},
            db_operation_type{"insert", "r_ticket_allowed_hosts"},
            db_operation_type{"insert", "r_ticket_allowed_users"},
            db_operation_type{"insert", "r_ticket_main"},

            db_operation_type{"update", "r_coll_main"},
            db_operation_type{"update", "r_data_main"},
            db_operation_type{"update", "r_meta_map"},
            db_operation_type{"update", "r_objt_metamap"},
            db_operation_type{"update", "r_resc_main"},
            db_operation_type{"update", "r_ticket_allowed_groups"},
            db_operation_type{"update", "r_ticket_allowed_hosts"},
            db_operation_type{"update", "r_ticket_allowed_users"},
            db_operation_type{"update", "r_ticket_main"},

            db_operation_type{"delete", "r_coll_main"},
            db_operation_type{"delete", "r_data_main"},
            db_operation_type{"delete", "r_meta_map"},
            db_operation_type{"delete", "r_objt_metamap"},
            db_operation_type{"delete", "r_resc_main"},
            db_operation_type{"delete", "r_ticket_allowed_groups"},
            db_operation_type{"delete", "r_ticket_allowed_hosts"},
            db_operation_type{"delete", "r_ticket_allowed_users"},
            db_operation_type{"delete", "r_ticket_main"}
        );

        auto to_upper(std::string& _s) -> std::string& {
            std::transform(std::begin(_s), std::end(_s), std::begin(_s), [](unsigned char _c) { return std::toupper(_c); });
            return _s;
        }

        auto to_upper_copy(const std::string& _s) -> std::string {
            auto t = _s;
            return to_upper(t);
        }

        auto current_timestamp() -> std::string {
            using std::chrono::system_clock;
            using std::chrono::duration_cast;
            using std::chrono::seconds;
            return fmt::format("{:011}", duration_cast<seconds>(system_clock::now().time_since_epoch()).count());
        }

        template <typename Container>
        auto required_value(const Container& _data, const std::string_view _column_name) -> std::string_view {
            const auto end = std::end(_data);
            const auto iter = std::find_if(std::begin(_data), end, [&_column_name](const dml::column& _v) { return _v.name == _column_name; });
            if (iter == end) { THROW(SYS_INVALID_INPUT_PARAM, fmt::format("No data set for column [{}].", _column_name)); }
            return iter->value;
        }

        template <typename Container>
        auto optional_value(const Container& _data, const std::string_view _column_name, const std::string_view _default_value) -> std::string_view {
            const auto end = std::end(_data);
            const auto iter = std::find_if(std::begin(_data), end, [&_column_name](const dml::column& _v) { return _v.name == _column_name; });
            if (iter == end) { return _default_value; }
            return iter->value;
        }

        auto database_type_not_supported(const std::string_view _db_instance_name, const std::string_view _function_name) noexcept -> int {
            log_db::error("{} :: Database type not supported [{}].", _function_name, _db_instance_name);
            return DATABASE_TYPE_NOT_SUPPORTED;
        }

        auto throw_if_invalid_table_column(const std::string_view _column, const std::string_view _table_name) -> void {
            const auto pred = [target = db_column_type{_table_name, _column}](const db_column_type& _v) noexcept { return _v == target; };
            if (std::none_of(std::begin(db_columns), std::end(db_columns), pred)) {
                const auto msg = fmt::format("Invalid table column [{}.{}].", _table_name, _column);
                log_db::error("{} :: {}", __func__, msg);
                THROW(SYS_INVALID_INPUT_PARAM, msg);
            }
        }

        auto throw_if_invalid_conditional_operator(const std::string_view _operator) -> void {
            const auto pred = [&_operator](const std::string_view _v) noexcept { return _v == _operator; };
            if (std::none_of(std::begin(supported_operators), std::end(supported_operators), pred)) {
                const auto msg = fmt::format("Invalid conditional operator [{}].", _operator);
                log_db::error("{} :: {}", __func__, msg);
                THROW(SYS_INVALID_INPUT_PARAM, msg);
            }
        }

        auto init_conditionals(const std::string_view _table_name, const std::vector<dml::condition>& _conditions) -> std::tuple<std::string, std::vector<std::string>> {
            using vector_type = std::vector<std::string>;
            vector_type condition_clauses; condition_clauses.reserve(_conditions.size());
            vector_type values; values.reserve(_conditions.size());
            for (auto&& c : _conditions) {
                throw_if_invalid_table_column(c.column, _table_name);
                throw_if_invalid_conditional_operator(c.op);
                if ("in" == c.op) {
                    struct {
                        using result_type = std::string_view;
                        auto operator()() const noexcept -> result_type { return "?"; }
                    } gen;
                    auto first = boost::make_function_input_iterator(gen, vector_type::size_type{0});
                    auto last = boost::make_function_input_iterator(gen, c.values.size());
                    condition_clauses.push_back(fmt::format("{} in ({})", c.column, fmt::join(first, last, ", ")));
                    values.insert(std::end(values), std::begin(c.values), std::end(c.values));
                } else {
                    condition_clauses.push_back(fmt::format("{} {} ?", c.column, c.op));
                    values.push_back(c.values.front());
                }
            }
            return {fmt::format("{}", fmt::join(condition_clauses, " and ")), std::move(values)};
        }

        auto get_non_ticket_id_column(const std::vector<dml::column>& _columns) -> const dml::column* {
            const auto end = std::end(_columns);
            const auto iter = std::find_if_not(std::begin(_columns), end, [](const dml::column& _v) { return _v.name == "ticket_id"; });
            return (iter == end) ? nullptr : &*iter;
        }

        auto is_db_operation_supported(const std::string_view _db_op, const std::string_view _table_name) -> bool {
            const auto pred = [target = db_operation_type{_db_op, _table_name}](const db_operation_type& _v) noexcept { return _v == target; };
            if (std::none_of(std::begin(supported_operations), std::end(supported_operations), pred)) {
                log_db::error("{} :: Database operation [{}] not supported on table [{}].", __func__, _db_op, _table_name);
                return false;
            }
            return true;
        }

        auto insert_collection(nanodbc::connection& _db_conn, const std::string_view _db_instance_name, const dml::insert_op& _op) -> int {
            try {
                std::string_view sql;
                if (_db_instance_name == "postgres") { sql = "insert into R_COLL_MAIN (coll_id, parent_coll_name, coll_name, coll_owner_name, coll_owner_zone, coll_map_id, coll_inheritance, coll_type, coll_info1, coll_info2, coll_expiry_ts, r_comment, create_ts, modify_ts) values (nextval('R_OBJECTID'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; }
                else if (_db_instance_name == "oracle") { sql = "insert into R_COLL_MAIN (coll_id, parent_coll_name, coll_name, coll_owner_name, coll_owner_zone, coll_map_id, coll_inheritance, coll_type, coll_info1, coll_info2, coll_expiry_ts, r_comment, create_ts, modify_ts) values (select R_OBJECTID.nextval from DUAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; }
                else if (_db_instance_name == "mysql") { sql = "insert into R_COLL_MAIN (coll_id, parent_coll_name, coll_name, coll_owner_name, coll_owner_zone, coll_map_id, coll_inheritance, coll_type, coll_info1, coll_info2, coll_expiry_ts, r_comment, create_ts, modify_ts) values (R_OBJECTID_nextval(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; }
                else { return database_type_not_supported(_db_instance_name, __func__); }
                const auto timestamp = current_timestamp();
                nanodbc::statement stmt{_db_conn}; nanodbc::prepare(stmt, sql.data());
                int i = 0;
                stmt.bind(i++, required_value(_op.data, "parent_coll_name").data());
                stmt.bind(i++, required_value(_op.data, "coll_name").data());
                stmt.bind(i++, required_value(_op.data, "coll_owner_name").data());
                stmt.bind(i++, required_value(_op.data, "coll_owner_zone").data());
                stmt.bind(i++, optional_value(_op.data, "coll_map_id", "0").data());
                stmt.bind(i++, optional_value(_op.data, "coll_inheritance", "").data());
                stmt.bind(i++, optional_value(_op.data, "coll_type", "").data());
                stmt.bind(i++, optional_value(_op.data, "coll_info1", "").data());
                stmt.bind(i++, optional_value(_op.data, "coll_info2", "").data());
                stmt.bind(i++, optional_value(_op.data, "coll_expiry_ts", "").data());
                stmt.bind(i++, optional_value(_op.data, "r_comment", "").data());
                stmt.bind(i++, optional_value(_op.data, "create_ts", timestamp).data());
                stmt.bind(i++, optional_value(_op.data, "modify_ts", timestamp).data());
                nanodbc::execute(stmt);
            } catch (const std::exception& e) { THROW(SYS_LIBRARY_ERROR, e.what()); }
            return 0;
        }

        auto insert_replica(nanodbc::connection& _db_conn, const std::string_view _db_instance_name, const dml::insert_op& _op) -> int {
            try {
                std::string_view sql;
                if (_db_instance_name == "postgres") { sql = "insert into R_DATA_MAIN (data_id, coll_id, data_name, data_repl_num, data_version, data_type_name, data_size, resc_group_name, resc_name, data_path, data_owner_name, data_owner_zone, data_is_dirty, data_status, data_checksum, data_expiry_ts, data_map_id, data_mode, r_comment, create_ts, modify_ts, resc_hier, resc_id) values (nextval('R_OBJECTID'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; }
                else if (_db_instance_name == "oracle") { sql = "insert into R_DATA_MAIN (data_id, coll_id, data_name, data_repl_num, data_version, data_type_name, data_size, resc_group_name, resc_name, data_path, data_owner_name, data_owner_zone, data_is_dirty, data_status, data_checksum, data_expiry_ts, data_map_id, data_mode, r_comment, create_ts, modify_ts, resc_hier, resc_id) values (select R_OBJECTID.nextval from DUAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; }
                else if (_db_instance_name == "mysql") { sql = "insert into R_DATA_MAIN (data_id, coll_id, data_name, data_repl_num, data_version, data_type_name, data_size, resc_group_name, resc_name, data_path, data_owner_name, data_owner_zone, data_is_dirty, data_status, data_checksum, data_expiry_ts, data_map_id, data_mode, r_comment, create_ts, modify_ts, resc_hier, resc_id) values (R_OBJECTID_nextval(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; }
                else { return database_type_not_supported(_db_instance_name, __func__); }
                const auto timestamp = current_timestamp();
                nanodbc::statement stmt{_db_conn}; nanodbc::prepare(stmt, sql.data());
                int i = 0;
                stmt.bind(i++, required_value(_op.data, "coll_id").data());
                stmt.bind(i++, required_value(_op.data, "data_name").data());
                stmt.bind(i++, required_value(_op.data, "data_repl_num").data());
                stmt.bind(i++, optional_value(_op.data, "data_version", "0").data());
                stmt.bind(i++, required_value(_op.data, "data_type_name").data());
                stmt.bind(i++, optional_value(_op.data, "data_size", "0").data());
                stmt.bind(i++, optional_value(_op.data, "resc_group_name", "EMPTY_RESC_GROUP_NAME").data());
                stmt.bind(i++, optional_value(_op.data, "resc_name", "EMPTY_RESC_NAME").data());
                stmt.bind(i++, required_value(_op.data, "data_path").data());
                stmt.bind(i++, required_value(_op.data, "data_owner_name").data());
                stmt.bind(i++, required_value(_op.data, "data_owner_zone").data());
                stmt.bind(i++, optional_value(_op.data, "data_is_dirty", "0").data());
                stmt.bind(i++, optional_value(_op.data, "data_status", "").data());
                stmt.bind(i++, optional_value(_op.data, "data_checksum", "").data());
                stmt.bind(i++, optional_value(_op.data, "data_expiry_ts", "").data());
                stmt.bind(i++, optional_value(_op.data, "data_map_id", "0").data());
                stmt.bind(i++, optional_value(_op.data, "data_mode", "").data());
                stmt.bind(i++, optional_value(_op.data, "r_comment", "").data());
                stmt.bind(i++, optional_value(_op.data, "create_ts", timestamp).data());
                stmt.bind(i++, optional_value(_op.data, "modify_ts", timestamp).data());
                stmt.bind(i++, optional_value(_op.data, "resc_hier", "EMPTY_RESC_HIER").data());
                stmt.bind(i++, required_value(_op.data, "resc_id").data());
                nanodbc::execute(stmt);
            } catch (const std::exception& e) { THROW(SYS_LIBRARY_ERROR, e.what()); }
            return 0;
        }

        auto update_rows(nanodbc::connection& _db_conn, const std::string_view _db_instance_name, const dml::update_op& _op) -> int {
            try {
                if (_op.data.empty() || _op.conditions.empty()) return SYS_INVALID_INPUT_PARAM;
                std::vector<std::string> set_clauses; std::vector<std::string> set_bind_args;
                for (auto&& [name, value] : _op.data) {
                    throw_if_invalid_table_column(name, _op.table);
                    set_clauses.push_back(fmt::format("{} = ?", name));
                    set_bind_args.push_back(value);
                }
                const auto [conditions_string, conditions_bind_args] = init_conditionals(_op.table, _op.conditions);
                const auto sql = fmt::format("update {} set {} where {}", to_upper_copy(_op.table), fmt::join(set_clauses, ", "), conditions_string);
                nanodbc::statement stmt{_db_conn}; nanodbc::prepare(stmt, sql);
                for (std::size_t i = 0; i < set_bind_args.size(); ++i) stmt.bind(i, set_bind_args[i].data());
                for (std::size_t i = 0; i < conditions_bind_args.size(); ++i) stmt.bind(i + set_bind_args.size(), conditions_bind_args[i].data());
                nanodbc::execute(stmt);
            } catch (const std::exception& e) { THROW(SYS_LIBRARY_ERROR, e.what()); }
            return 0;
        }

        auto delete_rows(nanodbc::connection& _db_conn, const std::string_view _db_instance_name, const dml::delete_op& _op) -> int {
            try {
                if (_op.conditions.empty()) return SYS_INVALID_INPUT_PARAM;
                const auto [conditions_string, bind_args] = init_conditionals(_op.table, _op.conditions);
                const auto sql = fmt::format("delete from {} where {}", to_upper_copy(_op.table), conditions_string);
                nanodbc::statement stmt{_db_conn}; nanodbc::prepare(stmt, sql);
                for (std::size_t i = 0; i < bind_args.size(); ++i) stmt.bind(i, bind_args[i].data());
                nanodbc::execute(stmt);
            } catch (const std::exception& e) { THROW(SYS_LIBRARY_ERROR, e.what()); }
            return 0;
        }

        auto exec_insert(nanodbc::connection& _db_conn, const std::string_view _db_instance_name, const dml::insert_op& _op) -> int {
            if (!is_db_operation_supported("insert", _op.table)) return INVALID_OPERATION;
            if (_op.table == "r_coll_main") return insert_collection(_db_conn, _db_instance_name, _op);
            if (_op.table == "r_data_main") return insert_replica(_db_conn, _db_instance_name, _op);
            // ... (other tables)
            return ec_err::WRITE_ERROR;
        }

        auto exec_update(nanodbc::connection& _db_conn, const std::string_view _db_instance_name, const dml::update_op& _op) -> int {
            if (!is_db_operation_supported("update", _op.table)) return INVALID_OPERATION;
            return update_rows(_db_conn, _db_instance_name, _op);
        }

        auto exec_delete(nanodbc::connection& _db_conn, const std::string_view _db_instance_name, const dml::delete_op& _op) -> int {
            if (!is_db_operation_supported("delete", _op.table)) return INVALID_OPERATION;
            return delete_rows(_db_conn, _db_instance_name, _op);
        }
    } // anonymous namespace

    auto apply_atomic_operations(nanodbc::connection& _db_conn,
                                 const std::string_view _db_instance_name,
                                 const std::vector<dml::operation_type>& _ops) -> int
    {
        for (auto&& op : _ops) {
            const auto ec = std::visit(overloaded{
                [&_db_conn, &_db_instance_name](const dml::insert_op& _op) { return exec_insert(_db_conn, _db_instance_name, _op); },
                [&_db_conn, &_db_instance_name](const dml::update_op& _op) { return exec_update(_db_conn, _db_instance_name, _op); },
                [&_db_conn, &_db_instance_name](const dml::delete_op& _op) { return exec_delete(_db_conn, _db_instance_name, _op); }
            }, op);
            if (ec < 0) return ec;
        }
        return 0;
    }

} // namespace irods::experimental::catalog
