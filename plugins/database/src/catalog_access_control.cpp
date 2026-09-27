#include "irods/private/catalog_access_control.hpp"

#include "irods/irods_logger.hpp"
#include "irods/rodsErrorTable.h"

#include <filesystem>
#include <fmt/format.h>

#include <chrono>
#include <cstdlib>
#include <cstring>

namespace irods::experimental::catalog::access_control
{
    namespace
    {
        using log_db = irods::experimental::log::database;

        auto get_current_time_seconds() -> int64_t
        {
            return std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        }
    } // anonymous namespace

    auto check_collection_access(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        bool _admin_mode) -> int64_t
    {
        const std::string dir_str{_dir_name};

        if (_admin_mode) {
            auto coll_id_opt = _exec.query_integer(
                _conn,
                "select coll_id from R_COLL_MAIN where coll_name = ?",
                {dir_str});

            return coll_id_opt.has_value() ? coll_id_opt.value() : CAT_UNKNOWN_COLLECTION;
        }

        const std::string user_str{_user_name};
        const std::string zone_str{_user_zone};
        const std::string access_str{_access_level};

        const std::string sql =
            "select CM.coll_id "
            "from R_COLL_MAIN CM, R_OBJT_ACCESS OA, R_USER_GROUP UG, R_USER_MAIN UM, R_TOKN_MAIN TM "
            "where CM.coll_name = ? and "
                  "UM.user_name = ? and "
                  "UM.zone_name = ? and "
                  "UM.user_type_name != 'rodsgroup' and "
                  "UM.user_id = UG.user_id and "
                  "OA.object_id = CM.coll_id and "
                  "UG.group_user_id = OA.user_id and "
                  "OA.access_type_id >= TM.token_id and "
                  "TM.token_namespace = 'access_type' and "
                  "TM.token_name = ?";

        auto coll_id_opt = _exec.query_integer(_conn, sql, {dir_str, user_str, zone_str, access_str});
        if (coll_id_opt.has_value()) {
            return coll_id_opt.value();
        }

        auto exists_opt = _exec.query_integer(
            _conn,
            "select coll_id from R_COLL_MAIN where coll_name = ?",
            {dir_str});

        if (!exists_opt.has_value()) {
            return CAT_UNKNOWN_COLLECTION;
        }

        return CAT_NO_ACCESS_PERMISSION;
    }

    auto check_collection_access_and_inherit(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        int* _inherit_flag,
        std::string_view _ticket_str,
        std::string_view _ticket_host) -> int64_t
    {
        if (_inherit_flag) {
            *_inherit_flag = 0;
        }

        const std::string dir_str{_dir_name};
        const std::string ticket_str{_ticket_str};

        int64_t coll_id = -1;
        bool found = false;

        if (!ticket_str.empty()) {
            const std::string sql =
                "select CM.coll_id, CM.coll_inheritance "
                "from R_COLL_MAIN CM, R_TICKET_MAIN TM "
                "where CM.coll_name = ? and TM.ticket_string = ? and TM.ticket_type = 'write' and TM.object_id = CM.coll_id";

            auto res = _exec.execute_query(_conn, sql, {dir_str, ticket_str});
            if (res.next()) {
                coll_id = res.get<int64_t>(0);
                if (_inherit_flag && !res.is_null(1)) {
                    *_inherit_flag = (res.get<std::string>(1) == "1") ? 1 : 0;
                }
                found = true;
            }
        }
        else {
            const std::string user_str{_user_name};
            const std::string zone_str{_user_zone};
            const std::string access_str{_access_level};

            const std::string sql =
                "select CM.coll_id, CM.coll_inheritance "
                "from R_COLL_MAIN CM, R_OBJT_ACCESS OA, R_USER_GROUP UG, R_USER_MAIN UM, R_TOKN_MAIN TM "
                "where CM.coll_name = ? and UM.user_name = ? and UM.zone_name = ? and "
                      "UM.user_type_name != 'rodsgroup' and UM.user_id = UG.user_id and "
                      "OA.object_id = CM.coll_id and UG.group_user_id = OA.user_id and "
                      "OA.access_type_id >= TM.token_id and TM.token_namespace = 'access_type' and TM.token_name = ?";

            auto res = _exec.execute_query(_conn, sql, {dir_str, user_str, zone_str, access_str});
            if (res.next()) {
                coll_id = res.get<int64_t>(0);
                if (_inherit_flag && !res.is_null(1)) {
                    *_inherit_flag = (res.get<std::string>(1) == "1") ? 1 : 0;
                }
                found = true;
            }
        }

        if (!found) {
            auto exists_opt = _exec.query_integer(
                _conn,
                "select coll_id from R_COLL_MAIN where coll_name = ?",
                {dir_str});

            if (!exists_opt.has_value()) {
                return CAT_UNKNOWN_COLLECTION;
            }
            return CAT_NO_ACCESS_PERMISSION;
        }

        if (!ticket_str.empty()) {
            const auto ticket_status = check_object_id_by_ticket(
                _exec, _conn, std::to_string(coll_id), _access_level, _ticket_str, _ticket_host, _user_name, _user_zone);
            if (ticket_status != 0) {
                return ticket_status;
            }
        }

        return coll_id;
    }

    auto check_collection_id(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_id,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level) -> int64_t
    {
        const std::string dir_id_str{_dir_id};
        const std::string user_str{_user_name};
        const std::string zone_str{_user_zone};
        const std::string access_str{_access_level};

        const std::string sql =
            "select OA.object_id "
            "from R_OBJT_ACCESS OA, R_USER_GROUP UG, R_USER_MAIN UM, R_TOKN_MAIN TM "
            "where UM.user_name = ? and UM.zone_name = ? and UM.user_type_name != 'rodsgroup' and "
                  "UM.user_id = UG.user_id and OA.object_id = ? and UG.group_user_id = OA.user_id and "
                  "OA.access_type_id >= TM.token_id and TM.token_namespace = 'access_type' and TM.token_name = ?";

        auto obj_id_opt = _exec.query_integer(_conn, sql, {user_str, zone_str, dir_id_str, access_str});
        if (obj_id_opt.has_value()) {
            return 0;
        }

        auto exists_opt = _exec.query_integer(
            _conn,
            "select coll_id from R_COLL_MAIN where coll_id = ?",
            {dir_id_str});

        if (!exists_opt.has_value()) {
            return CAT_UNKNOWN_COLLECTION;
        }
        return CAT_NO_ACCESS_PERMISSION;
    }

    auto check_collection_own(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _user_name,
        std::string_view _user_zone) -> int64_t
    {
        auto coll_id_opt = _exec.query_integer(
            _conn,
            "select coll_id from R_COLL_MAIN where coll_name = ? and coll_owner_name = ? and coll_owner_zone = ?",
            {std::string{_dir_name}, std::string{_user_name}, std::string{_user_zone}});

        if (coll_id_opt.has_value()) {
            return coll_id_opt.value();
        }
        return CAT_NO_ACCESS_PERMISSION;
    }

    auto check_data_object_only(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _data_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        bool _admin_mode) -> int64_t
    {
        const std::string dir_str{_dir_name};
        const std::string data_str{_data_name};

        if (_admin_mode) {
            const std::string sql =
                "select DM.data_id "
                "from R_DATA_MAIN DM "
                "inner join R_COLL_MAIN CM on DM.coll_id = CM.coll_id "
                "where DM.data_name = ? and CM.coll_name = ?";

            auto data_id_opt = _exec.query_integer(_conn, sql, {data_str, dir_str});
            return data_id_opt.has_value() ? data_id_opt.value() : CAT_UNKNOWN_FILE;
        }

        const std::string user_str{_user_name};
        const std::string zone_str{_user_zone};
        const std::string access_str{_access_level};

        const std::string sql =
            "select DM.data_id "
            "from R_DATA_MAIN DM, R_OBJT_ACCESS OA, R_USER_GROUP UG, R_USER_MAIN UM, R_TOKN_MAIN TM, R_COLL_MAIN CM "
            "where DM.data_name = ? and DM.coll_id = CM.coll_id and CM.coll_name = ? and "
                  "UM.user_name = ? and UM.zone_name = ? and UM.user_type_name != 'rodsgroup' and "
                  "UM.user_id = UG.user_id and OA.object_id = DM.data_id and UG.group_user_id = OA.user_id and "
                  "OA.access_type_id >= TM.token_id and TM.token_namespace = 'access_type' and TM.token_name = ?";

        auto data_id_opt = _exec.query_integer(_conn, sql, {data_str, dir_str, user_str, zone_str, access_str});
        if (data_id_opt.has_value()) {
            return data_id_opt.value();
        }

        const std::string exists_sql =
            "select DM.data_id from R_DATA_MAIN DM, R_COLL_MAIN CM "
            "where DM.data_name = ? and DM.coll_id = CM.coll_id and CM.coll_name = ?";

        auto exists_opt = _exec.query_integer(_conn, exists_sql, {data_str, dir_str});
        if (!exists_opt.has_value()) {
            return CAT_UNKNOWN_FILE;
        }
        return CAT_NO_ACCESS_PERMISSION;
    }

    auto check_data_object_own(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _data_name,
        std::string_view _user_name,
        std::string_view _user_zone) -> int64_t
    {
        auto coll_id_opt = _exec.query_integer(
            _conn,
            "select coll_id from R_COLL_MAIN where coll_name = ?",
            {std::string{_dir_name}});

        if (!coll_id_opt.has_value()) {
            return CAT_UNKNOWN_COLLECTION;
        }

        auto data_id_opt = _exec.query_integer(
            _conn,
            "select data_id from R_DATA_MAIN where data_name = ? and coll_id = ? and data_owner_name = ? and data_owner_zone = ?",
            {std::string{_data_name}, std::to_string(coll_id_opt.value()), std::string{_user_name}, std::string{_user_zone}});

        if (data_id_opt.has_value()) {
            return data_id_opt.value();
        }
        return CAT_NO_ACCESS_PERMISSION;
    }

    auto check_user_in_group(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _group_name) -> int
    {
        auto user_id_opt = _exec.query_string(
            _conn,
            "select user_id from R_USER_MAIN where user_name = ? and zone_name = ? and user_type_name != 'rodsgroup'",
            {std::string{_user_name}, std::string{_user_zone}});

        if (!user_id_opt.has_value()) {
            return CAT_INVALID_USER;
        }

        const std::string sql =
            "select group_user_id from R_USER_GROUP where user_id = ? and group_user_id = "
            "(select user_id from R_USER_MAIN where user_type_name = 'rodsgroup' and user_name = ?)";

        auto grp_user_id_opt = _exec.query_integer(_conn, sql, {user_id_opt.value(), std::string{_group_name}});
        if (!grp_user_id_opt.has_value()) {
            return CAT_NO_ROWS_FOUND;
        }
        return 0;
    }

    auto check_ticket_restrictions(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _ticket_id,
        std::string_view _ticket_host,
        std::string_view _user_name,
        std::string_view _user_zone) -> int
    {
        const std::string ticket_id_str{_ticket_id};

        // 1. Host restrictions
        auto allowed_hosts = _exec.query_strings(
            _conn,
            "select host from R_TICKET_ALLOWED_HOSTS where ticket_id = ?",
            {ticket_id_str});

        if (!allowed_hosts.empty()) {
            bool host_ok = false;
            for (const auto& h : allowed_hosts) {
                if (h == _ticket_host) {
                    host_ok = true;
                    break;
                }
            }
            if (!host_ok) {
                return CAT_TICKET_HOST_EXCLUDED;
            }
        }

        // 2. User restrictions
        auto allowed_users = _exec.query_strings(
            _conn,
            "select user_name from R_TICKET_ALLOWED_USERS where ticket_id = ?",
            {ticket_id_str});

        if (!allowed_users.empty()) {
            bool user_ok = false;
            const std::string full_user = fmt::format("{}#{}", _user_name, _user_zone);
            for (const auto& u : allowed_users) {
                if (u == _user_name || u == full_user) {
                    user_ok = true;
                    break;
                }
            }
            if (!user_ok) {
                return CAT_TICKET_USER_EXCLUDED;
            }
        }

        // 3. Group restrictions
        auto allowed_groups = _exec.query_strings(
            _conn,
            "select group_name from R_TICKET_ALLOWED_GROUPS where ticket_id = ?",
            {ticket_id_str});

        if (!allowed_groups.empty()) {
            bool group_ok = false;
            for (const auto& g : allowed_groups) {
                if (check_user_in_group(_exec, _conn, _user_name, _user_zone, g) == 0) {
                    group_ok = true;
                    break;
                }
            }
            if (!group_ok) {
                return CAT_TICKET_GROUP_EXCLUDED;
            }
        }

        return 0;
    }

    auto check_object_id_by_ticket(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _data_id,
        std::string_view _access_level,
        std::string_view _ticket_str,
        std::string_view _ticket_host,
        std::string_view _user_name,
        std::string_view _user_zone) -> int
    {
        const std::string data_id_str{_data_id};
        const std::string ticket_str{_ticket_str};

        auto coll_name_opt = _exec.query_string(
            _conn,
            "select coll_name from R_COLL_MAIN "
            "where coll_id = ? or coll_id in (select coll_id from R_DATA_MAIN where data_id = ?)",
            {data_id_str, data_id_str});

        if (!coll_name_opt.has_value()) {
            return CAT_NO_ROWS_FOUND;
        }

        const bool is_modify = (_access_level.rfind("modify", 0) == 0);
        const auto zone_path = fmt::format("/{}", _user_zone);
        std::filesystem::path coll_path{coll_name_opt.value()};

        std::string ticket_id;
        int64_t uses_limit = 0;
        int64_t uses_count = 0;
        int64_t ticket_expiry = 0;
        int64_t write_file_count = 0;
        int64_t write_file_limit = 0;
        int64_t write_byte_count = 0;
        int64_t write_byte_limit = 0;
        bool found = false;

        while (coll_path != zone_path) {
            std::string sql;
            if (is_modify) {
                sql =
                    "select ticket_id, uses_limit, uses_count, ticket_expiry_ts, write_file_count, "
                    "write_file_limit, write_byte_count, write_byte_limit "
                    "from R_TICKET_MAIN where ticket_type = 'write' and ticket_string = ? and "
                    "(object_id = ? or object_id in (select coll_id from R_COLL_MAIN where coll_name = ?))";
            }
            else {
                sql =
                    "select ticket_id, uses_limit, uses_count, ticket_expiry_ts, 0, 0, 0, 0 "
                    "from R_TICKET_MAIN where ticket_string = ? and "
                    "(object_id = ? or object_id in (select coll_id from R_COLL_MAIN where coll_name = ?))";
            }

            auto res = _exec.execute_query(_conn, sql, {ticket_str, data_id_str, coll_path.string()});
            if (res.next()) {
                ticket_id = res.is_null(0) ? "" : res.get<std::string>(0);
                uses_limit = res.is_null(1) ? 0 : std::strtoll(res.get<std::string>(1).c_str(), nullptr, 10);
                uses_count = res.is_null(2) ? 0 : std::strtoll(res.get<std::string>(2).c_str(), nullptr, 10);
                ticket_expiry = res.is_null(3) ? 0 : std::strtoll(res.get<std::string>(3).c_str(), nullptr, 10);
                if (is_modify) {
                    write_file_count = res.is_null(4) ? 0 : std::strtoll(res.get<std::string>(4).c_str(), nullptr, 10);
                    write_file_limit = res.is_null(5) ? 0 : std::strtoll(res.get<std::string>(5).c_str(), nullptr, 10);
                    write_byte_count = res.is_null(6) ? 0 : std::strtoll(res.get<std::string>(6).c_str(), nullptr, 10);
                    write_byte_limit = res.is_null(7) ? 0 : std::strtoll(res.get<std::string>(7).c_str(), nullptr, 10);
                }
                found = true;
                break;
            }
            coll_path = coll_path.parent_path();
        }

        if (!found) {
            return CAT_TICKET_INVALID;
        }

        static int64_t previous_data_id_write = 0;
        static int64_t previous_data_id_uses = 0;
        static std::string previous_ticket_id;

        if (ticket_id != previous_ticket_id) {
            previous_ticket_id = ticket_id;
            previous_data_id_write = 0;
            previous_data_id_uses = 0;
        }

        if (ticket_expiry > 0 && ticket_expiry <= get_current_time_seconds()) {
            return CAT_TICKET_EXPIRED;
        }

        const auto restr_ec = check_ticket_restrictions(_exec, _conn, ticket_id, _ticket_host, _user_name, _user_zone);
        if (restr_ec != 0) {
            return restr_ec;
        }

        const int64_t int_data_id = std::strtoll(data_id_str.c_str(), nullptr, 10);

        if (is_modify) {
            if (write_byte_limit > 0 && write_byte_count > write_byte_limit) {
                return CAT_TICKET_WRITE_BYTES_EXCEEDED;
            }

            if (write_file_limit > 0) {
                if (previous_data_id_write != int_data_id) {
                    if (write_file_count >= write_file_limit) {
                        return CAT_TICKET_WRITE_USES_EXCEEDED;
                    }

                    _exec.execute_dml(
                        _conn,
                        "update R_TICKET_MAIN set write_file_count = write_file_count + 1 where ticket_id = ?",
                        {ticket_id});
                    previous_data_id_write = int_data_id;
                }
            }
        }

        if (uses_limit > 0) {
            if (previous_data_id_uses != int_data_id) {
                if (uses_count >= uses_limit) {
                    return CAT_TICKET_USES_EXCEEDED;
                }

                _exec.execute_dml(
                    _conn,
                    "update R_TICKET_MAIN set uses_count = uses_count + 1 where ticket_id = ?",
                    {ticket_id});
                previous_data_id_uses = int_data_id;
            }
        }

        return 0;
    }

    auto ticket_update_write_bytes(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _ticket_str,
        std::string_view _data_size,
        std::string_view _object_id) -> int
    {
        const auto size_bytes = std::strtoll(std::string{_data_size}.c_str(), nullptr, 10);
        if (size_bytes <= 0) {
            return 0;
        }

        const std::string obj_id_str{_object_id};
        const std::string ticket_str{_ticket_str};

        auto coll_name_opt = _exec.query_string(
            _conn,
            "select coll_name from R_COLL_MAIN where coll_id in (select coll_id from R_DATA_MAIN where data_id = ?)",
            {obj_id_str});

        if (!coll_name_opt.has_value()) {
            return CAT_NO_ROWS_FOUND;
        }

        std::filesystem::path coll_path{coll_name_opt.value()};
        std::string ticket_id;
        int64_t write_byte_limit = 0;
        bool found = false;

        while (coll_path != "/") {
            const std::string sql =
                "select ticket_id, write_byte_count, write_byte_limit from R_TICKET_MAIN "
                "where ticket_type = 'write' and ticket_string = ? and (object_id = ? or object_id in (select coll_id from R_DATA_MAIN where data_id = ?))";

            auto res = _exec.execute_query(_conn, sql, {ticket_str, obj_id_str, obj_id_str});
            if (res.next()) {
                ticket_id = res.is_null(0) ? "" : res.get<std::string>(0);
                write_byte_limit = res.is_null(2) ? 0 : std::strtoll(res.get<std::string>(2).c_str(), nullptr, 10);
                found = true;
                break;
            }
            coll_path = coll_path.parent_path();
        }

        if (!found) {
            return CAT_TICKET_INVALID;
        }

        if (write_byte_limit == 0) {
            return 0;
        }

        _exec.execute_dml(
            _conn,
            "update R_TICKET_MAIN set write_byte_count = write_byte_count + ? where ticket_id = ?",
            {std::to_string(size_bytes), ticket_id});

        return 0;
    }

    auto check_data_object_id(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _data_id,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        std::string_view _ticket_str,
        std::string_view _ticket_host) -> int
    {
        if (!_ticket_str.empty()) {
            return check_object_id_by_ticket(_exec, _conn, _data_id, _access_level, _ticket_str, _ticket_host, _user_name, _user_zone);
        }

        const std::string data_id_str{_data_id};
        const std::string user_str{_user_name};
        const std::string zone_str{_user_zone};
        const std::string access_str{_access_level};

        const std::string sql =
            "select OA.object_id "
            "from R_OBJT_ACCESS OA, R_DATA_MAIN DM, R_USER_GROUP UG, R_USER_MAIN UM, R_TOKN_MAIN TM "
            "where OA.object_id = ? and UM.user_name = ? and UM.zone_name = ? and "
                  "UM.user_type_name != 'rodsgroup' and UM.user_id = UG.user_id and "
                  "OA.object_id = DM.data_id and UG.group_user_id = OA.user_id and "
                  "OA.access_type_id >= TM.token_id and TM.token_namespace = 'access_type' and TM.token_name = ?";

        auto obj_id_opt = _exec.query_integer(_conn, sql, {data_id_str, user_str, zone_str, access_str});
        if (obj_id_opt.has_value() && obj_id_opt.value() > 0) {
            return 0;
        }
        return CAT_NO_ACCESS_PERMISSION;
    }

    auto check_resource_access(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _resc_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level) -> int64_t
    {
        const std::string resc_str{_resc_name};
        const std::string user_str{_user_name};
        const std::string zone_str{_user_zone};
        const std::string access_str{_access_level};

        const std::string sql =
            "select RM.resc_id "
            "from R_RESC_MAIN RM, R_OBJT_ACCESS OA, R_USER_GROUP UG, R_USER_MAIN UM, R_TOKN_MAIN TM "
            "where RM.resc_name = ? and UM.user_name = ? and UM.zone_name = ? and "
                  "UM.user_type_name != 'rodsgroup' and UM.user_id = UG.user_id and "
                  "OA.object_id = RM.resc_id and UG.group_user_id = OA.user_id and "
                  "OA.access_type_id >= TM.token_id and TM.token_namespace = 'access_type' and TM.token_name = ?";

        auto resc_id_opt = _exec.query_integer(_conn, sql, {resc_str, user_str, zone_str, access_str});
        if (resc_id_opt.has_value()) {
            return resc_id_opt.value();
        }

        auto exists_opt = _exec.query_integer(_conn, "select resc_id from R_RESC_MAIN where resc_name = ?", {resc_str});
        if (!exists_opt.has_value()) {
            return CAT_UNKNOWN_RESOURCE;
        }
        return CAT_NO_ACCESS_PERMISSION;
    }

    auto check_group_admin_access(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _group_name) -> int
    {
        auto user_id_opt = _exec.query_string(
            _conn,
            "select user_id from R_USER_MAIN where user_name = ? and zone_name = ? and user_type_name = 'groupadmin'",
            {std::string{_user_name}, std::string{_user_zone}});

        if (!user_id_opt.has_value()) {
            return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
        }

        if (_group_name.empty()) {
            return 0;
        }

        const std::string sql =
            "select UG.group_user_id from R_USER_GROUP UG where UG.user_id = ? and UG.group_user_id = "
            "(select user_id from R_USER_MAIN where user_type_name = 'rodsgroup' and user_name = ?)";

        auto grp_user_id_opt = _exec.query_integer(_conn, sql, {user_id_opt.value(), std::string{_group_name}});
        if (!grp_user_id_opt.has_value()) {
            return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
        }
        return 0;
    }

    auto get_group_member_count(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _group_name) -> int
    {
        const std::string sql =
            "select count(UG.user_id) from R_USER_GROUP UG "
            "where UG.group_user_id != UG.user_id and UG.group_user_id in "
            "(select user_id from R_USER_MAIN where user_name = ? and user_type_name = 'rodsgroup')";

        auto count_opt = _exec.query_integer(_conn, sql, {std::string{_group_name}});
        return count_opt.has_value() ? static_cast<int>(count_opt.value()) : 0;
    }

    auto check_name_token(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _namespace_name,
        std::string_view _token_name) -> int
    {
        auto token_opt = _exec.query_integer(
            _conn,
            "select token_id from R_TOKN_MAIN where token_namespace = ? and token_name = ?",
            {std::string{_namespace_name}, std::string{_token_name}});

        return token_opt.has_value() ? 0 : CAT_NO_ROWS_FOUND;
    }

} // namespace irods::experimental::catalog::access_control
