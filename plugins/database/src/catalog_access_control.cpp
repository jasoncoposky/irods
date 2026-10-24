#include "irods/private/catalog_access_control.hpp"

#include "irods/filesystem/path.hpp"
#include "irods/irods_logger.hpp"
#include "irods/private/genquery2_builder.hpp"
#include "irods/rodsErrorTable.h"

#include <fmt/format.h>

#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>

namespace irods::experimental::catalog::access_control
{
    namespace
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;
        using log_db = irods::experimental::log::database;

        auto get_current_time_seconds() -> int64_t
        {
            return std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        }

        auto parse_int64(std::string_view _sv) noexcept -> int64_t
        {
            while (!_sv.empty() && std::isspace(static_cast<unsigned char>(_sv.front()))) {
                _sv.remove_prefix(1);
            }
            if (!_sv.empty() && _sv.front() == '+') {
                _sv.remove_prefix(1);
            }
            int64_t val{};
            if (!_sv.empty()) {
                std::from_chars(_sv.data(), _sv.data() + _sv.size(), val);
            }
            return val;
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
        if (_admin_mode) {
            auto coll_id_opt = query_catalog_integer(
                _exec,
                _conn,
                gq2::builder::select({"coll_id"})
                    .from("COLLECTION")
                    .where(col("coll_name") == _dir_name)
                    .build());

            return coll_id_opt.has_value() ? coll_id_opt.value() : CAT_UNKNOWN_COLLECTION;
        }

        const std::string dir_str{_dir_name};
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

        auto exists_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == _dir_name)
                .build());

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
            auto res = execute_catalog(
                _exec,
                _conn,
                gq2::builder::select({"COLL_ID", "COLL_INHERITANCE"})
                    .where(col("COLL_NAME") == _dir_name &&
                           col("TICKET_STRING") == _ticket_str &&
                           col("TICKET_TYPE") == "write")
                    .build());
            if (res.query_result && res.query_result->next()) {
                coll_id = res.query_result->get<int64_t>(0);
                if (_inherit_flag && !res.query_result->is_null(1)) {
                    *_inherit_flag = (res.query_result->get<std::string>(1) == "1") ? 1 : 0;
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
            auto exists_opt = query_catalog_integer(
                _exec,
                _conn,
                gq2::builder::select({"coll_id"})
                    .from("COLLECTION")
                    .where(col("coll_name") == _dir_name)
                    .build());

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

        auto exists_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_id") == _dir_id)
                .build());

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
        auto coll_id_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == _dir_name &&
                       col("coll_owner_name") == _user_name &&
                       col("coll_owner_zone") == _user_zone)
                .build());

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
        if (_admin_mode) {
            auto data_id_opt = query_catalog_integer(
                _exec,
                _conn,
                gq2::builder::select({"DATA_ID"})
                    .where(col("DATA_NAME") == _data_name && col("COLL_NAME") == _dir_name)
                    .build());
            return data_id_opt.has_value() ? data_id_opt.value() : CAT_UNKNOWN_FILE;
        }

        const std::string dir_str{_dir_name};
        const std::string data_str{_data_name};
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

        auto exists_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"DATA_ID"})
                .where(col("DATA_NAME") == _data_name && col("COLL_NAME") == _dir_name)
                .build());
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
        auto coll_id_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"coll_id"})
                .from("COLLECTION")
                .where(col("coll_name") == _dir_name)
                .build());

        if (!coll_id_opt.has_value()) {
            return CAT_UNKNOWN_COLLECTION;
        }

        auto data_id_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"data_id"})
                .from("DATA_OBJECT")
                .where(col("data_name") == _data_name &&
                       col("coll_id") == std::to_string(coll_id_opt.value()) &&
                       col("data_owner_name") == _user_name &&
                       col("data_owner_zone") == _user_zone)
                .build());

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
        auto user_id_opt = query_catalog_string(
            _exec,
            _conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _user_name &&
                       col("zone_name") == _user_zone &&
                       col("user_type_name") != "rodsgroup")
                .build());

        if (!user_id_opt.has_value()) {
            return CAT_INVALID_USER;
        }

        auto group_id_opt = query_catalog_string(
            _exec,
            _conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_type_name") == "rodsgroup" && col("user_name") == _group_name)
                .build());

        if (!group_id_opt.has_value()) {
            return CAT_NO_ROWS_FOUND;
        }

        auto grp_user_id_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"group_user_id"})
                .from("USER_GROUP")
                .where(col("user_id") == user_id_opt.value() && col("group_user_id") == group_id_opt.value())
                .build());
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
        // 1. Host restrictions
        auto allowed_hosts = query_catalog_strings(
            _exec,
            _conn,
            gq2::builder::select({"host"})
                .from("TICKET_ALLOWED_HOSTS")
                .where(col("ticket_id") == _ticket_id)
                .build());

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
        auto allowed_users = query_catalog_strings(
            _exec,
            _conn,
            gq2::builder::select({"user_name"})
                .from("TICKET_ALLOWED_USERS")
                .where(col("ticket_id") == _ticket_id)
                .build());

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
        auto allowed_groups = query_catalog_strings(
            _exec,
            _conn,
            gq2::builder::select({"group_name"})
                .from("TICKET_ALLOWED_GROUPS")
                .where(col("ticket_id") == _ticket_id)
                .build());

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

        auto coll_name_opt = query_catalog_string(
            _exec,
            _conn,
            gq2::builder::select({"coll_name"})
                .from("COLLECTION")
                .where(col("coll_id") == _data_id)
                .build());
        if (!coll_name_opt.has_value()) {
            coll_name_opt = query_catalog_string(
                _exec,
                _conn,
                gq2::builder::select({"COLL_NAME"})
                    .where(col("DATA_ID") == _data_id)
                    .build());
        }

        if (!coll_name_opt.has_value()) {
            return CAT_NO_ROWS_FOUND;
        }

        const bool is_modify = (_access_level.rfind("modify", 0) == 0);
        const auto zone_path = fmt::format("/{}", _user_zone);
        irods::filesystem::path coll_path{coll_name_opt.value()};

        std::string ticket_id;
        int64_t uses_limit = 0;
        int64_t uses_count = 0;
        int64_t ticket_expiry = 0;
        int64_t write_file_count = 0;
        int64_t write_file_limit = 0;
        int64_t write_byte_count = 0;
        int64_t write_byte_limit = 0;
        bool found = false;

        while (coll_path.string_view() != zone_path && !coll_path.empty() && coll_path.string_view() != "/") {
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
                uses_limit = res.is_null(1) ? 0 : parse_int64(res.get<std::string>(1));
                uses_count = res.is_null(2) ? 0 : parse_int64(res.get<std::string>(2));
                ticket_expiry = res.is_null(3) ? 0 : parse_int64(res.get<std::string>(3));
                if (is_modify) {
                    write_file_count = res.is_null(4) ? 0 : parse_int64(res.get<std::string>(4));
                    write_file_limit = res.is_null(5) ? 0 : parse_int64(res.get<std::string>(5));
                    write_byte_count = res.is_null(6) ? 0 : parse_int64(res.get<std::string>(6));
                    write_byte_limit = res.is_null(7) ? 0 : parse_int64(res.get<std::string>(7));
                }
                found = true;
                break;
            }
            const auto parent_view = coll_path.parent_path_view();
            if (parent_view == coll_path.string_view()) {
                break;
            }
            coll_path = parent_view;
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

        const int64_t int_data_id = parse_int64(data_id_str);

        if (is_modify) {
            if (write_byte_limit > 0 && write_byte_count > write_byte_limit) {
                return CAT_TICKET_WRITE_BYTES_EXCEEDED;
            }

            if (write_file_limit > 0) {
                if (previous_data_id_write != int_data_id) {
                    if (write_file_count >= write_file_limit) {
                        return CAT_TICKET_WRITE_USES_EXCEEDED;
                    }

                    execute_catalog(
                        _exec,
                        _conn,
                        gq2::builder::update("TICKET")
                            .set("write_file_count", std::to_string(write_file_count + 1))
                            .where(col("ticket_id") == ticket_id)
                            .build());
                    previous_data_id_write = int_data_id;
                }
            }
        }

        if (uses_limit > 0) {
            if (previous_data_id_uses != int_data_id) {
                if (uses_count >= uses_limit) {
                    return CAT_TICKET_USES_EXCEEDED;
                }

                execute_catalog(
                    _exec,
                    _conn,
                    gq2::builder::update("TICKET")
                        .set("uses_count", std::to_string(uses_count + 1))
                        .where(col("ticket_id") == ticket_id)
                        .build());
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
        const auto size_bytes = parse_int64(_data_size);
        if (size_bytes <= 0) {
            return 0;
        }

        const std::string obj_id_str{_object_id};
        const std::string ticket_str{_ticket_str};

        auto coll_name_opt = query_catalog_string(
            _exec,
            _conn,
            gq2::builder::select({"COLL_NAME"})
                .where(col("DATA_ID") == _object_id)
                .build());

        if (!coll_name_opt.has_value()) {
            return CAT_NO_ROWS_FOUND;
        }

        irods::filesystem::path coll_path{coll_name_opt.value()};
        std::string ticket_id;
        int64_t write_byte_count = 0;
        int64_t write_byte_limit = 0;
        bool found = false;

        while (coll_path.string_view() != "/" && !coll_path.empty()) {
            const std::string sql =
                "select ticket_id, write_byte_count, write_byte_limit from R_TICKET_MAIN "
                "where ticket_type = 'write' and ticket_string = ? and (object_id = ? or object_id in (select coll_id from R_DATA_MAIN where data_id = ?))";

            auto res = _exec.execute_query(_conn, sql, {ticket_str, obj_id_str, obj_id_str});
            if (res.next()) {
                ticket_id = res.is_null(0) ? "" : res.get<std::string>(0);
                write_byte_count = res.is_null(1) ? 0 : parse_int64(res.get<std::string>(1));
                write_byte_limit = res.is_null(2) ? 0 : parse_int64(res.get<std::string>(2));
                found = true;
                break;
            }
            const auto parent_view = coll_path.parent_path_view();
            if (parent_view == coll_path.string_view()) {
                break;
            }
            coll_path = parent_view;
        }

        if (!found) {
            return CAT_TICKET_INVALID;
        }

        if (write_byte_limit == 0) {
            return 0;
        }

        execute_catalog(
            _exec,
            _conn,
            gq2::builder::update("TICKET")
                .set("write_byte_count", std::to_string(write_byte_count + size_bytes))
                .where(col("ticket_id") == ticket_id)
                .build());

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

        auto exists_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"resc_id"})
                .from("RESOURCE")
                .where(col("resc_name") == _resc_name)
                .build());
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
        auto user_id_opt = query_catalog_string(
            _exec,
            _conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _user_name &&
                       col("zone_name") == _user_zone &&
                       col("user_type_name") == "groupadmin")
                .build());

        if (!user_id_opt.has_value()) {
            return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
        }

        if (_group_name.empty()) {
            return 0;
        }

        auto group_id_opt = query_catalog_string(
            _exec,
            _conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_type_name") == "rodsgroup" && col("user_name") == _group_name)
                .build());

        if (!group_id_opt.has_value()) {
            return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
        }

        auto grp_user_id_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"group_user_id"})
                .from("USER_GROUP")
                .where(col("user_id") == user_id_opt.value() && col("group_user_id") == group_id_opt.value())
                .build());
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
        auto group_id_opt = query_catalog_string(
            _exec,
            _conn,
            gq2::builder::select({"user_id"})
                .from("USER")
                .where(col("user_name") == _group_name && col("user_type_name") == "rodsgroup")
                .build());
        if (!group_id_opt.has_value()) {
            return 0;
        }

        auto count_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({gq2::builder::count("user_id")})
                .from("USER_GROUP")
                .where(col("group_user_id") == group_id_opt.value() && col("user_id") != group_id_opt.value())
                .build());
        return count_opt.has_value() ? static_cast<int>(count_opt.value()) : 0;
    }

    auto check_name_token(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _namespace_name,
        std::string_view _token_name) -> int
    {
        auto token_opt = query_catalog_integer(
            _exec,
            _conn,
            gq2::builder::select({"token_id"})
                .from("TOKEN")
                .where(col("token_namespace") == _namespace_name && col("token_name") == _token_name)
                .build());

        return token_opt.has_value() ? 0 : CAT_NO_ROWS_FOUND;
    }

} // namespace irods::experimental::catalog::access_control
