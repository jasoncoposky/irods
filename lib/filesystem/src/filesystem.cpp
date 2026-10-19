#include "irods/filesystem/filesystem.hpp"

#include "irods/filesystem/path.hpp"
#include "irods/filesystem/path_utilities.hpp"
#include "irods/filesystem/collection_iterator.hpp"

// clang-format off
#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#  include "irods/rods.h"
#  include "irods/apiHeaderAll.h"
#  include "irods/rs_genquery2.hpp"
#  include "irods/rsObjStat.hpp"
#  include "irods/rsDataObjCopy.hpp"
#  include "irods/rsDataObjRename.hpp"
#  include "irods/rsDataObjUnlink.hpp"
#  include "irods/rsDataObjChksum.hpp"
#  include "irods/rsModAccessControl.hpp"
#  include "irods/rsCollCreate.hpp"
#  include "irods/rsModColl.hpp"
#  include "irods/rsRmColl.hpp"
#  include "irods/rsModAVUMetadata.hpp"
#  include "irods/rsModDataObjMeta.hpp"
#else
#  include "irods/rodsClient.h"
#  include "irods/genquery2.h"
#  include "irods/objStat.h"
#  include "irods/dataObjCopy.h"
#  include "irods/dataObjRename.h"
#  include "irods/dataObjUnlink.h"
#  include "irods/dataObjChksum.h"
#  include "irods/modAccessControl.h"
#  include "irods/collCreate.h"
#  include "irods/modColl.h"
#  include "irods/rmColl.h"
#  include "irods/modAVUMetadata.h"
#  include "irods/data_object_modify_info.h"
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
// clang-format on

#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/irods_log.hpp"
#include "irods/irods_error.hpp"
#include "irods/irods_exception.hpp"
#include "irods/irods_at_scope_exit.hpp"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <charconv>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <iterator>
#include <exception>
#include <algorithm>
#include <system_error>
#include <utility>
#include <vector>

namespace irods::experimental::filesystem::detail
{
#ifndef IRODS_FILESYSTEM_SQL_ESCAPED_VIEW_DEFINED
#define IRODS_FILESYSTEM_SQL_ESCAPED_VIEW_DEFINED
    struct sql_escaped_view
    {
        std::string_view value;
    };
#endif // IRODS_FILESYSTEM_SQL_ESCAPED_VIEW_DEFINED
} // namespace irods::experimental::filesystem::detail

#ifndef IRODS_FILESYSTEM_SQL_ESCAPED_FORMATTER_DEFINED
#define IRODS_FILESYSTEM_SQL_ESCAPED_FORMATTER_DEFINED
template <>
struct fmt::formatter<irods::experimental::filesystem::detail::sql_escaped_view> : fmt::formatter<std::string_view>
{
    template <typename FormatContext>
    auto format(irods::experimental::filesystem::detail::sql_escaped_view _esc, FormatContext& _ctx) const
    {
        auto out = _ctx.out();
        std::string_view sv = _esc.value;

        while (!sv.empty()) {
            const auto pos = sv.find_first_of("'\\");
            if (pos == std::string_view::npos) {
                out = fmt::format_to(out, "{}", sv);
                break;
            }
            if (pos > 0) {
                out = fmt::format_to(out, "{}", sv.substr(0, pos));
            }
            if (sv[pos] == '\'') {
                out = fmt::format_to(out, "''");
            }
            else {
                out = fmt::format_to(out, "\\x5c");
            }
            sv.remove_prefix(pos + 1);
        }

        return out;
    }
};
#endif // IRODS_FILESYSTEM_SQL_ESCAPED_FORMATTER_DEFINED

namespace irods::experimental::filesystem::NAMESPACE_IMPL
{
    namespace
    {
        using ::irods::experimental::filesystem::detail::sql_escaped_view;

        constexpr auto sql_escape(std::string_view _sv) noexcept -> sql_escaped_view
        {
            return sql_escaped_view{_sv};
        }

        constexpr auto sql_escape(const std::string& _s) noexcept -> sql_escaped_view
        {
            return sql_escaped_view{_s};
        }

        constexpr auto sql_escape(const path& _p) noexcept -> sql_escaped_view
        {
            return sql_escaped_view{_p.string_view()};
        }

        template <typename IntType>
        auto parse_int(std::string_view _sv, IntType& _out) noexcept -> bool
        {
            if (_sv.empty()) {
                return false;
            }
            const auto* first = _sv.data();
            const auto* last = first + _sv.size();
            const auto [ptr, ec] = std::from_chars(first, last, _out);
            return ec == std::errc{} && ptr == last;
        }

        auto execute_genquery2(rxComm& _comm,
                               const path& _p,
                               std::string& _query_string,
                               nlohmann::json& _rows,
                               std::error_code& _ec) noexcept -> bool
        {
            _ec.clear();

            Genquery2Input input{};
            input.query_string = _query_string.data();

            char zone_buf[NAME_LEN]{};
            if (const auto zv = zone_name_view(_p); zv && !zv->empty() && zv->size() < NAME_LEN) {
                std::memcpy(zone_buf, zv->data(), zv->size());
                zone_buf[zv->size()] = '\0';
                input.zone = zone_buf;
            }

            char* output{};
            irods::at_scope_exit free_output{[&output] {
                if (output) {
                    std::free(output);
                }
            }};

            if (const auto ec = rxGenQuery2(&_comm, &input, &output); ec < 0) {
                _ec = make_error_code(ec);
                return false;
            }

            if (!output) {
                _ec = make_error_code(SYS_INTERNAL_ERR);
                return false;
            }

            _rows = nlohmann::json::parse(output, nullptr, false);
            if (_rows.is_discarded() || !_rows.is_array()) {
                _ec = make_error_code(SYS_INTERNAL_ERR);
                return false;
            }

            return true;
        }

        template <typename RowCallback>
        auto execute_genquery2_paged(rxComm& _comm,
                                     const path& _p,
                                     std::string_view _base_query,
                                     std::error_code& _ec,
                                     RowCallback&& _on_row) -> bool
        {
            constexpr std::size_t page_size = 256;
            std::size_t offset = 0;

            while (true) {
                std::string paged_query = (offset == 0)
                    ? fmt::format("{} fetch first {} rows only", _base_query, page_size)
                    : fmt::format("{} offset {} fetch first {} rows only", _base_query, offset, page_size);

                nlohmann::json rows;
                if (!execute_genquery2(_comm, _p, paged_query, rows, _ec)) {
                    return false;
                }

                for (const auto& row : rows) {
                    if (!row.is_array()) {
                        _ec = make_error_code(SYS_INTERNAL_ERR);
                        return false;
                    }
                    if (!_on_row(row)) {
                        return false;
                    }
                }

                if (rows.size() < page_size) {
                    break;
                }

                offset += rows.size();
            }

            return true;
        }

#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
        int rsDataObjCopy(rsComm_t* _comm, dataObjCopyInp_t* _dataObjCopyInp)
        {
            transferStat_t* ts_ptr{};
            irods::at_scope_exit free_ts{[&ts_ptr] { std::free(ts_ptr); }};
            return ::rsDataObjCopy(_comm, _dataObjCopyInp, &ts_ptr);
        }

        const auto rsRmColl = [](rsComm_t* _comm, collInp_t* _rmCollInp, bool _track_progress) -> int
        {
            collOprStat_t* stat{};
            irods::at_scope_exit free_stat{[&stat] { std::free(stat); }};
            return ::rsRmColl(_comm, _rmCollInp, _track_progress ? &stat : nullptr);
        };
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

        struct basic_stat_result
        {
            int error{0};
            long long id{-1};
            long long size{0};
            int type{UNKNOWN_OBJ_T};
            long long ctime{0};
            long long mtime{0};
            bool is_special_coll{false};
        };

        auto basic_stat(rxComm& _comm, const path& _p, std::error_code& _ec) noexcept -> basic_stat_result
        {
            basic_stat_result s{};

            if (!validate_path(_p, _ec)) {
                s.error = _ec.value();
                return s;
            }

            dataObjInp_t input{};
            if (!copy_to_c_array(input.objPath, _p.string_view(), _ec)) {
                s.error = _ec.value();
                return s;
            }

            rodsObjStat_t* output{};
            s.error = rxObjStat(&_comm, &input, &output);

            if (s.error >= 0) {
                irods::at_scope_exit free_stat_out{[output] {
                    freeRodsObjStat(output);
                }};

                if (!output) {
                    s.error = SYS_INTERNAL_ERR;
                    _ec = make_error_code(SYS_INTERNAL_ERR);
                    return s;
                }

                if (!parse_int(output->dataId, s.id) ||
                    !parse_int(output->createTime, s.ctime) ||
                    !parse_int(output->modifyTime, s.mtime))
                {
                    s.error = SYS_INTERNAL_ERR;
                    _ec = make_error_code(SYS_INTERNAL_ERR);
                    return s;
                }

                s.size = output->objSize;
                s.type = output->objType;
                s.is_special_coll = (output->specColl != nullptr);
                _ec.clear();
            }
            else if (USER_FILE_DOES_NOT_EXIST == s.error) {
                s.error = 0;
                s.type = UNKNOWN_OBJ_T;
                _ec.clear();
            }
            else {
                _ec = make_error_code(s.error);
            }

            return s;
        }

        constexpr auto to_object_type(int _rods_obj_type) noexcept -> object_type
        {
            switch (_rods_obj_type) {
                case DATA_OBJ_T:
                    return object_type::data_object;
                case COLL_OBJ_T:
                    return object_type::collection;
                case UNKNOWN_OBJ_T:
                    return object_type::not_found;
                default:
                    return object_type::none;
            }
        }

        auto to_permission_enum(std::string_view _perm, std::error_code& _ec) noexcept -> perms
        {
            // clang-format off
            if (_perm == "null")            { return perms::null; }
            if (_perm == "read_metadata")   { return perms::read_metadata; }
            if (_perm == "read_object")     { return perms::read; }
            if (_perm == "read object")     { return perms::read; }
            if (_perm == "read")            { return perms::read; }
            if (_perm == "create_metadata") { return perms::create_metadata; }
            if (_perm == "modify_metadata") { return perms::modify_metadata; }
            if (_perm == "delete_metadata") { return perms::delete_metadata; }
            if (_perm == "create_object")   { return perms::create_object; }
            if (_perm == "modify_object")   { return perms::write; }
            if (_perm == "modify object")   { return perms::write; }
            if (_perm == "write")           { return perms::write; }
            if (_perm == "delete_object")   { return perms::delete_object; }
            if (_perm == "own")             { return perms::own; }
            // clang-format on

            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return perms::null;
        }

        auto fetch_permissions(rxComm& _comm,
                               const path& _p,
                               const basic_stat_result& _s,
                               std::vector<entity_permission>& _out_prms,
                               std::error_code& _ec) -> void
        {
            _ec.clear();

            std::string acl_query;
            if (DATA_OBJ_T == _s.type) {
                acl_query = fmt::format(
                    "select DATA_ACCESS_USER_ID, DATA_ACCESS_PERM_NAME "
                    "where DATA_ID = '{}' and DATA_ACCESS_USER_ID is not null",
                    _s.id);
            }
            else if (COLL_OBJ_T == _s.type) {
                acl_query = fmt::format(
                    "select COLL_ACCESS_USER_ID, COLL_ACCESS_PERM_NAME "
                    "where COLL_ID = '{}' and COLL_ACCESS_USER_ID is not null",
                    _s.id);
            }
            else {
                return;
            }

            struct raw_acl_entry
            {
                std::string user_id;
                perms prms;
            };
            std::vector<raw_acl_entry> raw_acls;

            if (!execute_genquery2_paged(_comm, _p, acl_query, _ec, [&](const nlohmann::json& _row) {
                    if (_row.size() < 2 || !_row[0].is_string() || !_row[1].is_string()) {
                        return true;
                    }
                    const auto& user_id = _row[0].get_ref<const std::string&>();
                    const auto& perm_str = _row[1].get_ref<const std::string&>();
                    if (user_id.empty()) {
                        return true;
                    }
                    const auto p = to_permission_enum(perm_str, _ec);
                    if (_ec) {
                        return false;
                    }
                    raw_acls.push_back({user_id, p});
                    return true;
                }))
            {
                return;
            }

            if (raw_acls.empty()) {
                return;
            }

            std::string user_in_clause;
            for (std::size_t i = 0; i < raw_acls.size(); ++i) {
                if (i > 0) {
                    user_in_clause += ", ";
                }
                fmt::format_to(std::back_inserter(user_in_clause), "'{}'", sql_escape(raw_acls[i].user_id));
            }

            const std::string user_query = fmt::format(
                "select USER_ID, USER_NAME, USER_ZONE, USER_TYPE where USER_ID in ({})",
                user_in_clause);

            struct user_details
            {
                std::string id;
                std::string name;
                std::string zone;
                std::string type;
            };
            std::vector<user_details> users;
            users.reserve(raw_acls.size());

            if (!execute_genquery2_paged(_comm, _p, user_query, _ec, [&](const nlohmann::json& _row) {
                    if (_row.size() < 4 || !_row[0].is_string() || !_row[1].is_string() ||
                        !_row[2].is_string() || !_row[3].is_string())
                    {
                        _ec = make_error_code(SYS_INTERNAL_ERR);
                        return false;
                    }
                    users.push_back({
                        _row[0].get<std::string>(),
                        _row[1].get<std::string>(),
                        _row[2].get<std::string>(),
                        _row[3].get<std::string>()
                    });
                    return true;
                }))
            {
                return;
            }

            _out_prms.reserve(_out_prms.size() + raw_acls.size());
            for (const auto& acl : raw_acls) {
                const auto it = std::find_if(users.begin(), users.end(), [&](const user_details& _u) {
                    return _u.id == acl.user_id;
                });
                if (it != users.end()) {
                    _out_prms.push_back({it->name, it->zone, acl.prms, it->type});
                }
            }
        }

        auto get_inheritance(rxComm& _comm, const path& _p, int _object_type, std::error_code& _ec) -> bool
        {
            _ec.clear();

            if (COLL_OBJ_T != _object_type) {
                return false;
            }

            std::string gql = fmt::format(
                "select COLL_INHERITANCE where COLL_NAME = '{}' fetch first 1 rows only",
                sql_escape(_p));

            nlohmann::json rows;
            if (!execute_genquery2(_comm, _p, gql, rows, _ec)) {
                return false;
            }

            if (!rows.empty() && rows[0].is_array() && !rows[0].empty() && rows[0][0].is_string()) {
                const auto& val_str = rows[0][0].get_ref<const std::string&>();
                int val = 0;
                if (parse_int(val_str, val)) {
                    return val != 0;
                }
                return std::atoi(val_str.c_str()) != 0;
            }

            return false;
        }

        auto is_collection_empty(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
        {
            _ec.clear();
            return collection_iterator{_comm, _p, _ec} == std::default_sentinel;
        }

        auto remove_by_type_impl(rxComm& _comm,
                                 const path& _p,
                                 int _obj_type,
                                 extended_remove_options _opts,
                                 std::error_code& _ec) -> bool
        {
            _ec.clear();

            if (UNKNOWN_OBJ_T == _obj_type) {
                return false;
            }

            if (DATA_OBJ_T == _obj_type) {
                dataObjInp_t input{};
                at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};

                input.oprType = _opts.unregister ? UNREG_OPR : 0;

                if (!copy_to_c_array(input.objPath, _p.string_view(), _ec)) {
                    return false;
                }

                if (_opts.no_trash) {
                    addKeyVal(&input.condInput, FORCE_FLAG_KW, "");
                }

                if (const auto err = rxDataObjUnlink(&_comm, &input); err < 0) {
                    _ec = make_error_code(err);
                    return false;
                }

                return true;
            }

            if (COLL_OBJ_T == _obj_type) {
                if (!_opts.recursive) {
                    const bool empty = is_collection_empty(_comm, _p, _ec);
                    if (_ec) {
                        return false;
                    }
                    if (!empty) {
                        _ec = make_error_code(SYS_COLLECTION_NOT_EMPTY);
                        return false;
                    }
                }

                collInp_t input{};
                at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};

                if (!copy_to_c_array(input.collName, _p.string_view(), _ec)) {
                    return false;
                }

                if (_opts.no_trash) {
                    addKeyVal(&input.condInput, FORCE_FLAG_KW, "");
                }

                if (_opts.recursive) {
                    addKeyVal(&input.condInput, RECURSIVE_OPR__KW, "");
                }

                if (const auto err = rxRmColl(&_comm, &input, _opts.progress); err < 0) {
                    _ec = make_error_code(err);
                    return false;
                }

                return true;
            }

            _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
            return false;
        }

        auto remove_impl(rxComm& _comm, const path& _p, extended_remove_options _opts, std::error_code& _ec) -> bool
        {
            const auto st = basic_stat(_comm, _p, _ec);
            if (_ec) {
                return false;
            }
            return remove_by_type_impl(_comm, _p, st.type, _opts, _ec);
        }

        auto remove_all_impl(rxComm& _comm,
                             const path& _p,
                             extended_remove_options _opts,
                             std::error_code& _ec) -> std::uintmax_t
        {
            const auto st = basic_stat(_comm, _p, _ec);
            if (_ec) {
                return static_cast<std::uintmax_t>(-1);
            }

            if (UNKNOWN_OBJ_T == st.type) {
                return 0;
            }

            if (DATA_OBJ_T == st.type) {
                _opts.recursive = false;
                if (remove_by_type_impl(_comm, _p, st.type, _opts, _ec)) {
                    return 1;
                }
                return _ec ? static_cast<std::uintmax_t>(-1) : 0;
            }

            if (COLL_OBJ_T != st.type) {
                _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
                return static_cast<std::uintmax_t>(-1);
            }

            // Normalize trailing slashes for exact collection + descendant prefix queries.
            std::string_view p_view = _p.string_view();
            while (p_view.size() > 1 && is_separator(p_view.back())) {
                p_view.remove_suffix(1);
            }

            std::uintmax_t count = 1; // The collection _p itself.

            std::string data_sql = (p_view == "/")
                ? std::string{"select count(distinct DATA_ID) where COLL_NAME = '/' or COLL_NAME like '/%'"}
                : fmt::format("select count(distinct DATA_ID) where COLL_NAME = '{}' or COLL_NAME like '{}/%'",
                              sql_escape(p_view),
                              sql_escape(p_view));

            std::string sub_colls_sql = (p_view == "/")
                ? std::string{"select count(COLL_ID) where COLL_NAME != '/' and COLL_NAME like '/%'"}
                : fmt::format("select count(COLL_ID) where COLL_NAME like '{}/%'", sql_escape(p_view));

            for (auto* sql_ptr : {&data_sql, &sub_colls_sql}) {
                nlohmann::json rows;
                if (!execute_genquery2(_comm, _p, *sql_ptr, rows, _ec)) {
                    return static_cast<std::uintmax_t>(-1);
                }

                for (const auto& row : rows) {
                    if (row.is_array() && !row.empty() && row[0].is_string()) {
                        std::uint64_t sub_count = 0;
                        if (parse_int(row[0].get_ref<const std::string&>(), sub_count)) {
                            count += sub_count;
                        }
                    }
                }
            }

            _opts.recursive = true;

            if (remove_by_type_impl(_comm, _p, st.type, _opts, _ec)) {
                return count;
            }

            return _ec ? static_cast<std::uintmax_t>(-1) : 0;
        }

        constexpr auto permission_to_access_string(bool _add_admin_flag, perms _prms) noexcept -> const char*
        {
            if (_add_admin_flag) {
                switch (_prms) {
                    case perms::null:            return "admin:null";
                    case perms::read_metadata:   return "admin:read_metadata";
                    case perms::read_object:
                    case perms::read:            return "admin:read";
                    case perms::create_metadata: return "admin:create_metadata";
                    case perms::modify_metadata: return "admin:modify_metadata";
                    case perms::delete_metadata: return "admin:delete_metadata";
                    case perms::create_object:   return "admin:create_object";
                    case perms::modify_object:
                    case perms::write:           return "admin:write";
                    case perms::delete_object:   return "admin:delete_object";
                    case perms::own:             return "admin:own";
                }
            }
            else {
                switch (_prms) {
                    case perms::null:            return "null";
                    case perms::read_metadata:   return "read_metadata";
                    case perms::read_object:
                    case perms::read:            return "read";
                    case perms::create_metadata: return "create_metadata";
                    case perms::modify_metadata: return "modify_metadata";
                    case perms::delete_metadata: return "delete_metadata";
                    case perms::create_object:   return "create_object";
                    case perms::modify_object:
                    case perms::write:           return "write";
                    case perms::delete_object:   return "delete_object";
                    case perms::own:             return "own";
                }
            }
            return nullptr;
        }

        auto set_permissions_impl(bool _add_admin_flag,
                                  rxComm& _comm,
                                  const path& _p,
                                  const std::string& _user_or_group,
                                  perms _prms,
                                  std::error_code& _ec) noexcept -> void
        {
            if (!validate_path_length(_p.string_view(), _ec)) {
                return;
            }

            const char* access_str = permission_to_access_string(_add_admin_flag, _prms);
            if (!access_str) {
                _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
                return;
            }

            char username[NAME_LEN]{};
            char zone[NAME_LEN]{};

            if (const auto err = parseUserName(_user_or_group.c_str(), username, zone); err != 0) {
                _ec = make_error_code(err);
                return;
            }

            modAccessControlInp_t input{};
            input.userName = username;
            input.zone = zone;
            input.path = const_cast<char*>(_p.c_str());
            input.accessLevel = const_cast<char*>(access_str);

            if (const auto err = rxModAccessControl(&_comm, &input); err != 0) {
                _ec = make_error_code(err);
            }
        }

        auto set_inheritance_impl(bool _add_admin_flag,
                                  rxComm& _comm,
                                  const path& _p,
                                  bool _value,
                                  std::error_code& _ec) noexcept -> void
        {
            const auto st = basic_stat(_comm, _p, _ec);
            if (_ec) {
                return;
            }

            if (COLL_OBJ_T != st.type) {
                _ec = make_error_code(NOT_A_COLLECTION);
                return;
            }

            modAccessControlInp_t input{};
            input.userName = const_cast<char*>("");
            input.zone = const_cast<char*>("");
            input.path = const_cast<char*>(_p.c_str());
            input.accessLevel = const_cast<char*>(
                _add_admin_flag ? (_value ? "admin:inherit" : "admin:noinherit")
                                : (_value ? "inherit" : "noinherit"));

            if (const auto err = rxModAccessControl(&_comm, &input); err != 0) {
                _ec = make_error_code(err);
            }
        }

        auto has_prefix(const path& _p, const path& _prefix) -> bool
        {
            if (_p == _prefix) {
                return false;
            }

            auto p_iter = std::begin(_prefix);
            auto p_last = std::end(_prefix);
            auto c_iter = std::begin(_p);
            auto c_last = std::end(_p);

            for (; p_iter != p_last && c_iter != c_last && *p_iter == *c_iter; ++p_iter, ++c_iter);

            return (p_iter == p_last);
        }

        auto do_metadata_op_impl(bool _add_admin_flag,
                                 rxComm& _comm,
                                 const path& _p,
                                 const metadata& _metadata,
                                 const char* _op,
                                 std::error_code& _ec) noexcept -> void
        {
            const auto st = basic_stat(_comm, _p, _ec);
            if (_ec) {
                return;
            }

            const char* type_arg = nullptr;
            if (DATA_OBJ_T == st.type) {
                type_arg = "-d";
            }
            else if (COLL_OBJ_T == st.type) {
                type_arg = "-C";
            }
            else {
                _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
                return;
            }

            if (!validate_path_length(_metadata.attribute, _ec) ||
                !validate_path_length(_metadata.value, _ec) ||
                !validate_path_length(_metadata.units, _ec))
            {
                return;
            }

            modAVUMetadataInp_t input{};
            input.arg0 = const_cast<char*>(_op);
            input.arg1 = const_cast<char*>(type_arg);
            input.arg2 = const_cast<char*>(_p.c_str());
            input.arg3 = const_cast<char*>(_metadata.attribute.c_str());
            input.arg4 = const_cast<char*>(_metadata.value.c_str());
            input.arg5 = const_cast<char*>(_metadata.units.c_str());

            at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};

            if (_add_admin_flag) {
                addKeyVal(&input.condInput, ADMIN_KW, "");
            }

            if (const auto err = rxModAVUMetadata(&_comm, &input); err != 0) {
                _ec = make_error_code(err);
            }
        }

        auto last_write_time_by_type(rxComm& _comm,
                                     const path& _p,
                                     int _obj_type,
                                     std::error_code& _ec) -> object_time_type
        {
            _ec.clear();

            std::string gql;
            if (DATA_OBJ_T == _obj_type) {
                gql = fmt::format("select max(DATA_MODIFY_TIME) "
                                  "where"
                                  " COLL_NAME = '{}' and"
                                  " DATA_NAME = '{}' and"
                                  " DATA_REPL_STATUS = '1'",
                                  sql_escape(_p.parent_path_view()),
                                  sql_escape(_p.object_name_view()));
            }
            else if (COLL_OBJ_T == _obj_type) {
                gql = fmt::format("select COLL_MODIFY_TIME where COLL_NAME = '{}' fetch first 1 rows only",
                                  sql_escape(_p));
            }
            else {
                _ec = make_error_code(INVALID_OBJECT_TYPE);
                return object_time_type::min();
            }

            nlohmann::json rows;
            if (!execute_genquery2(_comm, _p, gql, rows, _ec)) {
                return object_time_type::min();
            }

            if (!rows.empty() && rows[0].is_array() && !rows[0].empty() && rows[0][0].is_string()) {
                std::uint64_t secs = 0;
                if (parse_int(rows[0][0].get_ref<const std::string&>(), secs)) {
                    return object_time_type{std::chrono::seconds{secs}};
                }
            }

            _ec = make_error_code(CAT_NO_ROWS_FOUND);
            return object_time_type::min();
        }

        auto copy_data_object_with_stats(rxComm& _comm,
                                         const path& _from,
                                         const basic_stat_result& _from_stat,
                                         const path& _to,
                                         const basic_stat_result& _to_stat,
                                         copy_options _options,
                                         std::error_code& _ec) -> bool
        {
            _ec.clear();

            if (DATA_OBJ_T != _from_stat.type) {
                _ec = make_error_code(INVALID_OBJECT_TYPE);
                return false;
            }

            dataObjCopyInp_t input{};
            at_scope_exit free_memory{[&input] {
                clearKeyVal(&input.srcDataObjInp.condInput);
                clearKeyVal(&input.destDataObjInp.condInput);
            }};

            if (UNKNOWN_OBJ_T != _to_stat.type) {
                if (_from_stat.id == _to_stat.id) {
                    _ec = make_error_code(SAME_SRC_DEST_PATHS_ERR);
                    return false;
                }

                if (DATA_OBJ_T != _to_stat.type) {
                    _ec = make_error_code(INVALID_OBJECT_TYPE);
                    return false;
                }

                if (copy_options::skip_existing == _options) {
                    return false;
                }

                if (copy_options::overwrite_existing == _options) {
                    addKeyVal(&input.destDataObjInp.condInput, FORCE_FLAG_KW, "");
                }
                else if (copy_options::update_existing == _options) {
                    const auto from_mtime = last_write_time_by_type(_comm, _from, DATA_OBJ_T, _ec);
                    if (_ec) {
                        return false;
                    }
                    const auto to_mtime = last_write_time_by_type(_comm, _to, DATA_OBJ_T, _ec);
                    if (_ec) {
                        return false;
                    }
                    if (from_mtime <= to_mtime) {
                        return false;
                    }

                    addKeyVal(&input.destDataObjInp.condInput, FORCE_FLAG_KW, "");
                }
            }

            if (!copy_to_c_array(input.srcDataObjInp.objPath, _from.string_view(), _ec) ||
                !copy_to_c_array(input.destDataObjInp.objPath, _to.string_view(), _ec))
            {
                return false;
            }

            if (const auto err = rxDataObjCopy(&_comm, &input); err < 0) {
                _ec = make_error_code(err);
                return false;
            }

            return true;
        }
    } // anonymous namespace

    // Operational functions

    auto copy(rxComm& _comm, const path& _from, const path& _to, std::error_code& _ec) -> void
    {
        copy(_comm, _from, _to, copy_options::none, _ec);
    }

    auto copy(rxComm& _comm, const path& _from, const path& _to, copy_options _options, std::error_code& _ec) -> void
    {
        const auto from_stat = basic_stat(_comm, _from, _ec);
        if (_ec) {
            return;
        }

        if (UNKNOWN_OBJ_T == from_stat.type) {
            _ec = make_error_code(OBJ_PATH_DOES_NOT_EXIST);
            return;
        }

        const auto to_stat = basic_stat(_comm, _to, _ec);
        if (_ec) {
            return;
        }

        if (UNKNOWN_OBJ_T != to_stat.type && from_stat.id == to_stat.id) {
            _ec = make_error_code(SAME_SRC_DEST_PATHS_ERR);
            return;
        }

        if (DATA_OBJ_T != from_stat.type && COLL_OBJ_T != from_stat.type) {
            _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
            return;
        }

        if (UNKNOWN_OBJ_T != to_stat.type && DATA_OBJ_T != to_stat.type && COLL_OBJ_T != to_stat.type) {
            _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
            return;
        }

        if (COLL_OBJ_T == from_stat.type && DATA_OBJ_T == to_stat.type) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return;
        }

        if (DATA_OBJ_T == from_stat.type) {
            if (copy_options::collections_only == (copy_options::collections_only & _options)) {
                return;
            }

            if (COLL_OBJ_T == to_stat.type) {
                const auto target_path = _to / _from.object_name_view();
                const auto target_stat = basic_stat(_comm, target_path, _ec);
                if (_ec) {
                    return;
                }
                copy_data_object_with_stats(_comm, _from, from_stat, target_path, target_stat, _options, _ec);
                return;
            }

            copy_data_object_with_stats(_comm, _from, from_stat, _to, to_stat, _options, _ec);
        }
        else if (COLL_OBJ_T == from_stat.type) {
            if (copy_options::recursive == (copy_options::recursive & _options) ||
                copy_options::none == _options)
            {
                if (UNKNOWN_OBJ_T == to_stat.type) {
                    if (!create_collection(_comm, _to, _from, _ec)) {
                        if (!_ec) {
                            _ec = make_error_code(FILE_CREATE_ERROR);
                        }
                        return;
                    }
                }

                path child_to;
                for (collection_iterator it{_comm, _from, _ec}; !_ec && it != std::default_sentinel; it.increment(_ec)) {
                    child_to.assign_concat(_to.string_view(), it->path().object_name_view());
                    copy(_comm, it->path(), child_to, _options | copy_options::in_recursive_copy, _ec);
                    if (_ec) {
                        return;
                    }
                }
            }
        }
    }

    auto copy(rxComm& _comm, const path& _from, const path& _to, copy_options _options) -> void
    {
        std::error_code ec;
        copy(_comm, _from, _to, _options, ec);
        if (ec) {
            throw filesystem_error{"cannot copy", _from, _to, ec};
        }
    }

    auto copy_data_object(rxComm& _comm, const path& _from, const path& _to, std::error_code& _ec) -> bool
    {
        return copy_data_object(_comm, _from, _to, copy_options::none, _ec);
    }

    auto copy_data_object(rxComm& _comm, const path& _from, const path& _to, copy_options _options, std::error_code& _ec) -> bool
    {
        if (!validate_path_length(_from.string_view(), _ec) || !validate_path_length(_to.string_view(), _ec)) {
            return false;
        }

        const auto from_stat = basic_stat(_comm, _from, _ec);
        if (_ec) {
            return false;
        }

        if (DATA_OBJ_T != from_stat.type) {
            _ec = make_error_code(INVALID_OBJECT_TYPE);
            return false;
        }

        const auto to_stat = basic_stat(_comm, _to, _ec);
        if (_ec) {
            return false;
        }

        return copy_data_object_with_stats(_comm, _from, from_stat, _to, to_stat, _options, _ec);
    }

    auto copy_data_object(rxComm& _comm, const path& _from, const path& _to, copy_options _options) -> bool
    {
        std::error_code ec;
        const bool res = copy_data_object(_comm, _from, _to, _options, ec);
        if (ec) {
            throw filesystem_error{"cannot copy data object", _from, _to, ec};
        }
        return res;
    }

    auto create_collection(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        if (!validate_path_length(_p.string_view(), _ec)) {
            return false;
        }

        const auto parent_st = basic_stat(_comm, _p.parent_path(), _ec);
        if (_ec) {
            return false;
        }
        if (UNKNOWN_OBJ_T == parent_st.type) {
            _ec = make_error_code(OBJ_PATH_DOES_NOT_EXIST);
            return false;
        }

        const auto self_st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return false;
        }
        if (UNKNOWN_OBJ_T != self_st.type) {
            return false;
        }

        collInp_t input{};
        if (!copy_to_c_array(input.collName, _p.string_view(), _ec)) {
            return false;
        }

        if (const auto err = rxCollCreate(&_comm, &input); err != 0) {
            _ec = make_error_code(err);
            return false;
        }

        return true;
    }

    auto create_collection(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = create_collection(_comm, _p, ec);
        if (ec) {
            if (ec == make_error_code(OBJ_PATH_DOES_NOT_EXIST)) {
                throw filesystem_error{"path does not exist", _p.parent_path(), ec};
            }
            throw filesystem_error{"cannot create collection", _p, ec};
        }
        return res;
    }

    auto create_collection(rxComm& _comm, const path& _p, const path& _existing_p, std::error_code& _ec) -> bool
    {
        if (!validate_path(_p, _ec) || !validate_path(_existing_p, _ec)) {
            return false;
        }

        const auto s = status(_comm, _existing_p, status_options::include_permissions, _ec);
        if (_ec) {
            return false;
        }

        if (!is_collection(s)) {
            _ec = make_error_code(INVALID_OBJECT_TYPE);
            return false;
        }

        if (!create_collection(_comm, _p, _ec) && _ec) {
            return false;
        }

        for (const auto& p : s.permissions()) {
            permissions(_comm, _p, p.name, p.prms, _ec);
            if (_ec) {
                return false;
            }
        }

        return true;
    }

    auto create_collection(rxComm& _comm, const path& _p, const path& _existing_p) -> bool
    {
        std::error_code ec;
        const bool res = create_collection(_comm, _p, _existing_p, ec);
        if (ec) {
            if (ec == make_error_code(INVALID_OBJECT_TYPE)) {
                throw filesystem_error{"existing path is not a collection", _existing_p, ec};
            }
            throw filesystem_error{"cannot create collection", _p, _existing_p, ec};
        }
        return res;
    }

    auto create_collections(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        if (!validate_path_length(_p.string_view(), _ec)) {
            return false;
        }

        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return false;
        }
        if (UNKNOWN_OBJ_T != st.type) {
            return false;
        }

        collInp_t input{};
        at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};
        if (!copy_to_c_array(input.collName, _p.string_view(), _ec)) {
            return false;
        }
        addKeyVal(&input.condInput, RECURSIVE_OPR__KW, "");

        if (const auto err = rxCollCreate(&_comm, &input); err != 0) {
            _ec = make_error_code(err);
            return false;
        }

        return true;
    }

    auto create_collections(rxComm& _comm, const path& _p) -> bool
    {
        throw_if_path_length_exceeds_limit(_p);

        if (exists(_comm, _p)) {
            return false;
        }

        collInp_t input{};
        at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};
        std::error_code ec;
        copy_to_c_array(input.collName, _p.string_view(), ec);
        addKeyVal(&input.condInput, RECURSIVE_OPR__KW, "");

        return rxCollCreate(&_comm, &input) == 0;
    }

    auto exists(const object_status& _s) noexcept -> bool
    {
        return status_known(_s) && _s.type() != object_type::not_found;
    }

    auto exists(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return false;
        }
        return st.type != UNKNOWN_OBJ_T;
    }

    auto exists(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = exists(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check existence", _p, ec};
        }
        return res;
    }

    auto is_collection_registered(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        if (!validate_path(_p, _ec)) {
            return false;
        }

        std::string gql = fmt::format(
            "select COLL_ID where COLL_NAME = '{}' fetch first 1 rows only",
            sql_escape(_p));

        nlohmann::json rows;
        if (!execute_genquery2(_comm, _p, gql, rows, _ec)) {
            return false;
        }

        return !rows.empty();
    }

    auto is_collection_registered(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = is_collection_registered(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check if collection is registered", _p, ec};
        }
        return res;
    }

    auto is_data_object_registered(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        if (!validate_path(_p, _ec)) {
            return false;
        }

        std::string gql = fmt::format(
            "select DATA_ID where COLL_NAME = '{}' and DATA_NAME = '{}' fetch first 1 rows only",
            sql_escape(_p.parent_path_view()),
            sql_escape(_p.object_name_view()));

        nlohmann::json rows;
        if (!execute_genquery2(_comm, _p, gql, rows, _ec)) {
            return false;
        }

        return !rows.empty();
    }

    auto is_data_object_registered(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = is_data_object_registered(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check if data object is registered", _p, ec};
        }
        return res;
    }

    auto equivalent(rxComm& _comm, const path& _p1, const path& _p2, std::error_code& _ec) -> bool
    {
        const auto p1_info = basic_stat(_comm, _p1, _ec);
        if (_ec) {
            return false;
        }
        if (p1_info.type == UNKNOWN_OBJ_T) {
            _ec = make_error_code(OBJ_PATH_DOES_NOT_EXIST);
            return false;
        }

        const auto p2_info = basic_stat(_comm, _p2, _ec);
        if (_ec) {
            return false;
        }
        if (p2_info.type == UNKNOWN_OBJ_T) {
            _ec = make_error_code(OBJ_PATH_DOES_NOT_EXIST);
            return false;
        }

        return p1_info.id == p2_info.id;
    }

    auto equivalent(rxComm& _comm, const path& _p1, const path& _p2) -> bool
    {
        std::error_code ec;
        const bool res = equivalent(_comm, _p1, _p2, ec);
        if (ec) {
            throw filesystem_error{"cannot check equivalence", _p1, _p2, ec};
        }
        return res;
    }

    auto data_object_size(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::uintmax_t
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return static_cast<std::uintmax_t>(-1);
        }

        if (DATA_OBJ_T != st.type) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return static_cast<std::uintmax_t>(-1);
        }

        std::string gql = fmt::format(
            "select DATA_SIZE, DATA_MODIFY_TIME "
            "where COLL_NAME = '{}' and DATA_NAME = '{}' and DATA_REPL_STATUS = '1' "
            "order by DATA_MODIFY_TIME desc fetch first 1 rows only",
            sql_escape(_p.parent_path_view()),
            sql_escape(_p.object_name_view()));

        nlohmann::json rows;
        if (!execute_genquery2(_comm, _p, gql, rows, _ec)) {
            return static_cast<std::uintmax_t>(-1);
        }

        if (rows.empty() || !rows[0].is_array() || rows[0].empty() || !rows[0][0].is_string()) {
            _ec = make_error_code(SYS_NO_GOOD_REPLICA);
            return static_cast<std::uintmax_t>(-1);
        }

        std::uint64_t row_size = 0;
        if (parse_int(rows[0][0].get_ref<const std::string&>(), row_size)) {
            return row_size;
        }

        _ec = make_error_code(SYS_INTERNAL_ERR);
        return static_cast<std::uintmax_t>(-1);
    }

    auto data_object_size(rxComm& _comm, const path& _p) -> std::uintmax_t
    {
        std::error_code ec;
        const auto res = data_object_size(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot get data object size", _p, ec};
        }
        return res;
    }

    auto is_collection(const object_status& _s) noexcept -> bool
    {
        return _s.type() == object_type::collection;
    }

    auto is_collection(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return false;
        }
        return st.type == COLL_OBJ_T;
    }

    auto is_collection(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = is_collection(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check if path is collection", _p, ec};
        }
        return res;
    }

    auto is_special_collection(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        if (!validate_path(_p, _ec)) {
            return false;
        }

        std::string gql = fmt::format(
            "select COLL_TYPE, COLL_INFO1, COLL_INFO2 where COLL_NAME = '{}' fetch first 1 rows only",
            sql_escape(_p));

        nlohmann::json rows;
        if (!execute_genquery2(_comm, _p, gql, rows, _ec)) {
            return false;
        }

        if (!rows.empty() && rows[0].is_array() && rows[0].size() >= 3 &&
            rows[0][0].is_string() && rows[0][1].is_string() && rows[0][2].is_string())
        {
            const auto& ctype = rows[0][0].get_ref<const std::string&>();
            const auto& info1 = rows[0][1].get_ref<const std::string&>();
            const auto& info2 = rows[0][2].get_ref<const std::string&>();
            return !ctype.empty() && (!info1.empty() || !info2.empty());
        }

        return false;
    }

    auto is_special_collection(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = is_special_collection(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check if collection is special", _p, ec};
        }
        return res;
    }

    auto is_empty(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return false;
        }

        if (COLL_OBJ_T == st.type) {
            return is_collection_empty(_comm, _p, _ec);
        }

        _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
        return false;
    }

    auto is_empty(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = is_empty(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check emptiness", _p, ec};
        }
        return res;
    }

    auto is_other(const object_status& _s) noexcept -> bool
    {
        return _s.type() == object_type::unknown;
    }

    auto is_other(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return false;
        }
        return to_object_type(st.type) == object_type::unknown;
    }

    auto is_other(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = is_other(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check if path is other", _p, ec};
        }
        return res;
    }

    auto is_data_object(const object_status& _s) noexcept -> bool
    {
        return _s.type() == object_type::data_object;
    }

    auto is_data_object(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return false;
        }
        return st.type == DATA_OBJ_T;
    }

    auto is_data_object(rxComm& _comm, const path& _p) -> bool
    {
        std::error_code ec;
        const bool res = is_data_object(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot check if path is data object", _p, ec};
        }
        return res;
    }

    auto last_write_time(rxComm& _comm, const path& _p, std::error_code& _ec) -> object_time_type
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return object_time_type::min();
        }
        return last_write_time_by_type(_comm, _p, st.type, _ec);
    }

    auto last_write_time(rxComm& _comm, const path& _p) -> object_time_type
    {
        std::error_code ec;
        const auto res = last_write_time(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot get mtime", _p, ec};
        }
        return res;
    }

    auto last_write_time(rxComm& _comm, const path& _p, object_time_type _new_time, std::error_code& _ec) -> void
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return;
        }

        if (COLL_OBJ_T != st.type) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return;
        }

        const auto timestamp = fmt::format("{:011}", _new_time.time_since_epoch().count());

        collInp_t input{};
        at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};
        if (!copy_to_c_array(input.collName, _p.string_view(), _ec)) {
            return;
        }
        addKeyVal(&input.condInput, COLLECTION_MTIME_KW, timestamp.c_str());

        if (const auto err = rxModColl(&_comm, &input); err != 0) {
            _ec = make_error_code(err);
        }
    }

    auto last_write_time(rxComm& _comm, const path& _p, object_time_type _new_time) -> void
    {
        std::error_code ec;
        last_write_time(_comm, _p, _new_time, ec);
        if (ec) {
            throw filesystem_error{"cannot set mtime", _p, ec};
        }
    }

    auto remove(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool
    {
        return remove(_comm, _p, remove_options::none, _ec);
    }

    auto remove(rxComm& _comm, const path& _p, remove_options _opts, std::error_code& _ec) -> bool
    {
        extended_remove_options opts{};
        opts.no_trash = (remove_options::no_trash == _opts);
        opts.recursive = false;
        return remove_impl(_comm, _p, opts, _ec);
    }

    auto remove(rxComm& _comm, const path& _p, remove_options _opts) -> bool
    {
        std::error_code ec;
        const bool res = remove(_comm, _p, _opts, ec);
        if (ec) {
            throw filesystem_error{"cannot remove", _p, ec};
        }
        return res;
    }

    auto remove(rxComm& _comm, const path& _p, extended_remove_options _opts, std::error_code& _ec) -> bool
    {
        return remove_impl(_comm, _p, _opts, _ec);
    }

    auto remove(rxComm& _comm, const path& _p, extended_remove_options _opts) -> bool
    {
        std::error_code ec;
        const bool res = remove(_comm, _p, _opts, ec);
        if (ec) {
            throw filesystem_error{"cannot remove", _p, ec};
        }
        return res;
    }

    auto remove_all(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::uintmax_t
    {
        return remove_all(_comm, _p, remove_options::none, _ec);
    }

    auto remove_all(rxComm& _comm, const path& _p, remove_options _opts, std::error_code& _ec) -> std::uintmax_t
    {
        extended_remove_options opts{};
        opts.no_trash = (remove_options::no_trash == _opts);
        opts.recursive = true;
        return remove_all_impl(_comm, _p, opts, _ec);
    }

    auto remove_all(rxComm& _comm, const path& _p, remove_options _opts) -> std::uintmax_t
    {
        std::error_code ec;
        const auto res = remove_all(_comm, _p, _opts, ec);
        if (ec) {
            throw filesystem_error{"cannot remove all", _p, ec};
        }
        return res;
    }

    auto remove_all(rxComm& _comm, const path& _p, extended_remove_options _opts, std::error_code& _ec) -> std::uintmax_t
    {
        _opts.recursive = true;
        return remove_all_impl(_comm, _p, _opts, _ec);
    }

    auto remove_all(rxComm& _comm, const path& _p, extended_remove_options _opts) -> std::uintmax_t
    {
        std::error_code ec;
        const auto res = remove_all(_comm, _p, _opts, ec);
        if (ec) {
            throw filesystem_error{"cannot remove all", _p, ec};
        }
        return res;
    }

    auto permissions(rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = false;
        set_permissions_impl(add_admin_flag, _comm, _p, _user_or_group, _prms, _ec);
    }

    auto permissions(rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms) -> void
    {
        std::error_code ec;
        permissions(_comm, _p, _user_or_group, _prms, ec);
        if (ec) {
            throw filesystem_error{"cannot set permissions", _p, ec};
        }
    }

    auto permissions(admin_tag, rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = true;
        set_permissions_impl(add_admin_flag, _comm, _p, _user_or_group, _prms, _ec);
    }

    auto permissions(admin_tag, rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms) -> void
    {
        std::error_code ec;
        permissions(admin, _comm, _p, _user_or_group, _prms, ec);
        if (ec) {
            throw filesystem_error{"cannot set permissions", _p, ec};
        }
    }

    auto enable_inheritance(rxComm& _comm, const path& _p, bool _value, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = false;
        set_inheritance_impl(add_admin_flag, _comm, _p, _value, _ec);
    }

    auto enable_inheritance(rxComm& _comm, const path& _p, bool _value) -> void
    {
        std::error_code ec;
        enable_inheritance(_comm, _p, _value, ec);
        if (ec) {
            throw filesystem_error{"cannot set inheritance", _p, ec};
        }
    }

    auto enable_inheritance(admin_tag, rxComm& _comm, const path& _p, bool _value, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = true;
        set_inheritance_impl(add_admin_flag, _comm, _p, _value, _ec);
    }

    auto enable_inheritance(admin_tag, rxComm& _comm, const path& _p, bool _value) -> void
    {
        std::error_code ec;
        enable_inheritance(admin, _comm, _p, _value, ec);
        if (ec) {
            throw filesystem_error{"cannot set inheritance", _p, ec};
        }
    }

    auto rename(rxComm& _comm, const path& _old_p, const path& _new_p, std::error_code& _ec) -> void
    {
        if (!validate_path(_old_p, _ec) || !validate_path(_new_p, _ec)) {
            return;
        }

        const auto old_norm = _old_p.lexically_normal();
        const auto new_norm = _new_p.lexically_normal();

        const auto old_stat = basic_stat(_comm, old_norm, _ec);
        if (_ec) {
            return;
        }

        const auto new_stat = basic_stat(_comm, new_norm, _ec);
        if (_ec) {
            return;
        }

        // Case 1: "_new_p" is the same object as "_old_p".
        if (UNKNOWN_OBJ_T != old_stat.type && UNKNOWN_OBJ_T != new_stat.type && old_stat.id == new_stat.id) {
            return;
        }

        if (has_prefix(_new_p, _old_p)) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return;
        }

        {
            const auto obj_name = _new_p.object_name_view();
            if (obj_name == "." || obj_name == "..") {
                _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
                return;
            }
        }

        if (DATA_OBJ_T == old_stat.type) {
            if (UNKNOWN_OBJ_T != new_stat.type) {
                if (DATA_OBJ_T != new_stat.type) {
                    _ec = make_error_code(INVALID_OBJECT_TYPE);
                    return;
                }
            }
            else {
                const auto parent_st = basic_stat(_comm, _new_p.parent_path(), _ec);
                if (_ec) {
                    return;
                }
                if (UNKNOWN_OBJ_T == parent_st.type) {
                    _ec = make_error_code(OBJ_PATH_DOES_NOT_EXIST);
                    return;
                }
            }
        }
        else if (COLL_OBJ_T == old_stat.type) {
            if (UNKNOWN_OBJ_T != new_stat.type) {
                if (COLL_OBJ_T != new_stat.type) {
                    _ec = make_error_code(INVALID_OBJECT_TYPE);
                    return;
                }
            }
            else if (is_separator(_new_p.string().back())) {
                _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
                return;
            }
            else {
                const auto parent_st = basic_stat(_comm, _new_p.parent_path(), _ec);
                if (_ec) {
                    return;
                }
                if (COLL_OBJ_T != parent_st.type) {
                    _ec = make_error_code(OBJ_PATH_DOES_NOT_EXIST);
                    return;
                }
            }
        }
        else {
            _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
            return;
        }

        dataObjCopyInp_t input{};
        if (!copy_to_c_array(input.srcDataObjInp.objPath, _old_p.string_view(), _ec) ||
            !copy_to_c_array(input.destDataObjInp.objPath, _new_p.string_view(), _ec))
        {
            return;
        }

        if (const auto err = rxDataObjRename(&_comm, &input); err < 0) {
            _ec = make_error_code(err);
        }
    }

    auto rename(rxComm& _comm, const path& _old_p, const path& _new_p) -> void
    {
        std::error_code ec;
        rename(_comm, _old_p, _new_p, ec);
        if (ec) {
            throw filesystem_error{"cannot rename object", _old_p, _new_p, ec};
        }
    }

    auto move(rxComm& _comm, const path& _old_p, const path& _new_p, std::error_code& _ec) -> void
    {
        rename(_comm, _old_p, _new_p, _ec);
    }

    auto move(rxComm& _comm, const path& _old_p, const path& _new_p) -> void
    {
        rename(_comm, _old_p, _new_p);
    }

    auto data_object_checksum(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::string
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return {};
        }

        if (DATA_OBJ_T != st.type) {
            _ec = make_error_code(SYS_INVALID_INPUT_PARAM);
            return {};
        }

        std::string gql = fmt::format(
            "select DATA_CHECKSUM, DATA_MODIFY_TIME "
            "where COLL_NAME = '{}' and DATA_NAME = '{}' and DATA_REPL_STATUS = '1' "
            "order by DATA_MODIFY_TIME desc fetch first 1 rows only",
            sql_escape(_p.parent_path_view()),
            sql_escape(_p.object_name_view()));

        nlohmann::json rows;
        if (!execute_genquery2(_comm, _p, gql, rows, _ec)) {
            return {};
        }

        if (!rows.empty() && rows[0].is_array() && !rows[0].empty() && rows[0][0].is_string()) {
            return rows[0][0].get<std::string>();
        }

        return {};
    }

    auto data_object_checksum(rxComm& _comm, const path& _p) -> std::string
    {
        std::error_code ec;
        auto res = data_object_checksum(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot get data object checksum", _p, ec};
        }
        return res;
    }

    auto status(rxComm& _comm, const path& _p, status_options _opts, std::error_code& _ec) -> object_status
    {
        const auto s = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return object_status{object_type::none};
        }

        object_status result{to_object_type(s.type)};

        if (COLL_OBJ_T == s.type &&
            (_opts & status_options::include_inheritance) == status_options::include_inheritance)
        {
            result.inheritance(get_inheritance(_comm, _p, s.type, _ec));
            if (_ec) {
                return object_status{object_type::none};
            }
        }

        if ((DATA_OBJ_T == s.type || COLL_OBJ_T == s.type) &&
            (_opts & status_options::include_permissions) == status_options::include_permissions)
        {
            std::vector<entity_permission> prms;
            fetch_permissions(_comm, _p, s, prms, _ec);
            if (_ec) {
                return object_status{object_type::none};
            }
            result.permissions(std::move(prms));
        }

        return result;
    }

    auto status(rxComm& _comm, const path& _p, std::error_code& _ec) -> object_status
    {
        return status(_comm, _p, status_options::full, _ec);
    }

    auto status(rxComm& _comm, const path& _p, status_options _opts) -> object_status
    {
        std::error_code ec;
        auto res = status(_comm, _p, _opts, ec);
        if (ec) {
            throw filesystem_error{"cannot get status", _p, ec};
        }
        return res;
    }

    auto status(rxComm& _comm, const path& _p) -> object_status
    {
        return status(_comm, _p, status_options::full);
    }

    auto status_known(const object_status& _s) noexcept -> bool
    {
        return _s.type() != object_type::none;
    }

    auto get_metadata(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::vector<metadata>
    {
        const auto st = basic_stat(_comm, _p, _ec);
        if (_ec) {
            return {};
        }

        std::string sql;
        if (DATA_OBJ_T == st.type) {
            sql = fmt::format(
                "select META_DATA_ATTR_NAME, META_DATA_ATTR_VALUE, META_DATA_ATTR_UNITS "
                "where DATA_NAME = '{}' and COLL_NAME = '{}' and META_DATA_ATTR_NAME is not null",
                sql_escape(_p.object_name_view()),
                sql_escape(_p.parent_path_view()));
        }
        else if (COLL_OBJ_T == st.type) {
            sql = fmt::format(
                "select META_COLL_ATTR_NAME, META_COLL_ATTR_VALUE, META_COLL_ATTR_UNITS "
                "where COLL_NAME = '{}' and META_COLL_ATTR_NAME is not null",
                sql_escape(_p));
        }
        else {
            _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
            return {};
        }

        std::vector<metadata> results;
        if (!execute_genquery2_paged(_comm, _p, sql, _ec, [&](const nlohmann::json& _row) {
                if (_row.size() < 3 || !_row[0].is_string() || !_row[1].is_string() || !_row[2].is_string()) {
                    _ec = make_error_code(SYS_INTERNAL_ERR);
                    return false;
                }
                const auto& attr = _row[0].get_ref<const std::string&>();
                const auto& val = _row[1].get_ref<const std::string&>();
                const auto& units = _row[2].get_ref<const std::string&>();
                if (attr.empty() && val.empty() && units.empty()) {
                    return true;
                }
                results.push_back({attr, val, units});
                return true;
            }))
        {
            return {};
        }

        return results;
    }

    auto get_metadata(rxComm& _comm, const path& _p) -> std::vector<metadata>
    {
        std::error_code ec;
        auto res = get_metadata(_comm, _p, ec);
        if (ec) {
            throw filesystem_error{"cannot get metadata", _p, ec};
        }
        return res;
    }

    auto set_metadata(rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = false;
        do_metadata_op_impl(add_admin_flag, _comm, _p, _metadata, "set", _ec);
    }

    auto set_metadata(rxComm& _comm, const path& _p, const metadata& _metadata) -> void
    {
        std::error_code ec;
        set_metadata(_comm, _p, _metadata, ec);
        if (ec) {
            throw filesystem_error{"cannot set metadata", _p, ec};
        }
    }

    auto set_metadata(admin_tag, rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = true;
        do_metadata_op_impl(add_admin_flag, _comm, _p, _metadata, "set", _ec);
    }

    auto set_metadata(admin_tag, rxComm& _comm, const path& _p, const metadata& _metadata) -> void
    {
        std::error_code ec;
        set_metadata(admin, _comm, _p, _metadata, ec);
        if (ec) {
            throw filesystem_error{"cannot set metadata", _p, ec};
        }
    }

    auto add_metadata(rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = false;
        do_metadata_op_impl(add_admin_flag, _comm, _p, _metadata, "add", _ec);
    }

    auto add_metadata(rxComm& _comm, const path& _p, const metadata& _metadata) -> void
    {
        std::error_code ec;
        add_metadata(_comm, _p, _metadata, ec);
        if (ec) {
            throw filesystem_error{"cannot add metadata", _p, ec};
        }
    }

    auto add_metadata(admin_tag, rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = true;
        do_metadata_op_impl(add_admin_flag, _comm, _p, _metadata, "add", _ec);
    }

    auto add_metadata(admin_tag, rxComm& _comm, const path& _p, const metadata& _metadata) -> void
    {
        std::error_code ec;
        add_metadata(admin, _comm, _p, _metadata, ec);
        if (ec) {
            throw filesystem_error{"cannot add metadata", _p, ec};
        }
    }

    auto remove_metadata(rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = false;
        do_metadata_op_impl(add_admin_flag, _comm, _p, _metadata, "rm", _ec);
    }

    auto remove_metadata(rxComm& _comm, const path& _p, const metadata& _metadata) -> void
    {
        std::error_code ec;
        remove_metadata(_comm, _p, _metadata, ec);
        if (ec) {
            throw filesystem_error{"cannot remove metadata", _p, ec};
        }
    }

    auto remove_metadata(admin_tag, rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void
    {
        constexpr auto add_admin_flag = true;
        do_metadata_op_impl(add_admin_flag, _comm, _p, _metadata, "rm", _ec);
    }

    auto remove_metadata(admin_tag, rxComm& _comm, const path& _p, const metadata& _metadata) -> void
    {
        std::error_code ec;
        remove_metadata(admin, _comm, _p, _metadata, ec);
        if (ec) {
            throw filesystem_error{"cannot remove metadata", _p, ec};
        }
    }
} // namespace irods::experimental::filesystem::NAMESPACE_IMPL
