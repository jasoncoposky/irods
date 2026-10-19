#include "irods/user_administration.hpp"

#ifdef IRODS_USER_ADMINISTRATION_ENABLE_SERVER_SIDE_API
#  define IRODS_QUERY_ENABLE_SERVER_SIDE_API
#  include "irods/rodsConnect.h"
#  include "irods/rs_genquery2.hpp"
#endif // IRODS_USER_ADMINISTRATION_ENABLE_SERVER_SIDE_API

#include "irods/genquery2_builder.hpp"
#include "irods/version.hpp"

namespace irods::experimental::administration::NAMESPACE_IMPL
{
    namespace
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto get_local_zone([[maybe_unused]] RxComm& _comm) -> std::string
        {
#ifdef IRODS_USER_ADMINISTRATION_ENABLE_SERVER_SIDE_API
            return getLocalZoneName();
#else
            const auto q = gq2::builder::select({"ZONE_NAME"})
                               .where(col("ZONE_TYPE") == "local")
                               .limit(1)
                               .build();

            const auto rows = gq2::execute(_comm, q);
            if (!rows.empty()) {
                return rows.front().at(0).get<std::string>();
            }

            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(CAT_NO_ROWS_FOUND, "Cannot get local zone name.");
#endif // IRODS_USER_ADMINISTRATION_ENABLE_SERVER_SIDE_API
        } // get_local_zone
    } // anonymous namespace

    auto local_unique_name(RxComm& _comm, const user& _user) -> std::string
    {
        // Implies that the user belongs to the local zone and
        // is not a remote user (i.e. federation).
        if (_user.zone.empty()) {
            return _user.name;
        }

        auto name = _user.name;

        if (_user.zone != get_local_zone(_comm)) {
            name += '#';
            name += _user.zone;
        }

        return name;
    } // local_unique_name

    auto add_user(RxComm& _comm, const user& _user, user_type _user_type, zone_type _zone_type) -> void
    {
        std::string name = local_unique_name(_comm, _user);
        std::string zone;

        if (zone_type::local == _zone_type) {
            zone = get_local_zone(_comm);
        }

        const auto current_user{user{_comm.clientUser.userName, _comm.clientUser.rodsZone}};
        const auto user_type{type(_comm, current_user)};

        if (user_type == irods::experimental::administration::user_type::groupadmin) {
            // clang-format off
            userAdminInp_t input{.arg0 = "mkuser",
                                 .arg1 = const_cast<char*>(name.data()),
                                 .arg3 = const_cast<char*>(zone.data())};
            // clang-format on
            if (const auto ec{rxUserAdmin(&_comm, &input)}; ec != 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(ec, fmt::format("Failed to add user [{}].", name));
            }
            return;
        }

        GeneralAdminInput input{};
        input.arg0 = "add";
        input.arg1 = "user";
        input.arg2 = name.data();
        input.arg3 = to_c_str(_user_type);
        input.arg4 = zone.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec != 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to add user [{}].", name));
        }
    } // add_user

    auto remove_user(RxComm& _comm, const user& _user) -> void
    {
        const auto name = local_unique_name(_comm, _user);

        GeneralAdminInput input{};
        input.arg0 = "rm";
        input.arg1 = "user";
        input.arg2 = name.data();
        input.arg3 = _user.zone.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec != 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to remove user [{}].", name));
        }
    } // remove_user

    auto add_group(RxComm& _comm, const group& _group) -> void
    {
        const auto zone = get_local_zone(_comm);

        const auto current_user{user{_comm.clientUser.userName, _comm.clientUser.rodsZone}};
        const auto user_type{type(_comm, current_user)};

        if (user_type == irods::experimental::administration::user_type::groupadmin) {
            userAdminInp_t input{.arg0 = "mkgroup",
                                 .arg1 = const_cast<char*>(_group.name.data()),
                                 .arg2 = "rodsgroup",
                                 .arg3 = const_cast<char*>(zone.data())};

            if (const auto ec{rxUserAdmin(&_comm, &input)}; ec != 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(ec, fmt::format("Failed to add group [{}].", _group.name));
            }
            return;
        }

        GeneralAdminInput input{};
        input.arg0 = "add";
#ifdef IRODS_USER_ADMINISTRATION_ENABLE_SERVER_SIDE_API
        // If this is the server side, the API should know about how to add a "group". arg3 - which represents the
        // user "type" when adding a user - is not necessary because groups do not have types.
        input.arg1 = "group";
#else
        // If this is not the server side, what we provide to the API depends on the version of the server with which
        // the client is connected. For 4.3.4 and later, the API should add a "group" without providing a user "type".
        // Any earlier version only knows how to add a "user" of type "rodsgroup" (which is a group).
        const auto version = irods::to_version(static_cast<char*>(_comm.svrVersion->relVersion));
        if (version && version.value() > irods::version{4, 3, 3}) {
            input.arg1 = "group";
        }
        else {
            input.arg1 = "user";
            input.arg3 = "rodsgroup";
        }
#endif // IRODS_USER_ADMINISTRATION_ENABLE_SERVER_SIDE_API
        input.arg2 = _group.name.data();
        input.arg4 = zone.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec != 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to add group [{}].", _group.name));
        }
    } // add_group

    auto remove_group(RxComm& _comm, const group& _group) -> void
    {
        const auto zone = get_local_zone(_comm);
        GeneralAdminInput input{};
        input.arg0 = "rm";
#ifdef IRODS_USER_ADMINISTRATION_ENABLE_SERVER_SIDE_API
        input.arg1 = "group";
#else
        const auto version = irods::to_version(_comm.svrVersion->relVersion);
        input.arg1 = (version && version.value() > irods::version{4, 3, 1}) ? "group" : "user";
#endif
        input.arg2 = _group.name.data();
        input.arg3 = zone.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec != 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to remove group [{}].", _group.name));
        }
    } // remove_group

    auto add_user_to_group(RxComm& _comm, const group& _group, const user& _user) -> void
    {
        const auto name{local_unique_name(_comm, _user)};
        const auto current_user{user{_comm.clientUser.userName, _comm.clientUser.rodsZone}};
        const auto user_type{type(_comm, current_user)};

        if (user_type == irods::experimental::administration::user_type::groupadmin) {
            userAdminInp_t input{.arg0 = "modify",
                                 .arg1 = "group",
                                 .arg2 = const_cast<char*>(_group.name.data()),
                                 .arg3 = "add",
                                 .arg4 = const_cast<char*>(_user.name.data()),
                                 .arg5 = const_cast<char*>(_user.zone.data())};

            if (const auto ec{rxUserAdmin(&_comm, &input)}; ec != 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(ec, fmt::format("Failed to add user [{}] to group [{}].", name, _group.name));
            }
            return;
        }

        GeneralAdminInput input{};
        input.arg0 = "modify";
        input.arg1 = "group";
        input.arg2 = _group.name.data();
        input.arg3 = "add";
        input.arg4 = _user.name.data();
        input.arg5 = _user.zone.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec != 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to add user [{}] to group [{}].", name, _group.name));
        }
    } // add_user_to_group

    auto remove_user_from_group(RxComm& _comm, const group& _group, const user& _user) -> void
    {
        const auto name{local_unique_name(_comm, _user)};
        const auto current_user{user{_comm.clientUser.userName, _comm.clientUser.rodsZone}};
        const auto user_type{type(_comm, current_user)};

        if (user_type == irods::experimental::administration::user_type::groupadmin) {
            userAdminInp_t input{.arg0 = "modify",
                                 .arg1 = "group",
                                 .arg2 = const_cast<char*>(_group.name.data()),
                                 .arg3 = "remove",
                                 .arg4 = const_cast<char*>(_user.name.data()),
                                 .arg5 = const_cast<char*>(_user.zone.data())};

            if (const auto ec{rxUserAdmin(&_comm, &input)}; ec != 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(ec, fmt::format("Failed to remove user [{}] from group [{}].", name, _group.name));
            }
            return;
        }

        GeneralAdminInput input{};
        input.arg0 = "modify";
        input.arg1 = "group";
        input.arg2 = _group.name.data();
        input.arg3 = "remove";
        input.arg4 = _user.name.data();
        input.arg5 = _user.zone.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec != 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to remove user [{}] from group [{}].", name, _group.name));
        }
    } // remove_user_from_group

    auto users(RxComm& _comm) -> std::vector<user>
    {
        const auto q = gq2::builder::select({"USER_NAME", "USER_ZONE"})
                           .where(col("USER_TYPE") != "rodsgroup")
                           .build();

        const auto rows = gq2::execute(_comm, q);
        std::vector<user> result;
        result.reserve(rows.size());

        for (const auto& row : rows) {
            result.emplace_back(row.at(0).get<std::string>(), row.at(1).get<std::string>());
        }

        return result;
    } // users

    auto users(RxComm& _comm, const group& _group) -> std::vector<user>
    {
        const auto q = gq2::builder::select({"USER_NAME", "USER_ZONE"})
                           .where(col("USER_TYPE") != "rodsgroup" &&
                                  col("USER_GROUP_NAME") == _group.name)
                           .build();

        const auto rows = gq2::execute(_comm, q);
        std::vector<user> result;
        result.reserve(rows.size());

        for (const auto& row : rows) {
            result.emplace_back(row.at(0).get<std::string>(), row.at(1).get<std::string>());
        }

        return result;
    } // users

    auto groups(RxComm& _comm) -> std::vector<group>
    {
        const auto q = gq2::builder::select({"USER_GROUP_NAME"})
                           .where(col("USER_TYPE") == "rodsgroup")
                           .build();

        const auto rows = gq2::execute(_comm, q);
        std::vector<group> result;
        result.reserve(rows.size());

        for (const auto& row : rows) {
            result.emplace_back(row.at(0).get<std::string>());
        }

        return result;
    } // groups

    auto groups(RxComm& _comm, const user& _user) -> std::vector<group>
    {
        const auto zone = _user.zone.empty() ? get_local_zone(_comm) : _user.zone;
        const auto q = gq2::builder::select({"USER_GROUP_NAME"})
                           .where(col("USER_TYPE") != "rodsgroup" &&
                                  col("USER_NAME") == _user.name &&
                                  col("USER_ZONE") == zone)
                           .build();

        const auto rows = gq2::execute(_comm, q, zone);
        std::vector<group> result;
        result.reserve(rows.size());

        for (const auto& row : rows) {
            result.emplace_back(row.at(0).get<std::string>());
        }

        return result;
    } // groups

    auto exists(RxComm& _comm, const user& _user) -> bool
    {
        const auto zone = _user.zone.empty() ? get_local_zone(_comm) : _user.zone;
        const auto q = gq2::builder::select({"USER_ID"})
                           .where(col("USER_TYPE") != "rodsgroup" &&
                                  col("USER_NAME") == _user.name &&
                                  col("USER_ZONE") == zone)
                           .limit(1)
                           .build();

        return !gq2::execute(_comm, q).empty();
    } // exists

    auto exists(RxComm& _comm, const group& _group) -> bool
    {
        const auto q = gq2::builder::select({"USER_GROUP_ID"})
                           .where(col("USER_TYPE") == "rodsgroup" &&
                                  col("USER_GROUP_NAME") == _group.name)
                           .limit(1)
                           .build();

        return !gq2::execute(_comm, q).empty();
    } // exists

    auto id(RxComm& _comm, const user& _user) -> std::optional<std::string>
    {
        const auto zone = _user.zone.empty() ? get_local_zone(_comm) : _user.zone;
        const auto q = gq2::builder::select({"USER_ID"})
                           .where(col("USER_TYPE") != "rodsgroup" &&
                                  col("USER_NAME") == _user.name &&
                                  col("USER_ZONE") == zone)
                           .limit(1)
                           .build();

        const auto rows = gq2::execute(_comm, q);
        if (!rows.empty()) {
            return rows.front().at(0).get<std::string>();
        }

        return std::nullopt;
    } // id

    auto id(RxComm& _comm, const group& _group) -> std::optional<std::string>
    {
        const auto q = gq2::builder::select({"USER_GROUP_ID"})
                           .where(col("USER_TYPE") == "rodsgroup" &&
                                  col("USER_GROUP_NAME") == _group.name)
                           .limit(1)
                           .build();

        const auto rows = gq2::execute(_comm, q);
        if (!rows.empty()) {
            return rows.front().at(0).get<std::string>();
        }

        return std::nullopt;
    } // id

    auto type(RxComm& _comm, const user& _user) -> std::optional<user_type>
    {
        const auto zone = _user.zone.empty() ? get_local_zone(_comm) : _user.zone;
        const auto q = gq2::builder::select({"USER_TYPE"})
                           .where(col("USER_TYPE") != "rodsgroup" &&
                                  col("USER_NAME") == _user.name &&
                                  col("USER_ZONE") == zone)
                           .limit(1)
                           .build();

        const auto rows = gq2::execute(_comm, q);
        if (!rows.empty()) {
            return to_user_type(rows.front().at(0).get_ref<const std::string&>());
        }

        return std::nullopt;
    } // type

    auto auth_names(RxComm& _comm, const user& _user) -> std::vector<std::string>
    {
        const auto zone = _user.zone.empty() ? get_local_zone(_comm) : _user.zone;
        const auto q = gq2::builder::select({"USER_DN"})
                           .where(col("USER_TYPE") != "rodsgroup" &&
                                  col("USER_NAME") == _user.name &&
                                  col("USER_ZONE") == zone)
                           .build();

        const auto rows = gq2::execute(_comm, q);
        std::vector<std::string> result;
        result.reserve(rows.size());

        for (const auto& row : rows) {
            result.push_back(row.at(0).get<std::string>());
        }

        return result;
    } // auth_names

    auto user_is_member_of_group(RxComm& _comm, const group& _group, const user& _user) -> bool
    {
        const auto zone = _user.zone.empty() ? get_local_zone(_comm) : _user.zone;
        const auto q = gq2::builder::select({"USER_ID"})
                           .where(col("USER_TYPE") != "rodsgroup" &&
                                  col("USER_NAME") == _user.name &&
                                  col("USER_ZONE") == zone &&
                                  col("USER_GROUP_NAME") == _group.name)
                           .limit(1)
                           .build();

        return !gq2::execute(_comm, q).empty();
    } // user_is_member_of_group
} // namespace irods::experimental::administration::NAMESPACE_IMPL
