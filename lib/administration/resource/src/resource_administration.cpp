#include "irods/resource_administration.hpp"

#undef rxGeneralAdmin

#ifdef IRODS_RESOURCE_ADMINISTRATION_ENABLE_SERVER_SIDE_API
#  include "irods/rsGeneralAdmin.hpp"
#  include "irods/rs_genquery2.hpp"
#  define rxGeneralAdmin rsGeneralAdmin
#  define IRODS_QUERY_ENABLE_SERVER_SIDE_API
#else
#  include "irods/generalAdmin.h"
#  define rxGeneralAdmin rcGeneralAdmin
#endif // IRODS_RESOURCE_ADMINISTRATION_ENABLE_SERVER_SIDE_API

#include "irods/genquery2_builder.hpp"
#include "irods/rodsErrorTable.h"

#include <fmt/format.h>

#include <charconv>

namespace irods::experimental::administration::NAMESPACE_IMPL
{
    namespace
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        auto parse_u64(std::string_view _sv) noexcept -> std::uint64_t
        {
            std::uint64_t val = 0;
            std::from_chars(_sv.data(), _sv.data() + _sv.size(), val);
            return val;
        }
    } // anonymous namespace

    auto add_resource(RxComm& _comm, const resource_registration_info& _info) -> void
    {
        std::string location;

        if (!_info.host_name.empty() && !_info.vault_path.empty()) {
            location = fmt::format("{}:{}", _info.host_name, _info.vault_path);
        }

        generalAdminInp_t input{};
        input.arg0 = "add";
        input.arg1 = "resource";
        input.arg2 = _info.resource_name.data();
        input.arg3 = _info.resource_type.data();
        input.arg4 = location.data();
        input.arg5 = _info.context_string.data();
        input.arg6 = ""; // Zone name (cannot be null).

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec < 0) {
            constexpr char* msg = "Could not create resource [name=[{}], type=[{}], location=[{}], context=[{}]]";
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format(msg, _info.resource_name, _info.resource_type, location, _info.context_string));
        }
    }

    auto remove_resource(RxComm& _comm, const resource_name_type _name) -> void
    {
        generalAdminInp_t input{};
        input.arg0 = "rm";
        input.arg1 = "resource";
        input.arg2 = _name.data();
        input.arg3 = ""; // Dryrun flag (cannot be null).

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Could not remove resource [{}]", _name));
        }
    }

    auto add_child_resource(
        RxComm& _comm,
        const resource_name_type _parent,
        const resource_name_type _child,
        const std::string_view _context_string) -> void
    {
        generalAdminInp_t input{};
        input.arg0 = "add";
        input.arg1 = "childtoresc";
        input.arg2 = _parent.data();
        input.arg3 = _child.data();
        input.arg4 = _context_string.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Could not add child resource [{}] to resource [{}]", _child, _parent));
        }
    }

    auto remove_child_resource(RxComm& _comm, const resource_name_type _parent, const resource_name_type _child) -> void
    {
        generalAdminInp_t input{};
        input.arg0 = "rm";
        input.arg1 = "childfromresc";
        input.arg2 = _parent.data();
        input.arg3 = _child.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Could not remove child resource [{}] from resource [{}]", _child, _parent));
        }
    }

    auto resource_exists(RxComm& _comm, const resource_name_type _name) -> bool
    {
        try {
            const auto q = gq2::builder::select({"RESC_ID"})
                               .where(col("RESC_NAME") == _name)
                               .limit(1)
                               .build();

            return !gq2::execute(_comm, q).empty();
        }
        catch (const std::exception& e) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_LIBRARY_ERROR, e.what());
        }
        catch (...) {
            constexpr char* msg = "An unknown error occurred while verifying existence of resource [{}]";
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_UNKNOWN_ERROR, fmt::format(msg, _name));
        }
    }

    auto resource_info(RxComm& _comm, const resource_name_type _name) -> std::optional<class resource_info>
    {
        try {
            const auto q = gq2::builder::select({
                               "RESC_ID",
                               "RESC_TYPE_NAME",
                               "RESC_ZONE_NAME",
                               "RESC_LOC",
                               "RESC_VAULT_PATH",
                               "RESC_STATUS",
                               "RESC_CONTEXT",
                               "RESC_COMMENT",
                               "RESC_INFO",
                               "RESC_FREE_SPACE",
                               "RESC_FREE_SPACE_TIME",
                               "RESC_PARENT",
                               "RESC_CREATE_TIME",
                               "RESC_MODIFY_TIME",
                               "RESC_MODIFY_TIME_MILLIS",
                               "RESC_PARENT_CONTEXT"})
                               .where(col("RESC_NAME") == _name)
                               .limit(1)
                               .build();

            const auto rows = gq2::execute(_comm, q);
            if (!rows.empty()) {
                const auto& row = rows.front();
                class resource_info info;
                info.name_ = std::string{_name};

                // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
                info.id_ = row.at(0).get<std::string>();
                info.type_ = row.at(1).get<std::string>();
                info.zone_name_ = row.at(2).get<std::string>();
                info.host_name_ = row.at(3).get<std::string>();
                info.vault_path_ = row.at(4).get<std::string>();
                info.status_ = to_resource_status(row.at(5).get_ref<const std::string&>());
                info.context_string_ = row.at(6).get<std::string>();
                info.parent_context_string_ = row.at(15).get<std::string>();
                info.comments_ = row.at(7).get<std::string>();
                info.info_ = row.at(8).get<std::string>();
                info.free_space_ = row.at(9).get<std::string>();
                info.parent_id_ = row.at(11).get<std::string>();
                info.ctime_ = resource_time_type{std::chrono::seconds{parse_u64(row.at(12).get_ref<const std::string&>())}};
                info.mtime_ = resource_time_type{std::chrono::seconds{parse_u64(row.at(13).get_ref<const std::string&>())}};
                info.mtime_millis_ = std::chrono::milliseconds{parse_u64(row.at(14).get_ref<const std::string&>())};

                const auto& fs_time_str = row.at(10).get_ref<const std::string&>();
                if (!fs_time_str.empty()) {
                    info.free_space_time_ = resource_time_type{std::chrono::seconds{parse_u64(fs_time_str)}};
                }
                // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)

                return info;
            }

            return std::nullopt;
        }
        catch (const std::exception& e) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_LIBRARY_ERROR, e.what());
        }
        catch (...) {
            constexpr char* msg = "An unknown error occurred while retrieving information about resource [{}]";
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_UNKNOWN_ERROR, fmt::format(msg, _name));
        }
    }

    auto rebalance_resource(RxComm& _comm, const resource_name_type _name) -> void
    {
        generalAdminInp_t input{};
        input.arg0 = "modify";
        input.arg1 = "resource";
        input.arg2 = _name.data();
        input.arg3 = "rebalance";
        input.arg4 = ""; // Required to avoid segfaults in server.

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Encountered rebalance error for resource [{}]", _name));
        }
    }

    auto resource_name(RxComm& _comm, const resource_id_type _id) -> std::optional<std::string>
    {
        try {
            const auto q = gq2::builder::select({"RESC_NAME"})
                               .where(col("RESC_ID") == _id)
                               .limit(1)
                               .build();

            const auto rows = gq2::execute(_comm, q);
            if (!rows.empty()) {
                return rows.front().at(0).get<std::string>();
            }

            return std::nullopt;
        }
        catch (const std::exception& e) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_LIBRARY_ERROR, e.what());
        }
        catch (...) {
            constexpr char* msg = "An unknown error occurred while retrieving resource name with id [{}]";
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_UNKNOWN_ERROR, fmt::format(msg, _id));
        }
    }
} // namespace irods::experimental::administration::NAMESPACE_IMPL

