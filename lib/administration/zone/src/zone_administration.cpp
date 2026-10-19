#include "irods/zone_administration.hpp"

#include "irods/genquery2_builder.hpp"

#include <charconv>
#include <system_error>

#ifdef IRODS_ZONE_ADMINISTRATION_ENABLE_SERVER_SIDE_API
#  include "irods/rs_genquery2.hpp"
// The server maintains a list of the zones known to the system.
// The library takes advantage of this information where possible to avoid
// unnecessary network and database operations.
extern zoneInfo* ZoneInfoHead; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
#endif // IRODS_ZONE_ADMINISTRATION_ENABLE_SERVER_SIDE_API

namespace irods::experimental::administration::NAMESPACE_IMPL
{
    auto add_zone(RxComm& _comm, const std::string_view _zone_name, const zone_options& _opts) -> void
    {
        if (_zone_name.empty()) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_INVALID_INPUT_PARAM, "Invalid zone name: cannot be empty");
        }

        generalAdminInp_t input{};
        input.arg0 = "add";
        input.arg1 = "zone";
        input.arg2 = _zone_name.data();
        input.arg3 = "remote";
        input.arg4 = _opts.connection_info.data();
        input.arg5 = _opts.comment.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to add zone [{}].", _zone_name));
        }
    } // add_zone

    auto remove_zone(RxComm& _comm, const std::string_view _zone_name) -> void
    {
        if (_zone_name.empty()) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_INVALID_INPUT_PARAM, "Invalid zone name: cannot be empty");
        }

        generalAdminInp_t input{};
        input.arg0 = "rm";
        input.arg1 = "zone";
        input.arg2 = _zone_name.data();

        if (const auto ec = rxGeneralAdmin(&_comm, &input); ec < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, fmt::format("Failed to remove zone [{}].", _zone_name));
        }
    } // remove_zone

    auto zone_exists([[maybe_unused]] RxComm& _comm, const std::string_view _zone_name) -> bool
    {
        if (_zone_name.empty()) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_INVALID_INPUT_PARAM, "Invalid zone name: cannot be empty");
        }

#ifdef IRODS_ZONE_ADMINISTRATION_ENABLE_SERVER_SIDE_API
        for (const auto* p = ZoneInfoHead; p; p = p->next) { // NOLINT(readability-implicit-bool-conversion)
            if (_zone_name == p->zoneName) { // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                return true;
            }
        }
#else
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto q = gq2::builder::select({"ZONE_NAME"})
                           .where(col("ZONE_NAME") == _zone_name)
                           .limit(1)
                           .build();

        return !gq2::execute(_comm, q).empty();
#endif // IRODS_ZONE_ADMINISTRATION_ENABLE_SERVER_SIDE_API

        return false;
    } // zone_exists

    auto zone_info(RxComm& _comm, const std::string_view _zone_name) -> std::optional<struct zone_info>
    {
        if (_zone_name.empty()) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_INVALID_INPUT_PARAM, "Invalid zone name: cannot be empty");
        }

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto q = gq2::builder::select({"ZONE_ID", "ZONE_TYPE", "ZONE_CONNECTION", "ZONE_COMMENT"})
                           .where(col("ZONE_NAME") == _zone_name)
                           .limit(1)
                           .build();

        const auto rows = gq2::execute(_comm, q);
        if (!rows.empty()) {
            const auto& row = rows.front();
            // Set the zone id member variable to -1 if its value exceeds the representable range of a
            // signed 32-bit integer.
            int zone_id = -1;
            const auto& id_str = row.at(0).get_ref<const std::string&>();
            const auto* b = id_str.c_str();
            const auto* e = b + id_str.size(); // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            auto [ptr, ec] = std::from_chars(b, e, zone_id);
            if (std::errc{} != ec || e != ptr) {
                zone_id = -1;
            }

            // clang-format off
            return administration::zone_info{
                .name{_zone_name},
                .connection_info = row.at(2).get<std::string>(),
                .comment = row.at(3).get<std::string>(),
                .id = zone_id,
                .type = (row.at(1).get_ref<const std::string&>() == "local") ? zone_type::local : zone_type::remote
            };
            // clang-format on
        }

        return std::nullopt;
    } // zone_info
} // namespace irods::experimental::administration::NAMESPACE_IMPL

