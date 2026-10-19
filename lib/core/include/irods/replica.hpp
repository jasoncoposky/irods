#ifndef IRODS_REPLICA_HPP
#define IRODS_REPLICA_HPP

/// \file

#ifdef IRODS_REPLICA_ENABLE_SERVER_SIDE_API
    #define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
    #define IRODS_QUERY_ENABLE_SERVER_SIDE_API

    #include "irods/rsDataObjChksum.hpp"
    #include "irods/rsModDataObjMeta.hpp"
    #include "irods/rsFileStat.hpp"
    #include "irods/rs_genquery2.hpp"

    #define rxFileStat rsFileStat
#else
    #undef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
    #undef IRODS_QUERY_ENABLE_SERVER_SIDE_API

    #include "irods/dataObjChksum.h"
    #include "irods/modDataObjMeta.h"
    #include "irods/fileStat.h"

    #define rxFileStat rcFileStat
#endif

#include "irods/escape_utilities.hpp"
#include "irods/filesystem.hpp"
#include "irods/filesystem/path_utilities.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/irods_exception.hpp"
#include "irods/key_value_proxy.hpp"
#include "irods/objInfo.h"
#include "irods/query_builder.hpp"
#include "irods/rcConnect.h"
#include "irods/rcMisc.h"

#include <fmt/format.h>

#include <charconv>
#include <chrono>
#include <optional>
#include <string_view>
#include <variant>

struct DataObjInfo;

namespace irods::experimental::replica
{
    using replica_number_type = int;

    using leaf_resource_name_type = std::string_view;

    using query_value_type = std::vector<std::vector<std::string>>;

    /// \brief Describes whether the catalog should be updated when calculating a replica's checksum
    ///
    /// \since 4.2.9
    enum class verification_calculation
    {
        if_empty,
        always
    };

    /// \brief GenQuery columns which represent a replica
    ///
    /// \since 4.2.9
    enum genquery_column_index : std::size_t
    {
        DATA_ID,
        DATA_COLL_ID,
        DATA_NAME,
        DATA_REPL_NUM,
        DATA_VERSION,
        DATA_TYPE_NAME,
        DATA_SIZE,
        DATA_RESC_NAME,
        DATA_PATH,
        DATA_OWNER_NAME,
        DATA_OWNER_ZONE,
        DATA_REPL_STATUS,
        DATA_STATUS,
        DATA_CHECKSUM,
        DATA_EXPIRY,
        DATA_MAP_ID,
        DATA_COMMENTS,
        DATA_ACCESS_TIME,
        DATA_CREATE_TIME,
        DATA_MODIFY_TIME,
        DATA_MODE,
        DATA_RESC_HIER,
        DATA_RESC_ID,
        COLL_NAME
    };

    namespace detail
    {
        inline auto replica_columns() -> std::vector<std::string>
        {
            return {
                "DATA_ID",
                "DATA_COLL_ID",
                "DATA_NAME",
                "DATA_REPL_NUM",
                "DATA_VERSION",
                "DATA_TYPE_NAME",
                "DATA_SIZE",
                "DATA_RESC_NAME",
                "DATA_PATH",
                "DATA_OWNER_NAME",
                "DATA_OWNER_ZONE",
                "DATA_REPL_STATUS",
                "DATA_STATUS",
                "DATA_CHECKSUM",
                "DATA_EXPIRY",
                "DATA_MAP_ID",
                "DATA_COMMENTS",
                "DATA_ACCESS_TIME",
                "DATA_CREATE_TIME",
                "DATA_MODIFY_TIME",
                "DATA_MODE",
                "DATA_RESC_HIER",
                "DATA_RESC_ID",
                "COLL_NAME"
            };
        }

        /// \param[in] _comm connection object
        /// \param[in] _logical_path
        ///
        /// \throws irods::exception if the path does not refer to a data object
        ///
        /// \since 4.2.9
        template<typename rxComm>
        inline auto throw_if_path_is_not_a_data_object(
            rxComm& _comm,
            const irods::experimental::filesystem::path& _logical_path) -> void
        {
            namespace fs = irods::experimental::filesystem;
            const auto s = fs::NAMESPACE_IMPL::status(_comm, _logical_path, fs::status_options::basic);
            if (!fs::NAMESPACE_IMPL::is_data_object(s)) {
                THROW(SYS_INVALID_INPUT_PARAM, "path does not point to a data object");
            }
        } // throw_if_path_is_not_a_data_object

        /// \brief Enforce valid replica number for replica operation
        ///
        /// \param[in] _replica_number
        ///
        /// \throws irods::exception if the path does not refer to a data object
        ///
        /// \since 4.2.9
        inline auto throw_if_replica_number_is_invalid(const replica_number_type _replica_number) -> void
        {
            if (_replica_number < 0) {
                THROW(SYS_INVALID_INPUT_PARAM, "invalid replica number");
            }
        } // throw_if_replica_number_is_invalid

        /// \brief Enforce valid path for replica operation
        ///
        /// \param[in] _comm connection object
        /// \param[in] _logical_path
        ///
        /// \throws filesystem_error if the path is empty or too long
        /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
        ///
        /// \since 4.2.9
        template<typename rxComm>
        inline auto throw_if_replica_logical_path_is_invalid(
            rxComm& _comm,
            const irods::experimental::filesystem::path& _logical_path) -> void
        {
            throw_if_path_is_empty(_logical_path);

            throw_if_path_length_exceeds_limit(_logical_path);

            throw_if_path_is_not_a_data_object(_comm, _logical_path);
        } // throw_if_replica_logical_path_is_invalid

        /// \brief Enforce valid resource name for replica operation
        ///
        /// \param[in] _resource_name
        ///
        /// \throws irods::exception If the provided resource name is invalid
        ///
        /// \since 4.2.9
        inline auto throw_if_replica_resource_name_is_invalid(const leaf_resource_name_type& _resource_name) -> void
        {
            if (_resource_name.size() <= 0) {
                THROW(SYS_INVALID_INPUT_PARAM, "resource name cannot be empty");
            }
        } // throw_if_replica_resource_name_is_invalid

        /// \brief Gets a row from r_data_main using GenQuery2 AST
        ///
        /// \param[in] _comm connection object
        /// \param[in] _logical_path
        /// \param[in] _extra_condition Additional AST condition for selection of replica information
        ///
        /// \throws irods::exception If no replica information is found or query fails
        ///
        /// \retval Set of results from the query
        ///
        /// \since 4.2.9
        template<typename rxComm>
        auto get_data_object_info_impl(
            rxComm& _comm,
            const irods::experimental::filesystem::path& _logical_path,
            std::optional<irods::experimental::genquery2::builder::condition_builder> _extra_condition = std::nullopt) -> query_value_type
        {
            namespace gq2 = irods::experimental::genquery2;
            using gq2::builder::col;

            detail::throw_if_replica_logical_path_is_invalid(_comm, _logical_path);

            auto cond = (col("DATA_NAME") == _logical_path.object_name_view() &&
                         col("COLL_NAME") == _logical_path.parent_path_view());
            if (_extra_condition) {
                cond = ( std::move(cond) && std::move(*_extra_condition) );
            }

            const auto q = gq2::builder::select(replica_columns())
                               .where(std::move(cond))
                               .build();

            const auto zone = irods::experimental::filesystem::zone_name(_logical_path);
            const auto rows = gq2::execute(_comm, q, zone ? std::string_view{*zone} : std::string_view{});
            if (rows.empty()) {
                THROW(CAT_NO_ROWS_FOUND, "no replica information found");
            }

            query_value_type output;
            output.reserve(rows.size());
            for (const auto& row : rows) {
                std::vector<std::string> r;
                r.reserve(row.size());
                for (const auto& cell : row) {
                    r.push_back(cell.template get<std::string>());
                }
                output.push_back(std::move(r));
            }

            return output;
        } // get_data_object_info_impl

        /// \brief Calculate checksum for a replica
        ///
        /// The verification_calculation helps determine whether the calculated checksum will be registered in the catalog
        ///
        /// \param[in] _comm connection object
        /// \param[in] _data_object_input
        /// \param[in] _logical_path
        /// \param[in] _calculation mode for catalog update
        ///
        /// \throws filesystem_error if the path is empty or too long
        /// \throws irods::exception if the path does not refer to a data object or replica input is invalid
        ///
        /// \returns std::string
        /// \retval value of calculated checksum
        ///
        /// \since 4.2.9
        template<typename rxComm>
        auto replica_checksum_impl(
            rxComm& _comm,
            dataObjInp_t& _data_object_input,
            const irods::experimental::filesystem::path& _logical_path,
            const verification_calculation _calculation = verification_calculation::if_empty) -> std::string
        {
            detail::throw_if_replica_logical_path_is_invalid(_comm, _logical_path);

            std::snprintf(_data_object_input.objPath, sizeof(_data_object_input.objPath), "%s", _logical_path.c_str());

            if (verification_calculation::always == _calculation) {
                auto cond_input = irods::experimental::make_key_value_proxy(_data_object_input.condInput);
                cond_input[FORCE_CHKSUM_KW] = "";
            }

            char* checksum{};
            at_scope_exit free_checksum{[&checksum] {
                if (checksum) {
                    std::free(checksum);
                }
            }};

            if constexpr (std::is_same_v<rxComm, rsComm_t>) {
                if (const auto ec = rsDataObjChksum(&_comm, &_data_object_input, &checksum); ec < 0) {
                    THROW(ec, fmt::format("cannot calculate checksum for [{}]", _logical_path.c_str()));
                }
            }
            else {
                if (const auto ec = rcDataObjChksum(&_comm, &_data_object_input, &checksum); ec < 0) {
                    THROW(ec, fmt::format("cannot calculate checksum for [{}]", _logical_path.c_str()));
                }
            }

            return checksum ? checksum : std::string{};
        } // replica_checksum_impl

        /// \brief Sets value of the timestamp of last time this replica was written to
        ///
        /// \param[in] _comm connection object
        /// \param[in] _logical_path
        /// \param[in] _resource_hierarchy
        /// \param[in] _new_time timestamp to use as last_write_time
        ///
        /// \throws irods::filesystem::filesystem_error if the path is empty or too long
        /// \throws irods::exception if the path does not refer to a data object or replica input is invalid
        ///
        /// \since 4.2.9
        template<typename rxComm>
        auto last_write_time_impl(
            rxComm& _comm,
            const irods::experimental::filesystem::path& _logical_path,
            std::string_view _resource_hierarchy,
            const irods::experimental::filesystem::object_time_type _new_time) -> void
        {
            const auto seconds = _new_time.time_since_epoch();
            char time_buf[32]{};
            const auto [end_ptr, _] = fmt::format_to_n(time_buf, sizeof(time_buf) - 1, "{:011d}", seconds.count());
            *end_ptr = '\0';

            DataObjInfo info{};
            std::snprintf(info.objPath, sizeof(info.objPath), "%s", _logical_path.c_str());
            std::snprintf(info.rescHier, sizeof(info.rescHier), "%s", _resource_hierarchy.data());

            keyValPair_t kvp{};
            at_scope_exit free_memory{[&kvp] { clearKeyVal(&kvp); }};
            auto reg_params = make_key_value_proxy(kvp);
            reg_params[DATA_MODIFY_KW] = time_buf;

            modDataObjMeta_t input{};
            input.dataObjInfo = &info;
            input.regParam = reg_params.get();

            const auto mod_obj_info_fcn = [](rxComm& _comm, modDataObjMeta_t& _inp)
            {
                if constexpr (std::is_same_v<rxComm, rsComm_t>) {
                    return rsModDataObjMeta(&_comm, &_inp);
                }
                else {
                    return rcModDataObjMeta(&_comm, &_inp);
                }
            };

            if (const auto ec = mod_obj_info_fcn(_comm, input); ec != 0) {
                THROW(ec, fmt::format("cannot set mtime for [{}]", _logical_path.c_str()));
            }
        } // last_write_time_impl

        /// \brief Gets size of the replica's physical representation from storage
        ///
        /// \param[in] _comm connection object
        /// \param[in] _logical_path
        /// \param[in] _resource_hierarchy
        /// \param[in] _physical_path
        ///
        /// \throws irods::filesystem::filesystem_error if the path is empty or too long
        /// \throws irods::exception if the path does not refer to a data object or replica input is invalid
        ///
        /// \since 4.2.9
        template<typename rxComm>
        auto get_replica_size_from_storage_impl(
            rxComm& _comm,
            const irods::experimental::filesystem::path& _logical_path,
            std::string_view _resource_hierarchy,
            std::string_view _physical_path) -> rodsLong_t
        {
            fileStatInp_t stat_inp{};
            rstrcpy(stat_inp.objPath,  _logical_path.c_str(),      sizeof(stat_inp.objPath));
            rstrcpy(stat_inp.rescHier, _resource_hierarchy.data(), sizeof(stat_inp.rescHier));
            rstrcpy(stat_inp.fileName, _physical_path.data(),      sizeof(stat_inp.fileName));

            rodsStat *stat_out{};
            irods::at_scope_exit free_stat_out{[&stat_out] { free(stat_out); }};

            if (const auto status_rxFileStat = rxFileStat(&_comm, &stat_inp, &stat_out); status_rxFileStat < 0) {
                THROW(status_rxFileStat, fmt::format(
                    "rxFileStat of objPath [{}] rescHier [{}] fileName [{}] failed with [{}]",
                    stat_inp.objPath, stat_inp.rescHier, stat_inp.fileName, status_rxFileStat));
            }

            return stat_out->st_size;
        } // get_replica_size_from_storage_impl
    } // namespace detail

    /// \brief Gets a row from r_data_main using GenQuery2
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws irods::exception If no replica information is found or query fails
    ///
    /// \retval Set of results from the query
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto get_data_object_info(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number) -> query_value_type
    {
        using irods::experimental::genquery2::builder::col;

        detail::throw_if_replica_number_is_invalid(_replica_number);

        return detail::get_data_object_info_impl(
            _comm, _logical_path, col("DATA_REPL_NUM") == std::to_string(_replica_number));
    } // get_data_object_info

    /// \brief Gets a row from r_data_main using GenQuery2
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws irods::exception If no replica information is found or query fails
    ///
    /// \retval Set of results from the query
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto get_data_object_info(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> query_value_type
    {
        using irods::experimental::genquery2::builder::col;

        detail::throw_if_replica_resource_name_is_invalid(_leaf_resource_name);

        return detail::get_data_object_info_impl(
            _comm, _logical_path, col("DATA_RESC_NAME") == _leaf_resource_name);
    } // get_data_object_info

    /// \brief Gets a row from r_data_main using GenQuery2
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    ///
    /// \throws irods::exception If no replica information is found or query fails
    ///
    /// \retval Set of results from the query
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto get_data_object_info(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path) -> query_value_type
    {
        return detail::get_data_object_info_impl(_comm, _logical_path);
    } // get_data_object_info

    /// \brief Gets a row from r_data_main using GenQuery2
    ///
    /// \param[in] _comm connection object
    /// \param[in] _data_id
    ///
    /// \throws irods::exception If no replica information is found or query fails
    ///
    /// \retval Set of results from the query
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto get_data_object_info(
        rxComm& _comm,
        const rodsLong_t _data_id) -> query_value_type
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto q = gq2::builder::select(detail::replica_columns())
                           .where(col("DATA_ID") == std::to_string(_data_id))
                           .build();

        const auto rows = gq2::execute(_comm, q);
        if (rows.empty()) {
            THROW(CAT_NO_ROWS_FOUND, "no replica information found");
        }

        query_value_type output;
        output.reserve(rows.size());
        for (const auto& row : rows) {
            std::vector<std::string> r;
            r.reserve(row.size());
            for (const auto& cell : row) {
                r.push_back(cell.template get<std::string>());
            }
            output.push_back(std::move(r));
        }

        return output;
    } // get_data_object_info

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
    ///
    /// \returns std::uintmax_t
    /// \retval size of specified replica in bytes
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_size(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number) -> std::uintmax_t
    {
        const auto result = get_data_object_info(_comm, _logical_path, _replica_number).front();

        const std::string_view size = result[genquery_column_index::DATA_SIZE];
        std::uintmax_t val = 0;
        std::from_chars(size.data(), size.data() + size.size(), val);

        return val;
    } // replica_size

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or leaf resource is invalid
    ///
    /// \returns std::uintmax_t
    /// \retval size of specified replica in bytes
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_size(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> std::uintmax_t
    {
        const auto result = get_data_object_info(_comm, _logical_path, _leaf_resource_name).front();

        const std::string_view size = result[genquery_column_index::DATA_SIZE];
        std::uintmax_t val = 0;
        std::from_chars(size.data(), size.data() + size.size(), val);

        return val;
    } // replica_size

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
    ///
    /// \returns bool
    /// \retval true if replica size is 0; otherwise, false
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto is_replica_empty(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number) -> bool
    {
        return replica_size(_comm, _logical_path, _replica_number) == 0;
    } // is_replica_empty

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or leaf resource is invalid
    ///
    /// \returns bool
    /// \retval true if replica size is 0; otherwise, false
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto is_replica_empty(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> bool
    {
        return replica_size(_comm, _logical_path, _leaf_resource_name) == 0;
    } // is_replica_empty

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
    ///
    /// \returns std::string
    /// \retval value of calculated checksum
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_checksum(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number,
        const verification_calculation _calculation = verification_calculation::if_empty) -> std::string
    {
        detail::throw_if_replica_number_is_invalid(_replica_number);

        dataObjInp_t input{};
        at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};
        auto cond_input = make_key_value_proxy(input.condInput);
        cond_input[REPL_NUM_KW] = std::to_string(_replica_number);

        return detail::replica_checksum_impl(_comm, input, _logical_path, _calculation);
    } // replica_checksum

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or resource is invalid
    ///
    /// \returns std::string
    /// \retval value of calculated checksum
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_checksum(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name,
        const verification_calculation _calculation = verification_calculation::if_empty) -> std::string
    {
        detail::throw_if_replica_resource_name_is_invalid(_leaf_resource_name);

        dataObjInp_t input{};
        at_scope_exit free_memory{[&input] { clearKeyVal(&input.condInput); }};
        auto cond_input = make_key_value_proxy(input.condInput);
        cond_input[LEAF_RESOURCE_NAME_KW] = _leaf_resource_name;

        return detail::replica_checksum_impl(_comm, input, _logical_path, _calculation);
    } // replica_checksum

    /// \brief Returns timestamp of last time this replica was written to
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
    ///
    /// \returns irods::filesystem::object_time_type
    /// \retval Timestamp of the last time the replica was written to
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto last_write_time(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number) -> irods::experimental::filesystem::object_time_type
    {
        using object_time_type = irods::experimental::filesystem::object_time_type;

        const auto result = get_data_object_info(_comm, _logical_path, _replica_number).front();

        const std::string_view mtime = result[genquery_column_index::DATA_MODIFY_TIME];
        std::uint64_t val = 0;
        std::from_chars(mtime.data(), mtime.data() + mtime.size(), val);

        return object_time_type{std::chrono::seconds{val}};
    } // last_write_time

    /// \brief Returns timestamp of last time this replica was written to
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or leaf resource is invalid
    ///
    /// \returns irods::filesystem::object_time_type
    /// \retval Timestamp of the last time the replica was written to
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto last_write_time(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> irods::experimental::filesystem::object_time_type
    {
        using object_time_type = irods::experimental::filesystem::object_time_type;

        const auto result = get_data_object_info(_comm, _logical_path, _leaf_resource_name).front();

        const std::string_view mtime = result[genquery_column_index::DATA_MODIFY_TIME];
        std::uint64_t val = 0;
        std::from_chars(mtime.data(), mtime.data() + mtime.size(), val);

        return object_time_type{std::chrono::seconds{val}};
    } // last_write_time

    /// \brief Sets value of the timestamp of last time this replica was written to
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    /// \param[in] _new_time timestamp to use as last_write_time
    ///
    /// \throws irods::filesystem::filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto last_write_time(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number,
        const irods::experimental::filesystem::object_time_type _new_time) -> void
    {
        const auto replica_info = get_data_object_info(_comm, _logical_path, _replica_number).front();

        std::string_view resource_hierarchy = replica_info[genquery_column_index::DATA_RESC_HIER];

        return detail::last_write_time_impl(_comm, _logical_path, resource_hierarchy, _new_time);
    } // last_write_time

    /// \brief Sets value of the timestamp of last time this replica was written to
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    /// \param[in] _new_time timestamp to use as last_write_time
    ///
    /// \throws irods::filesystem::filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or leaf resource is invalid
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto last_write_time(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name,
        const irods::experimental::filesystem::object_time_type _new_time) -> void
    {
        const auto replica_info = get_data_object_info(_comm, _logical_path, _leaf_resource_name).front();

        std::string_view resource_hierarchy = replica_info[genquery_column_index::DATA_RESC_HIER];

        return detail::last_write_time_impl(_comm, _logical_path, resource_hierarchy, _new_time);
    } // last_write_time

    /// \brief Get the leaf resource name for a replica based on the provided replica number.
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws irods::exception If query fails
    ///
    /// \retval std::string containing the resource name which hosts the specified replica
    /// \retval std::nullopt if no replica by the specified replica number is found
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto to_leaf_resource(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number) -> std::optional<std::string>
    {
        try {
            const auto replica_info = get_data_object_info(_comm, _logical_path, _replica_number).front();

            return replica_info[genquery_column_index::DATA_RESC_NAME];
        }
        catch (const irods::exception& e) {
            if (CAT_NO_ROWS_FOUND == e.code()) {
                return std::nullopt;
            }
            throw;
        }
    } // to_leaf_resource

    /// \brief Get the replica number for a replica based on the provided leaf resource name.
    ///
    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws irods::exception If query fails
    ///
    /// \retval std::string containing the resource name which hosts the specified replica
    /// \retval std::nullopt if no replica by the specified leaf resource name is found
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto to_replica_number(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> std::optional<replica_number_type>
    {
        try {
            const auto replica_info = get_data_object_info(_comm, _logical_path, _leaf_resource_name).front();

            const std::string_view repl_num = replica_info[genquery_column_index::DATA_REPL_NUM];
            replica_number_type val = 0;
            std::from_chars(repl_num.data(), repl_num.data() + repl_num.size(), val);
            return val;
        }
        catch (const irods::exception& e) {
            if (CAT_NO_ROWS_FOUND == e.code()) {
                return std::nullopt;
            }
            throw;
        }
    } // to_replica_number

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws irods::exception If query fails
    ///
    /// \retval true if query returns results
    /// \retval false if query returns no results
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_exists(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> bool
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        detail::throw_if_replica_logical_path_is_invalid(_comm, _logical_path);
        detail::throw_if_replica_resource_name_is_invalid(_leaf_resource_name);

        const auto q = gq2::builder::select({"DATA_ID"})
                           .where(col("DATA_NAME") == _logical_path.object_name_view() &&
                                  col("COLL_NAME") == _logical_path.parent_path_view() &&
                                  col("DATA_RESC_NAME") == _leaf_resource_name)
                           .limit(1)
                           .build();

        const auto zone = irods::experimental::filesystem::zone_name(_logical_path);
        const auto rows = gq2::execute(_comm, q, zone ? std::string_view{*zone} : std::string_view{});
        return !rows.empty();
    } // replica_exists

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws irods::exception If query fails
    ///
    /// \retval true if query returns results
    /// \retval false if query returns no results
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_exists(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type& _replica_number) -> bool
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        detail::throw_if_replica_logical_path_is_invalid(_comm, _logical_path);
        detail::throw_if_replica_number_is_invalid(_replica_number);

        const auto q = gq2::builder::select({"DATA_ID"})
                           .where(col("DATA_NAME") == _logical_path.object_name_view() &&
                                  col("COLL_NAME") == _logical_path.parent_path_view() &&
                                  col("DATA_REPL_NUM") == std::to_string(_replica_number))
                           .limit(1)
                           .build();

        const auto zone = irods::experimental::filesystem::zone_name(_logical_path);
        const auto rows = gq2::execute(_comm, q, zone ? std::string_view{*zone} : std::string_view{});
        return !rows.empty();
    } // replica_exists

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
    ///
    /// \returns int
    /// \retval replica status
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_status(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number) -> int
    {
        const auto result = get_data_object_info(_comm, _logical_path, _replica_number).front();

        const std::string_view status = result[genquery_column_index::DATA_REPL_STATUS];
        int val = 0;
        std::from_chars(status.data(), status.data() + status.size(), val);

        return val;
    } // replica_status

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or leaf resource is invalid
    ///
    /// \returns int
    /// \retval replica status
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto replica_status(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> int
    {
        const auto result = get_data_object_info(_comm, _logical_path, _leaf_resource_name).front();

        const std::string_view status = result[genquery_column_index::DATA_REPL_STATUS];
        int val = 0;
        std::from_chars(status.data(), status.data() + status.size(), val);

        return val;
    } // replica_status

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _replica_number
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or replica number is invalid
    ///
    /// \returns std::uintmax_t
    /// \retval size of the data in storage which specified replica represents
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto get_replica_size_from_storage(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const replica_number_type _replica_number) -> rodsLong_t
    {
        const auto result = get_data_object_info(_comm, _logical_path, _replica_number).front();

        std::string_view hierarchy = result[genquery_column_index::DATA_RESC_HIER];
        std::string_view physical_path = result[genquery_column_index::DATA_PATH];

        return detail::get_replica_size_from_storage_impl(_comm, _logical_path, hierarchy, physical_path);
    } // get_replica_size_from_storage

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _leaf_resource_name
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or leaf resource is invalid
    ///
    /// \returns std::uintmax_t
    /// \retval size of the data in storage which specified replica represents
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto get_replica_size_from_storage(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        const leaf_resource_name_type& _leaf_resource_name) -> rodsLong_t
    {
        const auto result = get_data_object_info(_comm, _logical_path, _leaf_resource_name).front();

        std::string_view hierarchy = result[genquery_column_index::DATA_RESC_HIER];
        std::string_view physical_path = result[genquery_column_index::DATA_PATH];

        return detail::get_replica_size_from_storage_impl(_comm, _logical_path, hierarchy, physical_path);
    } // get_replica_size_from_storage

    /// \param[in] _comm connection object
    /// \param[in] _logical_path
    /// \param[in] _resource_hierarchy
    /// \param[in] _physical_path
    ///
    /// \throws filesystem_error if the path is empty or too long
    /// \throws irods::exception if the path does not refer to a data object or leaf resource is invalid
    ///
    /// \returns std::uintmax_t
    /// \retval size of the data in storage which specified replica represents
    ///
    /// \since 4.2.9
    template<typename rxComm>
    auto get_replica_size_from_storage(
        rxComm& _comm,
        const irods::experimental::filesystem::path& _logical_path,
        std::string_view _resource_hierarchy,
        std::string_view _physical_path) -> rodsLong_t
    {
        return detail::get_replica_size_from_storage_impl(_comm, _logical_path, _resource_hierarchy, _physical_path);
    } // get_replica_size_from_storage

} // namespace irods::experimental::replica

#endif // #ifndef IRODS_REPLICA_HPP
