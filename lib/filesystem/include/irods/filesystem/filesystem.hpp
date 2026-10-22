#undef IRODS_FILESYSTEM_FILESYSTEM_HPP_INCLUDE_HEADER

#if defined(IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API)
#  if !defined(IRODS_FILESYSTEM_FILESYSTEM_HPP_FOR_SERVER)
#    define IRODS_FILESYSTEM_FILESYSTEM_HPP_FOR_SERVER
#    define IRODS_FILESYSTEM_FILESYSTEM_HPP_INCLUDE_HEADER
#  endif
#elif !defined(IRODS_FILESYSTEM_FILESYSTEM_HPP_FOR_CLIENT)
#  define IRODS_FILESYSTEM_FILESYSTEM_HPP_FOR_CLIENT
#  define IRODS_FILESYSTEM_FILESYSTEM_HPP_INCLUDE_HEADER
#endif

#ifdef IRODS_FILESYSTEM_FILESYSTEM_HPP_INCLUDE_HEADER

#include "irods/filesystem/config.hpp"
#include "irods/filesystem/object_status.hpp"
#include "irods/filesystem/permissions.hpp"
#include "irods/filesystem/copy_options.hpp"
#include "irods/filesystem/filesystem_error.hpp"
#include "irods/filesystem/path_utilities.hpp"

#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#  include "irods/rs_atomic_apply_metadata_operations.hpp"
#else
#  include "irods/atomic_apply_metadata_operations.h"
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

#include "irods/irods_at_scope_exit.hpp"
#include "irods/system_error.hpp"
#include "irods/rodsErrorTable.h"

#include <nlohmann/json.hpp>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <chrono>
#include <vector>
#include <variant>
#include <type_traits>
#include <algorithm>
#include <system_error>

/// Provides various facilities for performing logical operations on data objects and collections.
///
/// \since 4.2.6
namespace irods::filesystem
{
    class path;

    using object_time_type = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>;

#ifndef IRODS_FILESYSTEM_FILESYSTEM_COMMON_TYPES_AND_OBJECTS
#define IRODS_FILESYSTEM_FILESYSTEM_COMMON_TYPES_AND_OBJECTS
    //
    // The data types and objects defined in this space represent entities that
    // are shared across the client-side and server-side interfaces.
    //
    // These symbols MUST NOT be introduced into the space more than once!
    //

    enum class remove_options
    {
        none,
        no_trash
    }; // enum class remove_options

    struct extended_remove_options
    {
        bool no_trash = false;
        bool verbose = false;
        bool progress = false;
        bool recursive = false;
        bool unregister = false;
    }; // struct extended_remove_options

    struct metadata
    {
        std::string attribute;
        std::string value;
        std::string units;
    }; // struct metadata

    // clang-format off
    /// A tag type used to instruct an operation to run in administrator mode.
    const inline struct admin_tag {} admin;
    // clang-format on
#endif // IRODS_FILESYSTEM_FILESYSTEM_COMMON_TYPES_AND_OBJECTS

    namespace NAMESPACE_IMPL
    {
        // Operational functions

        auto copy(rxComm& _comm, const path& _from, const path& _to, copy_options _options = copy_options::none) -> void;
        auto copy(rxComm& _comm, const path& _from, const path& _to, std::error_code& _ec) -> void;
        auto copy(rxComm& _comm, const path& _from, const path& _to, copy_options _options, std::error_code& _ec) -> void;

        auto copy_data_object(rxComm& _comm, const path& _from, const path& _to, copy_options _options = copy_options::none) -> bool;
        auto copy_data_object(rxComm& _comm, const path& _from, const path& _to, std::error_code& _ec) -> bool;
        auto copy_data_object(rxComm& _comm, const path& _from, const path& _to, copy_options _options, std::error_code& _ec) -> bool;

        auto create_collection(rxComm& _comm, const path& _p) -> bool;
        auto create_collection(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;
        auto create_collection(rxComm& _comm, const path& _p, const path& _existing_p) -> bool;
        auto create_collection(rxComm& _comm, const path& _p, const path& _existing_p, std::error_code& _ec) -> bool;
        auto create_collections(rxComm& _comm, const path& _p) -> bool;
        auto create_collections(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        auto exists(const object_status& _s) noexcept -> bool;
        auto exists(rxComm& _comm, const path& _p) -> bool;
        auto exists(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        /// \brief Checks if the path is registered in the catalog as a collection.
        ///
        /// \throws filesystem_error If the path is empty or exceeds the path limit.
        ///
        /// \param[in] _comm The communication object.
        /// \param[in] _p    The path to a collection.
        ///
        /// \return A boolean indicating whether the collection is registered.
        auto is_collection_registered(rxComm& _comm, const path& _p) -> bool;
        auto is_collection_registered(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        /// \brief Checks if the path is registered in the catalog as a data object.
        ///
        /// \throws filesystem_error If the path is empty or exceeds the path limit.
        ///
        /// \param[in] _comm The communication object.
        /// \param[in] _p    The path to a data object.
        ///
        /// \return A boolean indicating whether the data object is registered.
        auto is_data_object_registered(rxComm& _comm, const path& _p) -> bool;
        auto is_data_object_registered(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        auto equivalent(rxComm& _comm, const path& _p1, const path& _p2) -> bool;
        auto equivalent(rxComm& _comm, const path& _p1, const path& _p2, std::error_code& _ec) -> bool;

        /// \brief Returns the size of the latest good replica.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, or does not
        ///                          reference a data object.
        ///
        /// \param[in] _comm The communication object.
        /// \param[in] _p    The path to a data object.
        ///
        /// \return An integer representing the size of the data object.
        auto data_object_size(rxComm& _comm, const path& _p) -> std::uintmax_t;
        auto data_object_size(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::uintmax_t;

        auto is_collection(const object_status& _s) noexcept -> bool;
        auto is_collection(rxComm& _comm, const path& _p) -> bool;
        auto is_collection(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        /// \brief Checks if the path points to a special collection.
        ///
        /// This function always inspects the collection entry in the catalog.
        ///
        /// \throws filesystem_error If the path is empty or exceeds the path limit.
        ///
        /// \param[in] _comm The communication object.
        /// \param[in] _p    The path to a collection.
        ///
        /// \return A boolean indicating whether the collection is special.
        auto is_special_collection(rxComm& _comm, const path& _p) -> bool;
        auto is_special_collection(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        auto is_empty(rxComm& _comm, const path& _p) -> bool;
        auto is_empty(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        auto is_other(const object_status& _s) noexcept -> bool;
        auto is_other(rxComm& _comm, const path& _p) -> bool;
        auto is_other(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        auto is_data_object(const object_status& _s) noexcept -> bool;
        auto is_data_object(rxComm& _comm, const path& _p) -> bool;
        auto is_data_object(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;

        /// \brief Returns the mtime of the latest good replica or a collection.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, or does not
        ///                          reference a data object or collection.
        ///
        /// \param[in] _comm The communication object.
        /// \param[in] _p    The path to a data object or collection.
        ///
        /// \return An object_time_type representing the mtime.
        auto last_write_time(rxComm& _comm, const path& _p) -> object_time_type;
        auto last_write_time(rxComm& _comm, const path& _p, std::error_code& _ec) -> object_time_type;

        /// \brief Updates the mtime of a collection.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, or does not
        ///                          reference a collection.
        ///
        /// \param[in] _comm     The communication object.
        /// \param[in] _p        The path to a collection.
        /// \param[in] _new_time The new mtime.
        ///
        /// \return An object_time_type representing the mtime.
        auto last_write_time(rxComm& _comm, const path& _p, object_time_type _new_time) -> void;
        auto last_write_time(rxComm& _comm, const path& _p, object_time_type _new_time, std::error_code& _ec) -> void;

        auto remove(rxComm& _comm, const path& _p, remove_options _opts = remove_options::none) -> bool;
        auto remove(rxComm& _comm, const path& _p, std::error_code& _ec) -> bool;
        auto remove(rxComm& _comm, const path& _p, remove_options _opts, std::error_code& _ec) -> bool;
        auto remove(rxComm& _comm, const path& _p, extended_remove_options _opts) -> bool;
        auto remove(rxComm& _comm, const path& _p, extended_remove_options _opts, std::error_code& _ec) -> bool;

        auto remove_all(rxComm& _comm, const path& _p, remove_options _opts = remove_options::none) -> std::uintmax_t;
        auto remove_all(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::uintmax_t;
        auto remove_all(rxComm& _comm, const path& _p, remove_options _opts, std::error_code& _ec) -> std::uintmax_t;
        auto remove_all(rxComm& _comm, const path& _p, extended_remove_options _opts) -> std::uintmax_t;
        auto remove_all(rxComm& _comm, const path& _p, extended_remove_options _opts, std::error_code& _ec) -> std::uintmax_t;

        auto permissions(rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms) -> void;
        auto permissions(rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms, std::error_code& _ec) -> void;

        /// \brief Modifies the permissions of a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, or does not
        ///                          reference a collection or data object.
        ///
        /// \param[in] _admin         A tag which instructs the function to operate in administrator mode.
        ///                           The client must be an administrator to use this function.
        /// \param[in] _comm          The communication object.
        /// \param[in] _p             The logical path to a collection or data object.
        /// \param[in] _user_or_group The user or group for which the permission will apply to.
        /// \param[in] _prms          The permission to set for \p _user_or_group.
        ///
        /// \since 4.2.11
        auto permissions(admin_tag, rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms) -> void;
        auto permissions(admin_tag, rxComm& _comm, const path& _p, const std::string& _user_or_group, perms _prms, std::error_code& _ec) -> void;

        /// \brief Modifies the inheritance option of a collection.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, or does not
        ///                          reference a collection.
        ///
        /// \param[in] _comm  The communication object.
        /// \param[in] _p     The path to a collection.
        /// \param[in] _value A boolean indicating whether inheritance should be enabled or not.
        ///
        /// \since 4.2.11
        auto enable_inheritance(rxComm& _comm, const path& _p, bool _value) -> void;
        auto enable_inheritance(rxComm& _comm, const path& _p, bool _value, std::error_code& _ec) -> void;

        /// \brief Modifies the inheritance option of a collection.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, or does not
        ///                          reference a collection.
        ///
        /// \param[in] _admin A tag which instructs the function to operate in administrator mode. The client
        ///                   must be an administrator to use this function.
        /// \param[in] _comm  The communication object.
        /// \param[in] _p     The path to a collection.
        /// \param[in] _value A boolean indicating whether inheritance should be enabled or not.
        ///
        /// \since 4.2.11
        auto enable_inheritance(admin_tag _admin, rxComm& _comm, const path& _p, bool _value) -> void;
        auto enable_inheritance(admin_tag _admin, rxComm& _comm, const path& _p, bool _value, std::error_code& _ec) -> void;

        auto rename(rxComm& _comm, const path& _from, const path& _to) -> void;
        auto rename(rxComm& _comm, const path& _from, const path& _to, std::error_code& _ec) -> void;

        auto move(rxComm& _comm, const path& _from, const path& _to) -> void;
        auto move(rxComm& _comm, const path& _from, const path& _to, std::error_code& _ec) -> void;

        auto status(rxComm& _comm, const path& _p) -> object_status;
        auto status(rxComm& _comm, const path& _p, status_options _opts) -> object_status;
        auto status(rxComm& _comm, const path& _p, std::error_code& _ec) -> object_status;
        auto status(rxComm& _comm, const path& _p, status_options _opts, std::error_code& _ec) -> object_status;

        auto status_known(const object_status& _s) noexcept -> bool;

        /// \brief Returns the checksum of the latest good replica.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, or does not
        ///                          reference a data object.
        ///
        /// \param[in] _comm The communication object.
        /// \param[in] _p    The path to a data object.
        ///
        /// \return A string representing the checksum.
        auto data_object_checksum(rxComm& _comm, const path& _p) -> std::string;
        auto data_object_checksum(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::string;

        /// \brief Retrieves all metadata AVUs on a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm The communication object.
        /// \param[in] _p    The path to a collection or data object.
        ///
        /// \return A list of metadata AVUs.
        auto get_metadata(rxComm& _comm, const path& _p) -> std::vector<metadata>;
        auto get_metadata(rxComm& _comm, const path& _p, std::error_code& _ec) -> std::vector<metadata>;

        /// \brief Sets a single metadata AVU on a collection or data object.
        ///
        /// Upon successful execution, \p _p will have, at most, one metadata AVU that uses the attribute name
        /// defined by \p _metadata.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm     The communication object.
        /// \param[in] _p        The path to a collection or data object.
        /// \param[in] _metadata The metadata AVU to set.
        auto set_metadata(rxComm& _comm, const path& _p, const metadata& _metadata) -> void;
        auto set_metadata(rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void;

        /// \brief Sets a single metadata AVU on a collection or data object.
        ///
        /// Upon successful execution, \p _p will have, at most, one metadata AVU that uses the attribute name
        /// defined by \p _metadata.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _admin    A tag which instructs the function to operate in administrator mode. The client
        ///                      must be an administrator to use this function.
        /// \param[in] _comm     The communication object.
        /// \param[in] _p        The path to a collection or data object.
        /// \param[in] _metadata The metadata AVU to set.
        ///
        /// \since 4.2.12
        auto set_metadata(admin_tag _admin, rxComm& _comm, const path& _p, const metadata& _metadata) -> void;
        auto set_metadata(admin_tag _admin, rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void;

        /// \brief Adds a single metadata AVU on a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm     The communication object.
        /// \param[in] _p        The path to a collection or data object.
        /// \param[in] _metadata The metadata AVU to add.
        auto add_metadata(rxComm& _comm, const path& _p, const metadata& _metadata) -> void;
        auto add_metadata(rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void;

        /// \brief Adds a single metadata AVU on a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _admin    A tag which instructs the function to operate in administrator mode. The client
        ///                      must be an administrator to use this function.
        /// \param[in] _comm     The communication object.
        /// \param[in] _p        The path to a collection or data object.
        /// \param[in] _metadata The metadata AVU to add.
        ///
        /// \since 4.2.12
        auto add_metadata(admin_tag _admin, rxComm& _comm, const path& _p, const metadata& _metadata) -> void;
        auto add_metadata(admin_tag _admin, rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void;

        namespace detail
        {
            template <typename Iterator>
            auto do_metadata_op(
                bool _add_admin_flag,
                rxComm& _comm,
                const path& _path,
                Iterator _first,
                Iterator _last,
                std::string_view _op,
                std::error_code& _ec) -> void;

            template <typename Iterator>
            auto do_metadata_op(
                bool _add_admin_flag,
                rxComm& _comm,
                const path& _path,
                Iterator _first,
                Iterator _last,
                std::string_view _op) -> void;
        } // namespace detail

        /// \brief Atomically adds multiple metadata AVUs to a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm  The communication object.
        /// \param[in] _p     The path to a collection or data object.
        /// \param[in] _first An iterator pointing to the first element of a metadata AVU range.
        /// \param[in] _last  An iterator pointing to the last element of a metadata AVU range (not inclusive).
        template <typename Iterator>
        auto add_metadata_atomic(rxComm& _comm, const path& _p, Iterator _first, Iterator _last) -> void
        {
            constexpr auto add_admin_flag = false;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "add");
        } // add_metadata_atomic

        template <typename Iterator>
        auto add_metadata_atomic(rxComm& _comm, const path& _p, Iterator _first, Iterator _last, std::error_code& _ec) -> void
        {
            constexpr auto add_admin_flag = false;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "add", _ec);
        } // add_metadata_atomic

        /// \brief Atomically adds multiple metadata AVUs to a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _admin A tag which instructs the function to operate in administrator mode. The client
        ///                   must be an administrator to use this function.
        /// \param[in] _comm  The communication object.
        /// \param[in] _p     The path to a collection or data object.
        /// \param[in] _first An iterator pointing to the first element of a metadata AVU range.
        /// \param[in] _last  An iterator pointing to the last element of a metadata AVU range (not inclusive).
        ///
        /// \since 4.2.12
        template <typename Iterator>
        auto add_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, Iterator _first, Iterator _last)
            -> void
        {
            static_cast<void>(_admin);
            constexpr auto add_admin_flag = true;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "add");
        } // add_metadata_atomic

        template <typename Iterator>
        auto add_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, Iterator _first, Iterator _last, std::error_code& _ec)
            -> void
        {
            static_cast<void>(_admin);
            constexpr auto add_admin_flag = true;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "add", _ec);
        } // add_metadata_atomic

        /// \brief Atomically adds multiple metadata AVUs to a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm      The communication object.
        /// \param[in] _p         The path to a collection or data object.
        /// \param[in] _container The list of metadata AVUs to add.
        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto add_metadata_atomic(rxComm& _comm, const path& _p, const Container& _container) -> void
        {
            add_metadata_atomic(_comm, _p, std::begin(_container), std::end(_container));
        } // add_metadata_atomic

        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto add_metadata_atomic(rxComm& _comm, const path& _p, const Container& _container, std::error_code& _ec) -> void
        {
            add_metadata_atomic(_comm, _p, std::begin(_container), std::end(_container), _ec);
        } // add_metadata_atomic

        /// \brief Atomically adds multiple metadata AVUs to a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _admin     A tag which instructs the function to operate in administrator mode. The client
        ///                       must be an administrator to use this function.
        /// \param[in] _comm      The communication object.
        /// \param[in] _p         The path to a collection or data object.
        /// \param[in] _container The list of metadata AVUs to add.
        ///
        /// \since 4.2.12
        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto add_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, const Container& _container) -> void
        {
            static_cast<void>(_admin);
            add_metadata_atomic(admin, _comm, _p, std::begin(_container), std::end(_container));
        } // add_metadata_atomic

        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto add_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, const Container& _container, std::error_code& _ec) -> void
        {
            static_cast<void>(_admin);
            add_metadata_atomic(admin, _comm, _p, std::begin(_container), std::end(_container), _ec);
        } // add_metadata_atomic

        /// \brief Removes a single metadata AVU from a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm     The communication object.
        /// \param[in] _p        The path to a collection or data object.
        /// \param[in] _metadata The metadata AVU to remove.
        auto remove_metadata(rxComm& _comm, const path& _p, const metadata& _metadata) -> void;
        auto remove_metadata(rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void;

        /// \brief Removes a single metadata AVU from a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _admin    A tag which instructs the function to operate in administrator mode. The client
        ///                      must be an administrator to use this function.
        /// \param[in] _comm     The communication object.
        /// \param[in] _p        The path to a collection or data object.
        /// \param[in] _metadata The metadata AVU to remove.
        ///
        /// \since 4.2.12
        auto remove_metadata(admin_tag _admin, rxComm& _comm, const path& _p, const metadata& _metadata) -> void;
        auto remove_metadata(admin_tag _admin, rxComm& _comm, const path& _p, const metadata& _metadata, std::error_code& _ec) -> void;

        /// \brief Atomically removes multiple metadata AVUs from a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm  The communication object.
        /// \param[in] _p     The path to a collection or data object.
        /// \param[in] _first An iterator pointing to the first element of a metadata AVU range.
        /// \param[in] _last  An iterator pointing to the last element of a metadata AVU range (not inclusive).
        template <typename Iterator>
        auto remove_metadata_atomic(rxComm& _comm, const path& _p, Iterator _first, Iterator _last) -> void
        {
            constexpr auto add_admin_flag = false;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "remove");
        } // remove_metadata_atomic

        template <typename Iterator>
        auto remove_metadata_atomic(rxComm& _comm, const path& _p, Iterator _first, Iterator _last, std::error_code& _ec) -> void
        {
            constexpr auto add_admin_flag = false;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "remove", _ec);
        } // remove_metadata_atomic

        /// \brief Atomically removes multiple metadata AVUs from a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _admin A tag which instructs the function to operate in administrator mode. The client
        ///                   must be an administrator to use this function.
        /// \param[in] _comm  The communication object.
        /// \param[in] _p     The path to a collection or data object.
        /// \param[in] _first An iterator pointing to the first element of a metadata AVU range.
        /// \param[in] _last  An iterator pointing to the last element of a metadata AVU range (not inclusive).
        ///
        /// \since 4.2.12
        template <typename Iterator>
        auto remove_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, Iterator _first, Iterator _last)
            -> void
        {
            static_cast<void>(_admin);
            constexpr auto add_admin_flag = true;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "remove");
        } // remove_metadata_atomic

        template <typename Iterator>
        auto remove_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, Iterator _first, Iterator _last, std::error_code& _ec)
            -> void
        {
            static_cast<void>(_admin);
            constexpr auto add_admin_flag = true;
            detail::do_metadata_op(add_admin_flag, _comm, _p, _first, _last, "remove", _ec);
        } // remove_metadata_atomic

        /// \brief Atomically removes multiple metadata AVUs from a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _comm      The communication object.
        /// \param[in] _p         The path to a collection or data object.
        /// \param[in] _container The list of metadata AVUs to remove.
        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto remove_metadata_atomic(rxComm& _comm, const path& _p, const Container& _container) -> void
        {
            remove_metadata_atomic(_comm, _p, std::begin(_container), std::end(_container));
        } // remove_metadata_atomic

        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto remove_metadata_atomic(rxComm& _comm, const path& _p, const Container& _container, std::error_code& _ec) -> void
        {
            remove_metadata_atomic(_comm, _p, std::begin(_container), std::end(_container), _ec);
        } // remove_metadata_atomic

        /// \brief Atomically removes multiple metadata AVUs from a collection or data object.
        ///
        /// \throws filesystem_error If the path is empty, exceeds the path limit, does not reference
        ///                          a collection or data object, or an error occurs.
        ///
        /// \param[in] _admin     A tag which instructs the function to operate in administrator mode. The client
        ///                       must be an administrator to use this function.
        /// \param[in] _comm      The communication object.
        /// \param[in] _p         The path to a collection or data object.
        /// \param[in] _container The list of metadata AVUs to remove.
        ///
        /// \since 4.2.12
        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto remove_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, const Container& _container)
            -> void
        {
            static_cast<void>(_admin);
            remove_metadata_atomic(admin, _comm, _p, std::begin(_container), std::end(_container));
        } // remove_metadata_atomic

        template <
            typename Container,
            typename = decltype(std::begin(std::declval<Container>())),
            typename = std::enable_if_t<std::is_same_v<std::decay_t<typename Container::value_type>, metadata>>>
        auto remove_metadata_atomic(admin_tag _admin, rxComm& _comm, const path& _p, const Container& _container, std::error_code& _ec)
            -> void
        {
            static_cast<void>(_admin);
            remove_metadata_atomic(admin, _comm, _p, std::begin(_container), std::end(_container), _ec);
        } // remove_metadata_atomic

        namespace detail
        {
            template <typename Iterator>
            auto do_metadata_op(
                bool _add_admin_flag,
                rxComm& _comm,
                const path& _path,
                Iterator _first,
                Iterator _last,
                std::string_view _op,
                std::error_code& _ec) -> void
            {
                if (!validate_path(_path, _ec)) {
                    return;
                }

                std::string_view entity_type;

                const auto s = status(_comm, _path, status_options::basic, _ec);
                if (_ec) {
                    return;
                }

                if (is_data_object(s)) {
                    entity_type = "data_object";
                }
                else if (is_collection(s)) {
                    entity_type = "collection";
                }
                else {
                    _ec = make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION);
                    return;
                }

                using json = nlohmann::json;

                std::vector<json> operations;
                if constexpr (std::is_base_of_v<std::forward_iterator_tag,
                                                typename std::iterator_traits<Iterator>::iterator_category>)
                {
                    operations.reserve(static_cast<std::size_t>(std::distance(_first, _last)));
                }

                std::for_each(_first, _last, [_op, &operations](const metadata& md) {
                    // clang-format off
                    operations.push_back({
                        {"operation", _op},
                        {"attribute", md.attribute},
                        {"value", md.value},
                        {"units", md.units}
                    });
                    // clang-format on
                });

                // clang-format off
                const auto json_input = json{
                    {"admin_mode", _add_admin_flag},
                    {"entity_name", _path.c_str()},
                    {"entity_type", entity_type},
                    {"operations", std::move(operations)}
                }.dump();
                // clang-format on

                char* json_error_string{};
                irods::at_scope_exit free_error_string{[&json_error_string] { std::free(json_error_string); }};

                if (const auto err = rx_atomic_apply_metadata_operations(&_comm, json_input.c_str(), &json_error_string);
                    err != 0)
                {
                    _ec = make_error_code(err);
                }
            } // do_metadata_op

            template <typename Iterator>
            auto do_metadata_op(
                bool _add_admin_flag,
                rxComm& _comm,
                const path& _path,
                Iterator _first,
                Iterator _last,
                std::string_view _op) -> void
            {
                std::error_code ec;
                do_metadata_op(_add_admin_flag, _comm, _path, _first, _last, _op, ec);
                if (ec) {
                    if (ec == make_error_code(CAT_NOT_A_DATAOBJ_AND_NOT_A_COLLECTION)) {
                        throw filesystem_error{"cannot apply metadata operations: unknown object type", _path, ec};
                    }
                    throw filesystem_error{"cannot apply metadata operations", _path, ec};
                }
            } // do_metadata_op
        } // namespace detail
    } // namespace NAMESPACE_IMPL
} // namespace irods::filesystem

namespace irods::experimental
{
    namespace filesystem = ::irods::filesystem;
} // namespace irods::experimental

#endif // IRODS_FILESYSTEM_FILESYSTEM_HPP_INCLUDE_HEADER
