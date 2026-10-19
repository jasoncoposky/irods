#ifndef IRODS_CATALOG_ACCESS_CONTROL_HPP
#define IRODS_CATALOG_ACCESS_CONTROL_HPP

#include "irods/private/nanodbc_executor.hpp"
#include <nanodbc/nanodbc.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace irods::experimental::catalog::access_control
{
    // ==========================================
    // Collection Permission & Inheritance Checks
    // ==========================================

    // Check that a collection exists and user has 'access_level' permission.
    // Return code is either an iRODS error code (< 0) or the collectionId (>= 0).
    // If admin_mode is true, only check that the collection exists.
    auto check_collection_access(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        bool _admin_mode = false) -> int64_t;

    // Check that a collection exists and user has 'access_level' permission.
    // Return code is either an iRODS error code (< 0) or the collectionId (>= 0).
    // While at it, get the inheritance flag. Also validates tickets if provided.
    auto check_collection_access_and_inherit(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        int* _inherit_flag,
        std::string_view _ticket_str = "",
        std::string_view _ticket_host = "") -> int64_t;

    // Check collection access by collection ID.
    auto check_collection_id(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_id,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level) -> int64_t;

    // Check that a collection exists and user owns it.
    auto check_collection_own(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _user_name,
        std::string_view _user_zone) -> int64_t;

    // ==========================================
    // Data Object & Ticket Access Checks
    // ==========================================

    // Check that a dataObj (iRODS file) exists and user has specified permission
    // (but don't check collection access, only its existence).
    // If admin_mode is true, only check that the data object and collection exist.
    auto check_data_object_only(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _data_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        bool _admin_mode = false) -> int64_t;

    // Check that a dataObj exists and user owns it.
    auto check_data_object_own(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _dir_name,
        std::string_view _data_name,
        std::string_view _user_name,
        std::string_view _user_zone) -> int64_t;

    // Check that a user has specified permission to a dataObj by data ID.
    auto check_data_object_id(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _data_id,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level,
        std::string_view _ticket_str = "",
        std::string_view _ticket_host = "") -> int;

    // Check on additional restrictions on a ticket.
    auto check_ticket_restrictions(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _ticket_id,
        std::string_view _ticket_host,
        std::string_view _user_name,
        std::string_view _user_zone) -> int;

    // Check access via a Ticket to a data-object or collection.
    auto check_object_id_by_ticket(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _data_id,
        std::string_view _access_level,
        std::string_view _ticket_str,
        std::string_view _ticket_host,
        std::string_view _user_name,
        std::string_view _user_zone) -> int;

    // Update ticket write byte count stats.
    auto ticket_update_write_bytes(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _ticket_str,
        std::string_view _data_size,
        std::string_view _object_id) -> int;

    // ==========================================
    // Resource, Group Admin, and Token Checks
    // ==========================================

    // Check that a resource exists and user has 'access_level' permission.
    auto check_resource_access(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _resc_name,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _access_level) -> int64_t;

    // Check that the user has group-admin permission for group.
    auto check_group_admin_access(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _group_name) -> int;

    // Get the number of users who are members of a user group.
    auto get_group_member_count(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _group_name) -> int;

    // Check name token exists in namespace.
    auto check_name_token(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _namespace_name,
        std::string_view _token_name) -> int;

    // Check if user is in group.
    auto check_user_in_group(
        nanodbc_executor& _exec,
        nanodbc::connection& _conn,
        std::string_view _user_name,
        std::string_view _user_zone,
        std::string_view _group_name) -> int;

} // namespace irods::experimental::catalog::access_control

#endif // IRODS_CATALOG_ACCESS_CONTROL_HPP
