#include <catch2/catch_all.hpp>

#include "irods/private/catalog_access_control.hpp"
#include "irods/private/nanodbc_executor.hpp"
#include "irods/rodsErrorTable.h"

TEST_CASE("catalog_access_control collection access and inheritance", "[catalog][access_control]")
{
    using namespace irods::experimental::catalog;
    using namespace irods::experimental::catalog::access_control;

    nanodbc_executor executor(16);
    nanodbc::connection conn; // unconnected

    SECTION("check_collection_access on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_collection_access(executor, conn, "/tempZone/home", "rods", "tempZone", "own", false),
            nanodbc::database_error);

        CHECK_THROWS_AS(
            check_collection_access(executor, conn, "/tempZone/home", "rods", "tempZone", "own", true),
            nanodbc::database_error);
    }

    SECTION("check_collection_access_and_inherit on unconnected connection throws database_error")
    {
        int inherit_flag = 0;
        CHECK_THROWS_AS(
            check_collection_access_and_inherit(executor, conn, "/tempZone/home", "rods", "tempZone", "own", &inherit_flag),
            nanodbc::database_error);
    }

    SECTION("check_collection_id on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_collection_id(executor, conn, "10001", "rods", "tempZone", "own"),
            nanodbc::database_error);
    }

    SECTION("check_collection_own on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_collection_own(executor, conn, "/tempZone/home", "rods", "tempZone"),
            nanodbc::database_error);
    }
}

TEST_CASE("catalog_access_control data object and ticket access", "[catalog][access_control]")
{
    using namespace irods::experimental::catalog;
    using namespace irods::experimental::catalog::access_control;

    nanodbc_executor executor(16);
    nanodbc::connection conn; // unconnected

    SECTION("check_data_object_only on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_data_object_only(executor, conn, "/tempZone/home", "foo.txt", "rods", "tempZone", "read_object", false),
            nanodbc::database_error);

        CHECK_THROWS_AS(
            check_data_object_only(executor, conn, "/tempZone/home", "foo.txt", "rods", "tempZone", "read_object", true),
            nanodbc::database_error);
    }

    SECTION("check_data_object_own on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_data_object_own(executor, conn, "/tempZone/home", "foo.txt", "rods", "tempZone"),
            nanodbc::database_error);
    }

    SECTION("check_data_object_id on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_data_object_id(executor, conn, "20002", "rods", "tempZone", "read_object"),
            nanodbc::database_error);
    }

    SECTION("check_ticket_restrictions on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_ticket_restrictions(executor, conn, "30003", "127.0.0.1", "rods", "tempZone"),
            nanodbc::database_error);
    }

    SECTION("check_object_id_by_ticket on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_object_id_by_ticket(executor, conn, "20002", "read", "ticket_abc", "127.0.0.1", "rods", "tempZone"),
            nanodbc::database_error);
    }

    SECTION("ticket_update_write_bytes on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            ticket_update_write_bytes(executor, conn, "ticket_abc", "1024", "20002"),
            nanodbc::database_error);
    }
}

TEST_CASE("catalog_access_control resource, group admin, and token validation", "[catalog][access_control]")
{
    using namespace irods::experimental::catalog;
    using namespace irods::experimental::catalog::access_control;

    nanodbc_executor executor(16);
    nanodbc::connection conn; // unconnected

    SECTION("check_resource_access on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_resource_access(executor, conn, "demoResc", "rods", "tempZone", "read_object"),
            nanodbc::database_error);
    }

    SECTION("check_group_admin_access on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_group_admin_access(executor, conn, "rods", "tempZone", "rodsgroup"),
            nanodbc::database_error);
    }

    SECTION("get_group_member_count on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            get_group_member_count(executor, conn, "rodsgroup"),
            nanodbc::database_error);
    }

    SECTION("check_name_token on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_name_token(executor, conn, "access_type", "own"),
            nanodbc::database_error);
    }

    SECTION("check_user_in_group on unconnected connection throws database_error")
    {
        CHECK_THROWS_AS(
            check_user_in_group(executor, conn, "rods", "tempZone", "rodsgroup"),
            nanodbc::database_error);
    }
}
