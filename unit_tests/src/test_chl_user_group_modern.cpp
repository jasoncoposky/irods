#include <catch2/catch_all.hpp>
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include <string>
#include <vector>

TEST_CASE("User and Group Management DML", "[icat][user]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    gq2::options opts;
    opts.admin_mode = true;

    SECTION("Build user insert into USER")
    {
        auto stmt = gq2::builder::insert_into("USER")
            .set("USER_ID", "4001")
            .set("USER_NAME", "alice")
            .set("USER_TYPE_NAME", "rodsuser")
            .set("USER_ZONE", "tempZone")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "4001");
        REQUIRE(params[1] == "alice");
    }

    SECTION("Build group membership insert")
    {
        auto stmt = gq2::builder::insert_into("USER_GROUP")
            .set("GROUP_USER_ID", "4000")
            .set("USER_ID", "4001")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "4000");
        REQUIRE(params[1] == "4001");
    }

    SECTION("Build group membership removal")
    {
        auto stmt = gq2::builder::remove_from("USER_GROUP")
            .where(col("GROUP_USER_ID") == "4000" && col("USER_ID") == "4001")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "4000");
        REQUIRE(params[1] == "4001");
    }

    SECTION("Build user deletion from USER")
    {
        auto stmt = gq2::builder::remove_from("USER")
            .where(col("USER_NAME") == "alice" && col("USER_ZONE") == "tempZone")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "alice");
        REQUIRE(params[1] == "tempZone");
    }

    SECTION("Build user deletion from USER_GROUP")
    {
        auto stmt = gq2::builder::remove_from("USER_GROUP")
            .where(col("USER_ID") == "4001" || col("GROUP_USER_ID") == "4001")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "4001");
        REQUIRE(params[1] == "4001");
    }

    SECTION("Build password insert into USER_PASSWORD")
    {
        auto stmt = gq2::builder::insert_into("USER_PASSWORD")
            .set("USER_ID", "4001")
            .set("RCAT_PASSWORD", "scrambled_secret")
            .set("PASS_EXPIRY_TS", "9999-12-31-23.59.01")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "4001");
        REQUIRE(params[1] == "scrambled_secret");
    }

    SECTION("Build auth insert into USER_AUTH")
    {
        auto stmt = gq2::builder::insert_into("USER_AUTH")
            .set("USER_ID", "4001")
            .set("USER_AUTH_NAME", "/C=US/O=Test/CN=alice")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "4001");
        REQUIRE(params[1] == "/C=US/O=Test/CN=alice");
    }

    SECTION("Build expired password deletion from USER_PASSWORD")
    {
        auto stmt = gq2::builder::remove_from("USER_PASSWORD")
            .where(col("USER_ID") == "4001" && col("RCAT_PASSWORD") == "temp_secret")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "4001");
        REQUIRE(params[1] == "temp_secret");
    }
}


