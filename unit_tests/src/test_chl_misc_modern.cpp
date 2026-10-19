#include <catch2/catch_all.hpp>
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include <string>
#include <vector>

TEST_CASE("Specific Query, Quotas, and Zone Management DML", "[icat][misc]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build specific query insert with alias")
    {
        auto stmt = gq2::builder::insert_into("SPECIFIC_QUERY")
            .set("SQL_STR", "SELECT coll_name, sum(data_size) FROM R_DATA_MAIN GROUP BY coll_name")
            .set("ALIAS", "list_coll_sizes")
            .set("CREATE_TS", "1727280000")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[1] == "list_coll_sizes");
    }

    SECTION("Build specific query delete by alias")
    {
        auto stmt = gq2::builder::remove_from("SPECIFIC_QUERY")
            .where(col("ALIAS") == "list_coll_sizes")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 1);
        REQUIRE(params[0] == "list_coll_sizes");
    }

    SECTION("Build quota setting insert")
    {
        auto stmt = gq2::builder::insert_into("QUOTA")
            .set("USER_ID", "4001")
            .set("RESC_ID", "3001")
            .set("QUOTA_LIMIT", "107374182400")
            .set("MODIFY_TS", "1727280000")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "4001");
        REQUIRE(params[2] == "107374182400");
    }

    SECTION("Build quota deletion")
    {
        auto stmt = gq2::builder::remove_from("QUOTA")
            .where(col("USER_ID") == "4001" && col("RESC_ID") == "3001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "4001");
        REQUIRE(params[1] == "3001");
    }

    SECTION("Build zone registration insert")
    {
        auto stmt = gq2::builder::insert_into("ZONE")
            .set("ZONE_ID", "1001")
            .set("ZONE_NAME", "remoteZone")
            .set("ZONE_TYPE_NAME", "remote")
            .set("ZONE_CONN_STRING", "localhost:1247")
            .set("ZONE_COMMENT", "Testing remote zone")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 5);
        REQUIRE(params[0] == "1001");
        REQUIRE(params[1] == "remoteZone");
        REQUIRE(params[2] == "remote");
    }
}
