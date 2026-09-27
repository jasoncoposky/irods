#include <catch2/catch_all.hpp>
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include <string>
#include <vector>

TEST_CASE("Resource Topology DML Generation", "[icat][resc]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build resource child edge registration")
    {
        auto stmt = gq2::builder::insert_into("RESOURCE_PARENT")
            .set("PARENT_RESC_ID", "3001")
            .set("CHILD_RESC_ID", "3002")
            .set("CONTEXT", "weight=1")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "3001");
        REQUIRE(params[1] == "3002");
        REQUIRE(params[2] == "weight=1");
    }

    SECTION("Build resource child unlinking")
    {
        auto stmt = gq2::builder::remove_from("RESOURCE_PARENT")
            .where(col("PARENT_RESC_ID") == "3001" && col("CHILD_RESC_ID") == "3002")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "3001");
        REQUIRE(params[1] == "3002");
    }

    SECTION("Build resource update")
    {
        auto stmt = gq2::builder::update("RESOURCE")
            .set("RESC_STATUS", "down")
            .set("RESC_COMMENTS", "maintenance")
            .where(col("RESC_ID") == "3001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE") != std::string::npos);
        REQUIRE(sql.find("SET") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "down");
        REQUIRE(params[1] == "maintenance");
        REQUIRE(params[2] == "3001");
    }
}
