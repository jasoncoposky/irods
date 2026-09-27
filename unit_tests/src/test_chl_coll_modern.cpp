#include <catch2/catch_all.hpp>
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include <string>
#include <vector>

TEST_CASE("Collection DML Generation", "[icat][coll]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build insert into COLLECTION")
    {
        auto stmt = gq2::builder::insert_into("COLLECTION")
            .set("COLL_ID", "5001")
            .set("COLL_NAME", "/tempZone/home/rods/subcoll")
            .set("COLL_PARENT_NAME", "/tempZone/home/rods")
            .set("COLL_OWNER_NAME", "rods")
            .set("COLL_OWNER_ZONE", "tempZone")
            .set("COLL_INHERITANCE", "1")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 6);
        REQUIRE(params[0] == "5001");
        REQUIRE(params[1] == "/tempZone/home/rods/subcoll");
        REQUIRE(params[2] == "/tempZone/home/rods");
    }

    SECTION("Build update for collection modification")
    {
        auto stmt = gq2::builder::update("COLLECTION")
            .set("COLL_COMMENTS", "Updated directory")
            .set("COLL_INHERITANCE", "0")
            .where(col("COLL_ID") == "5001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE") != std::string::npos);
        REQUIRE(sql.find("WHERE") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "Updated directory");
        REQUIRE(params[1] == "0");
        REQUIRE(params[2] == "5001");
    }

    SECTION("Build remove for collection deletion")
    {
        auto stmt = gq2::builder::remove_from("COLLECTION")
            .where(col("COLL_ID") == "5001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 1);
        REQUIRE(params[0] == "5001");
    }
}
