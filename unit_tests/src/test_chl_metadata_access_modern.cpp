#include <catch2/catch_all.hpp>
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include <string>
#include <vector>

TEST_CASE("AVU Metadata DML Generation", "[icat][avu]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build AVU insert into METADATA (R_META_MAIN)")
    {
        auto stmt = gq2::builder::insert_into("METADATA")
            .set("META_ID", "20001")
            .set("META_ATTR_NAME", "project")
            .set("META_ATTR_VALUE", "sequencing")
            .set("META_ATTR_UNIT", "v1")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "20001");
        REQUIRE(params[1] == "project");
        REQUIRE(params[2] == "sequencing");
        REQUIRE(params[3] == "v1");
    }

    SECTION("Build AVU map removal for delete")
    {
        auto stmt = gq2::builder::remove_from("METADATA_MAP")
            .where(col("OBJECT_ID") == "10001" && col("META_ID") == "20001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "10001");
        REQUIRE(params[1] == "20001");
    }

    SECTION("Build AVU update for set")
    {
        auto stmt = gq2::builder::update("METADATA")
            .set("META_ATTR_VALUE", "sequencing_v2")
            .set("META_ATTR_UNIT", "v2")
            .set("MODIFY_TS", "01727280000")
            .where(col("META_ID") == "20001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE") != std::string::npos);
        REQUIRE(sql.find("SET") != std::string::npos);
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "sequencing_v2");
        REQUIRE(params[1] == "v2");
        REQUIRE(params[2] == "01727280000");
        REQUIRE(params[3] == "20001");
    }

    SECTION("Build AVU mapping insert into METADATA_MAP (R_OBJT_METAMAP)")
    {
        auto stmt = gq2::builder::insert_into("METADATA_MAP")
            .set("OBJECT_ID", "10001")
            .set("META_ID", "20001")
            .set("CREATE_TS", "01727280000")
            .set("MODIFY_TS", "01727280000")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "10001");
        REQUIRE(params[1] == "20001");
        REQUIRE(params[2] == "01727280000");
        REQUIRE(params[3] == "01727280000");
    }
}

TEST_CASE("Access Control DML Generation", "[icat][acl]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build ACL permission update")
    {
        auto stmt = gq2::builder::update("ACCESS")
            .set("ACCESS_TYPE_ID", "1200")
            .where(col("OBJECT_ID") == "10001" && col("USER_ID") == "6001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "1200");
        REQUIRE(params[1] == "10001");
        REQUIRE(params[2] == "6001");
    }

    SECTION("Build ACL permission removal")
    {
        auto stmt = gq2::builder::remove_from("ACCESS")
            .where(col("OBJECT_ID") == "10001" && col("USER_ID") == "6001")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "10001");
        REQUIRE(params[1] == "6001");
    }
}

