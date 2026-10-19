#include <catch2/catch_all.hpp>

#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include <string>
#include <vector>

TEST_CASE("Data Object Metadata DML Generation", "[icat][data_obj]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build update for single replica status and checksum")
    {
        auto stmt = gq2::builder::update("DATA_OBJECT")
            .set("DATA_SIZE", "1048576")
            .set("DATA_CHECKSUM", "sha2:m4kL0w...")
            .set("DATA_REPL_STATUS", "1")
            .where(col("DATA_ID") == "10001" && col("DATA_REPL_NUM") == "0")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE R_DATA_MAIN SET") != std::string::npos);
        REQUIRE(sql.find("WHERE") != std::string::npos);
        REQUIRE(params.size() == 5);
        REQUIRE(params[0] == "1048576");
        REQUIRE(params[1] == "sha2:m4kL0w...");
        REQUIRE(params[2] == "1");
        REQUIRE(params[3] == "10001");
        REQUIRE(params[4] == "0");
    }

    SECTION("Build stale replica synchronization for ALL_REPL_STATUS_KW")
    {
        auto mark_stale_stmt = gq2::builder::update("DATA_OBJECT")
            .set("DATA_REPL_STATUS", "0")
            .where(col("DATA_ID") == "10001" && col("DATA_REPL_NUM") != "1")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(mark_stale_stmt, opts);

        REQUIRE(sql.find("data_is_dirty = ?") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "0");
        REQUIRE(params[1] == "10001");
        REQUIRE(params[2] == "1");
    }
}

TEST_CASE("Replica Registration and Unregistration DML", "[icat][data_obj]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build replica insert into DATA_OBJECT")
    {
        auto stmt = gq2::builder::insert_into("DATA_OBJECT")
            .set("DATA_ID", "10001")
            .set("COLL_ID", "5000")
            .set("DATA_NAME", "sample.dat")
            .set("DATA_REPL_NUM", "1")
            .set("RESC_NAME", "demoResc")
            .set("DATA_PATH", "/var/lib/irods/Vault/sample.dat")
            .set("DATA_SIZE", "2048")
            .set("DATA_REPL_STATUS", "1")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO R_DATA_MAIN") != std::string::npos);
        REQUIRE(params.size() == 8);
        REQUIRE(params[0] == "10001");
        REQUIRE(params[2] == "sample.dat");
    }

    SECTION("Build unregister replica removal")
    {
        auto stmt = gq2::builder::remove_from("DATA_OBJECT")
            .where(col("DATA_ID") == "10001" && col("DATA_REPL_NUM") == "1")
            .build();

        gq2::options opts;
        opts.admin_mode = true;
        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM R_DATA_MAIN") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "10001");
        REQUIRE(params[1] == "1");
    }
}

