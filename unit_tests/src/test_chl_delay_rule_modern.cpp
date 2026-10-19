#include <catch2/catch_all.hpp>
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include <string>
#include <vector>

TEST_CASE("Delay Rule Engine DML Generation", "[icat][rule_exec]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    gq2::options opts;
    opts.admin_mode = true;

    SECTION("Build rule submission insert")
    {
        auto stmt = gq2::builder::insert_into("RULE_EXEC")
            .set("RULE_EXEC_ID", "9001")
            .set("RULE_NAME", "my_rule")
            .set("EXE_TIME", "1727280000")
            .set("EXE_STATUS", "0")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "9001");
        REQUIRE(params[1] == "my_rule");
    }

    SECTION("Build atomic CAS rule lock update")
    {
        auto stmt = gq2::builder::update("RULE_EXEC")
            .set("LOCK_HOST", "icat.example.org")
            .set("LOCK_HOST_PID", "12345")
            .set("LOCK_TS", "1727280100")
            .where(col("RULE_EXEC_ID") == "9001" && col("LOCK_HOST") == "")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE") != std::string::npos);
        REQUIRE(params.size() == 5);
        REQUIRE(params[0] == "icat.example.org");
        REQUIRE(params[1] == "12345");
        REQUIRE(params[2] == "1727280100");
        REQUIRE(params[3] == "9001");
        REQUIRE(params[4] == "");
    }

    SECTION("Build delay rule unlock update")
    {
        auto stmt = gq2::builder::update("RULE_EXEC")
            .set("LOCK_HOST", "")
            .set("LOCK_HOST_PID", "")
            .set("LOCK_TS", "")
            .set("MODIFY_TS", "1727280200")
            .where(col("RULE_EXEC_ID") == "9001")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE") != std::string::npos);
        REQUIRE(params.size() == 5);
        REQUIRE(params[0] == "");
        REQUIRE(params[3] == "1727280200");
        REQUIRE(params[4] == "9001");
    }

    SECTION("Build delay rule deletion")
    {
        auto stmt = gq2::builder::remove_from("RULE_EXEC")
            .where(col("RULE_EXEC_ID") == "9001")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("DELETE FROM") != std::string::npos);
        REQUIRE(params.size() == 1);
        REQUIRE(params[0] == "9001");
    }

    SECTION("Build delay rule modification update")
    {
        auto stmt = gq2::builder::update("RULE_EXEC")
            .set("PRIORITY", "5")
            .set("EXE_FREQUENCY", "600")
            .where(col("RULE_EXEC_ID") == "9001")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql.find("UPDATE") != std::string::npos);
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "5");
        REQUIRE(params[1] == "600");
        REQUIRE(params[2] == "9001");
    }
}
