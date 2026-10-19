#include <catch2/catch_all.hpp>

#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"

#include <string>
#include <vector>

TEST_CASE("GenQuery2 DML SQL Lowering Engine", "[genquery2][sql][dml]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    gq2::options opts;
    opts.admin_mode = true;

    SECTION("Lower INSERT into R_DATA_MAIN")
    {
        auto ins = gq2::builder::insert_into("DATA_OBJECT")
            .set("DATA_ID", "10001")
            .set("DATA_NAME", "foo.txt")
            .set("DATA_SIZE", "1024")
            .build();

        auto [sql, params] = gq2::to_sql(ins, opts);

        REQUIRE(sql == "INSERT INTO R_DATA_MAIN (data_id, data_name, data_size) VALUES (?, ?, ?)");
        REQUIRE(params.size() == 3);
        REQUIRE(params[0] == "10001");
        REQUIRE(params[1] == "foo.txt");
        REQUIRE(params[2] == "1024");
    }

    SECTION("Lower UPDATE on R_DATA_MAIN with compound WHERE")
    {
        auto upd = gq2::builder::update("DATA_OBJECT")
            .set("DATA_SIZE", "2048")
            .set("DATA_CHECKSUM", "sha2:xyz...")
            .where(col("DATA_ID") == "10001" && col("DATA_REPL_NUM") == "0")
            .build();

        auto [sql, params] = gq2::to_sql(upd, opts);

        REQUIRE(sql == "UPDATE R_DATA_MAIN SET data_size = ?, data_checksum = ? WHERE data_id = ? and data_repl_num = ?");
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "2048");
        REQUIRE(params[1] == "sha2:xyz...");
        REQUIRE(params[2] == "10001");
        REQUIRE(params[3] == "0");
    }

    SECTION("Lower REMOVE from R_DATA_MAIN with WHERE condition")
    {
        auto rem = gq2::builder::remove_from("DATA_OBJECT")
            .where(col("DATA_ID") == "10001")
            .build();

        auto [sql, params] = gq2::to_sql(rem, opts);

        REQUIRE(sql == "DELETE FROM R_DATA_MAIN WHERE data_id = ?");
        REQUIRE(params.size() == 1);
        REQUIRE(params[0] == "10001");
    }

    SECTION("Lower via statement variant")
    {
        gq2::statement stmt = gq2::builder::insert_into("COLLECTION")
            .set("COLL_ID", "5001")
            .set("COLL_NAME", "/tempZone/home/rods")
            .build();

        auto [sql, params] = gq2::to_sql(stmt, opts);

        REQUIRE(sql == "INSERT INTO R_COLL_MAIN (coll_id, coll_name) VALUES (?, ?)");
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "5001");
        REQUIRE(params[1] == "/tempZone/home/rods");
    }

    SECTION("Direct table and column names")
    {
        auto ins = gq2::builder::insert_into("R_USER_MAIN")
            .set("user_id", "4001")
            .set("user_name", "alice")
            .build();

        auto [sql, params] = gq2::to_sql(ins, opts);

        REQUIRE(sql == "INSERT INTO R_USER_MAIN (user_id, user_name) VALUES (?, ?)");
        REQUIRE(params.size() == 2);
    }

    SECTION("Lower UPDATE with grouped condition")
    {
        using gq2::builder::group;
        auto upd = gq2::builder::update("DATA_OBJECT")
            .set("DATA_SIZE", "4096")
            .where(group(col("DATA_NAME") == "a.txt" || col("DATA_NAME") == "b.txt") && col("DATA_REPL_NUM") == "0")
            .build();

        auto [sql, params] = gq2::to_sql(upd, opts);

        REQUIRE(sql == "UPDATE R_DATA_MAIN SET data_size = ? WHERE (data_name = ? or data_name = ?) and data_repl_num = ?");
        REQUIRE(params.size() == 4);
        REQUIRE(params[0] == "4096");
        REQUIRE(params[1] == "a.txt");
        REQUIRE(params[2] == "b.txt");
        REQUIRE(params[3] == "0");
    }
}

TEST_CASE("GenQuery2 DML Admin Privilege Invariant", "[genquery2][dml][security]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    gq2::options unprivileged_opts;
    unprivileged_opts.admin_mode = false;

    gq2::options privileged_opts;
    privileged_opts.admin_mode = true;

    SECTION("Insert statement rejects unprivileged compilation")
    {
        auto ins = gq2::builder::insert_into("DATA_OBJECT")
            .set("DATA_NAME", "foo.txt")
            .build();

        CHECK_THROWS_WITH(
            gq2::to_sql(ins, unprivileged_opts),
            Catch::Matchers::ContainsSubstring("GenQuery2 insert operation requires rodsadmin privileges"));

        CHECK_NOTHROW(gq2::to_sql(ins, privileged_opts));
    }

    SECTION("Update statement rejects unprivileged compilation")
    {
        auto upd = gq2::builder::update("DATA_OBJECT")
            .set("DATA_SIZE", "1024")
            .where(col("DATA_NAME") == "foo.txt")
            .build();

        CHECK_THROWS_WITH(
            gq2::to_sql(upd, unprivileged_opts),
            Catch::Matchers::ContainsSubstring("GenQuery2 update operation requires rodsadmin privileges"));

        CHECK_NOTHROW(gq2::to_sql(upd, privileged_opts));
    }

    SECTION("Remove statement rejects unprivileged compilation")
    {
        auto rem = gq2::builder::remove_from("DATA_OBJECT")
            .where(col("DATA_NAME") == "foo.txt")
            .build();

        CHECK_THROWS_WITH(
            gq2::to_sql(rem, unprivileged_opts),
            Catch::Matchers::ContainsSubstring("GenQuery2 remove operation requires rodsadmin privileges"));

        CHECK_NOTHROW(gq2::to_sql(rem, privileged_opts));
    }

    SECTION("Variant statement dispatch enforces privilege check")
    {
        gq2::statement stmt = gq2::builder::remove_from("DATA_OBJECT")
            .where(col("DATA_NAME") == "foo.txt")
            .build();

        CHECK_THROWS_WITH(
            gq2::to_sql(stmt, unprivileged_opts),
            Catch::Matchers::ContainsSubstring("GenQuery2 remove operation requires rodsadmin privileges"));

        CHECK_NOTHROW(gq2::to_sql(stmt, privileged_opts));
    }
}

TEST_CASE("GenQuery2 SELECT with Explicit From Entity", "[genquery2][select]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    gq2::options opts;
    opts.admin_mode = true;

    SECTION("Select single column from entity")
    {
        auto sel = gq2::builder::select({"COLL_ID"})
            .from("COLLECTION")
            .where(col("COLL_NAME") == "/tempZone/home")
            .build();

        auto [sql, params] = gq2::to_sql(sel, opts);
        REQUIRE(sql.find("select") != std::string::npos);
        REQUIRE(sql.find("R_COLL_MAIN") != std::string::npos);
        REQUIRE(sql.find("where") != std::string::npos);
        REQUIRE(params.size() == 1);
        REQUIRE(params[0] == "/tempZone/home");
    }

    SECTION("Select aggregate function from entity")
    {
        auto sel = gq2::builder::select(gq2::builder::count("DATA_ID"))
            .from("DATA_OBJECT")
            .where(col("DATA_RESC_ID") == "10001")
            .build();

        auto [sql, params] = gq2::to_sql(sel, opts);
        REQUIRE(sql.find("count(") != std::string::npos);
        REQUIRE(sql.find("R_DATA_MAIN") != std::string::npos);
        REQUIRE(params.size() == 1);
        REQUIRE(params[0] == "10001");

        auto sum_sel = gq2::builder::select(gq2::builder::sum("DATA_SIZE"))
            .from("DATA_OBJECT")
            .where(col("COLL_ID") == "100")
            .build();

        auto [sum_sql, sum_params] = gq2::to_sql(sum_sel, opts);
        REQUIRE(sum_sql.find("sum(") != std::string::npos);
        REQUIRE(sum_sql.find("R_DATA_MAIN") != std::string::npos);
        REQUIRE(sum_params.size() == 1);
        REQUIRE(sum_params[0] == "100");
    }


    SECTION("Select unmapped entity and columns")
    {
        auto sel = gq2::builder::select({"option_value"})
            .from("R_GRID_CONFIGURATION")
            .where(col("namespace") == "server" && col("option_name") == "log_level")
            .build();

        auto [sql, params] = gq2::to_sql(sel, opts);
        REQUIRE(sql.find("select") != std::string::npos);
        REQUIRE(sql.find("R_GRID_CONFIGURATION") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "server");
        REQUIRE(params[1] == "log_level");
    }

    SECTION("Select with group_by from entity")
    {
        auto sel = gq2::builder::select({"resc_id", "data_owner_name"})
            .project(gq2::builder::sum("data_size"))
            .from("DATA_OBJECT")
            .where(col("resc_id") == "10001")
            .group_by({"resc_id", "data_owner_name"})
            .build();

        auto [sql, params] = gq2::to_sql(sel, opts);
        REQUIRE(sql.find("select") != std::string::npos);
        REQUIRE(sql.find("from R_DATA_MAIN") != std::string::npos);
        REQUIRE(sql.find("where") != std::string::npos);
        REQUIRE(sql.find("group by resc_id, data_owner_name") != std::string::npos);
        REQUIRE(params.size() == 1);
        REQUIRE(params[0] == "10001");
    }
}


