#include <catch2/catch_all.hpp>

#include "irods/private/genquery2_builder.hpp"

#include <string>
#include <variant>

TEST_CASE("GenQuery2 Fluent Builder API", "[genquery2][builder]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    SECTION("Build INSERT AST")
    {
        auto ins = gq2::builder::insert_into("DATA_OBJECT")
            .set("DATA_NAME", "file.txt")
            .set("DATA_SIZE", "512")
            .build();

        REQUIRE(ins.target_entity == "DATA_OBJECT");
        REQUIRE(ins.assignments.size() == 2);
        REQUIRE(ins.assignments[0].first == "DATA_NAME");
        REQUIRE(ins.assignments[0].second == "file.txt");
        REQUIRE(ins.assignments[1].first == "DATA_SIZE");
        REQUIRE(ins.assignments[1].second == "512");

        gq2::statement stmt = ins;
        REQUIRE(std::holds_alternative<gq2::insert>(stmt));
    }

    SECTION("Build UPDATE AST with compound WHERE condition")
    {
        auto upd = gq2::builder::update("DATA_OBJECT")
            .set("DATA_SIZE", "1024")
            .where(col("DATA_ID") == "100" && col("DATA_REPL_NUM") == "0")
            .build();

        REQUIRE(upd.target_entity == "DATA_OBJECT");
        REQUIRE(upd.assignments.size() == 1);
        REQUIRE(upd.assignments[0].first == "DATA_SIZE");
        REQUIRE(upd.assignments[0].second == "1024");
        REQUIRE(upd.where_conditions.size() >= 1);

        gq2::statement stmt = upd;
        REQUIRE(std::holds_alternative<gq2::update>(stmt));
    }

    SECTION("Build REMOVE AST with WHERE condition")
    {
        auto rem = gq2::builder::remove_from("DATA_OBJECT")
            .where(col("DATA_ID") == "100")
            .build();

        REQUIRE(rem.target_entity == "DATA_OBJECT");
        REQUIRE(rem.where_conditions.size() >= 1);

        gq2::statement stmt = rem;
        REQUIRE(std::holds_alternative<gq2::remove>(stmt));
    }

    SECTION("Build SELECT AST with projections, distinct, and limit")
    {
        auto sel = gq2::builder::select({"DATA_ID", "DATA_NAME"})
            .distinct(true)
            .where(col("DATA_SIZE") > "0" || col("DATA_NAME").like("%.txt"))
            .order_by({"DATA_ID"}, true)
            .limit("20", "10")
            .build();

        REQUIRE(sel.distinct == true);
        REQUIRE(sel.projections.size() == 2);
        REQUIRE(sel.range.number_of_rows == "20");
        REQUIRE(sel.range.offset == "10");
        REQUIRE(sel.order_by.sort_expressions.size() == 1);

        gq2::statement stmt = sel;
        REQUIRE(std::holds_alternative<gq2::select>(stmt));
    }

    SECTION("Condition operator negation")
    {
        auto rem = gq2::builder::remove_from("COLLECTION")
            .where(!(col("COLL_NAME") == "/tempZone/home"))
            .build();

        REQUIRE(rem.target_entity == "COLLECTION");
        REQUIRE(rem.where_conditions.size() >= 1);
    }

    SECTION("Build SELECT AST with explicit from entity and aggregates")
    {
        using gq2::builder::col;
        auto sel = gq2::builder::select({"DATA_ID"})
            .from("DATA_OBJECT")
            .where(col("DATA_NAME") == "foo.txt")
            .build();

        REQUIRE(sel.from_entity == "DATA_OBJECT");
        REQUIRE(sel.projections.size() == 1);

        auto count_sel = gq2::builder::select(gq2::builder::count("DATA_ID"))
            .from("DATA_OBJECT")
            .where(col("COLL_ID") == "100")
            .build();

        REQUIRE(count_sel.from_entity == "DATA_OBJECT");
        REQUIRE(count_sel.projections.size() == 1);

        auto sum_sel = gq2::builder::select(gq2::builder::sum("DATA_SIZE"))
            .from("DATA_OBJECT")
            .where(col("COLL_ID") == "100")
            .build();

        REQUIRE(sum_sel.from_entity == "DATA_OBJECT");
        REQUIRE(sum_sel.projections.size() == 1);
    }
}

