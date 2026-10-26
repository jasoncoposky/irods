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

    SECTION("Serialize SELECT AST via to_query_string with quote escaping and grouping")
    {
        using gq2::builder::col;
        using gq2::builder::group;

        auto sel = gq2::builder::select({"DATA_ID", "DATA_SIZE"})
            .where(col("COLL_NAME") == "/tempZone/home/user's_dir" &&
                   col("DATA_NAME") == "file'1.txt" &&
                   group(col("DATA_REPL_STATUS") == "1" || col("DATA_REPL_STATUS") == "0") &&
                   col("META_DATA_ATTR_NAME").is_not_null())
            .order_by("DATA_REPL_STATUS", false)
            .order_by("DATA_MODIFY_TIME", false)
            .limit("1")
            .build();

        REQUIRE(gq2::to_query_string(sel) ==
                "select DATA_ID, DATA_SIZE where COLL_NAME = '/tempZone/home/user''s_dir' and "
                "DATA_NAME = 'file''1.txt' and (DATA_REPL_STATUS = '1' or DATA_REPL_STATUS = '0') and "
                "META_DATA_ATTR_NAME is not null order by DATA_REPL_STATUS desc, DATA_MODIFY_TIME desc "
                "fetch first 1 rows only");

        auto count_sel = gq2::builder::select(gq2::builder::count("DATA_ID"))
            .where(col("COLL_NAME") == "/tempZone/home" || col("COLL_NAME").like("/tempZone/home/%"))
            .build();

        REQUIRE(gq2::to_query_string(count_sel) ==
                "select count(DATA_ID) where COLL_NAME = '/tempZone/home' or COLL_NAME like '/tempZone/home/%'");
    }

    SECTION("Build parent_of ancestor path IN predicate")
    {
        using gq2::builder::col;

        auto sel = gq2::builder::select({"COLL_ID", "COLL_NAME"})
            .where(col("COLL_NAME").parent_of("/tempZone/home/alice/sub") && col("COLL_TYPE").like("_%"))
            .build();

        REQUIRE(gq2::to_query_string(sel) ==
                "select COLL_ID, COLL_NAME where COLL_NAME in ('/', '/tempZone', '/tempZone/home', "
                "'/tempZone/home/alice', '/tempZone/home/alice/sub') and COLL_TYPE like '_%'");
    }

#if __has_include("buffer.hpp")
    SECTION("rows_buffer_view zero-copy cell access and JSON conversion")
    {
        lite3cpp::Buffer buf;
        buf.init_array();
        const std::size_t row0 = buf.arr_append_obj(0);
        const std::size_t proj0 = buf.set_arr(row0, "proj");
        buf.arr_append_str(proj0, "1001");
        buf.arr_append_str(proj0, "/tempZone/home/rods");
        buf.arr_append_i64(proj0, 4096);

        const std::size_t row1 = buf.arr_append_obj(0);
        const std::size_t proj1 = buf.set_arr(row1, "proj");
        buf.arr_append_str(proj1, "1002");
        buf.arr_append_str(proj1, "/tempZone/home/alice");
        buf.arr_append_i64(proj1, 8192);

        const gq2::rows_buffer_view view{buf};
        REQUIRE_FALSE(view.empty());
        REQUIRE(view.size() == 2);
        REQUIRE(view.col_count(0) == 3);
        CHECK(view.cell_str(0, 0) == "1001");
        CHECK(view.cell_str(0, 1) == "/tempZone/home/rods");
        CHECK(view.cell_str(0, 2) == "4096");
        CHECK(view.cell_str(1, 0) == "1002");
        CHECK(view.cell_str(1, 2) == "8192");

        const auto j = view.to_json();
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK(j[0][0] == "1001");
        CHECK(j[1][2] == "8192");

        char cbuf[32]{};
        CHECK(view.copy_cell(0, 1, cbuf, sizeof(cbuf)) == 19);
        CHECK(std::string_view{cbuf} == "/tempZone/home/rods");

        long long id_val = 0;
        long long size_val = 0;
        CHECK(view.cell_int(0, 0, id_val));
        CHECK(id_val == 1001);
        CHECK(view.cell_int(1, 2, size_val));
        CHECK(size_val == 8192);

        char* dup_str = view.cell_strdup(1, 1);
        REQUIRE(dup_str != nullptr);
        CHECK(std::string_view{dup_str} == "/tempZone/home/alice");
        std::free(dup_str);
    }
#endif
}


