#include <catch2/catch_all.hpp>

#include "irods/genquery2_builder.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_plugin_base.hpp"
#include "irods/rodsGenQuery.h"
#include "irods/rcMisc.h"

#include <string>
#include <string_view>
#include <vector>

namespace
{
    class dummy_plugin : public irods::plugin_base
    {
    public:
        dummy_plugin()
            : irods::plugin_base("dummy", "ctx")
        {
        }
    };
} // anonymous namespace

TEST_CASE("Hexagonal Database Port Constants and rows_buffer_to_genquery_out", "[hexagonal][genquery2][lite3]")
{
    SECTION("Database operation constants are defined")
    {
        CHECK(std::string_view{irods::DATABASE_OP_EXECUTE_GENQUERY2} == "database_execute_genquery2");
        CHECK(std::string_view{irods::DATABASE_OP_EXECUTE_GENQUERY2_BUFFER} == "database_execute_genquery2_buffer");
        CHECK(std::string_view{irods::DATABASE_OP_GET_CATALOG_VERSION} == "database_get_catalog_version");
        CHECK(std::string_view{irods::DATABASE_OP_INITIALIZE_CATALOG} == "database_initialize_catalog");
        CHECK(std::string_view{irods::DATABASE_OP_ATOMIC_APPLY} == "database_atomic_apply");
    }

    SECTION("plugin_base::has_operation checks operation table")
    {
        dummy_plugin p;
        CHECK_FALSE(p.has_operation(irods::DATABASE_OP_EXECUTE_GENQUERY2));
        CHECK_FALSE(p.has_operation(irods::DATABASE_OP_EXECUTE_GENQUERY2_BUFFER));
    }

    SECTION("rows_buffer_to_genquery_out converts lite3cpp::Buffer rows directly without JSON")
    {
        lite3cpp::Buffer buf(4096);
        buf.init_object();
        const std::size_t rows_ofs = buf.set_arr(0, "rows");

        const std::size_t r0 = buf.arr_append_arr(rows_ofs);
        buf.arr_append_str(r0, "/tempZone/home/rods");
        buf.arr_append_str(r0, "file1.txt");
        buf.arr_append_i64(r0, 1024);

        const std::size_t r1 = buf.arr_append_arr(rows_ofs);
        buf.arr_append_str(r1, "/tempZone/home/rods");
        buf.arr_append_str(r1, "file2.txt");
        buf.arr_append_i64(r1, 2048);

        const std::size_t r2 = buf.arr_append_arr(rows_ofs);
        buf.arr_append_str(r2, "/tempZone/home/rods");
        buf.arr_append_str(r2, "file3.txt");
        buf.arr_append_i64(r2, 4096);

        CHECK(irods::experimental::genquery2::rows_buffer_get_row_count(buf) == 3);

        const std::vector<int> col_ids{500, 403, 407};
        GenQueryOut* out = nullptr;

        // Limit to first 2 rows (simulating lookahead pagination where 3 rows were fetched for maxRows=2)
        const int ec = irods::experimental::genquery2::rows_buffer_to_genquery_out(buf, col_ids, &out, 9, 2);
        REQUIRE(ec == 0);
        REQUIRE(out != nullptr);
        CHECK(out->rowCnt == 2);
        CHECK(out->attriCnt == 3);
        CHECK(out->continueInx == 9);

        CHECK(out->sqlResult[0].attriInx == 500);
        CHECK(std::string_view{out->sqlResult[0].value} == "/tempZone/home/rods");
        CHECK(std::string_view{out->sqlResult[0].value + out->sqlResult[0].len} == "/tempZone/home/rods");

        CHECK(out->sqlResult[1].attriInx == 403);
        CHECK(std::string_view{out->sqlResult[1].value} == "file1.txt");
        CHECK(std::string_view{out->sqlResult[1].value + out->sqlResult[1].len} == "file2.txt");

        CHECK(out->sqlResult[2].attriInx == 407);
        CHECK(std::string_view{out->sqlResult[2].value} == "1024");
        CHECK(std::string_view{out->sqlResult[2].value + out->sqlResult[2].len} == "2048");

        freeGenQueryOut(&out);
        CHECK(out == nullptr);
    }
}
