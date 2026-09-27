#include <catch2/catch_all.hpp>

#include "irods/irods_database_constants.hpp"
#include "irods/icatHighLevelRoutines.hpp"
#include "irods/irods_plugin_base.hpp"
#include "irods/private/genquery2_ast_types.hpp"
#include "irods/private/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include "irods/rcConnect.h"

TEST_CASE("DATABASE_OP_EXECUTE_GENQUERY2 Operation Constant", "[genquery2][database][dispatch]")
{
    CHECK(irods::DATABASE_OP_EXECUTE_GENQUERY2 == "database_execute_genquery2");
}

TEST_CASE("plugin_base Operation Registration and Query", "[genquery2][plugin][dispatch]")
{
    class test_plugin : public irods::plugin_base
    {
    public:
        test_plugin()
            : plugin_base("test_instance", "test_context")
        {
        }
    };

    test_plugin plug;
    CHECK_FALSE(plug.has_operation(irods::DATABASE_OP_EXECUTE_GENQUERY2));
    CHECK_FALSE(plug.has_operation("non_existent_op"));

    plug.add_operation(
        irods::DATABASE_OP_EXECUTE_GENQUERY2,
        std::function<irods::error(irods::plugin_context&)>(
            [](irods::plugin_context&) -> irods::error {
                return SUCCESS();
            }));

    CHECK(plug.has_operation(irods::DATABASE_OP_EXECUTE_GENQUERY2));
}

TEST_CASE("chl_execute_genquery2 Native AST Dispatch and Lowering", "[genquery2][icat][dispatch]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    gq2::options opts;
    opts.database = "postgres";
    opts.user_name = "rods";
    opts.user_zone = "tempZone";
    opts.admin_mode = true;
    opts.default_number_of_rows = 256;

    SECTION("Statement variant holds select and compiles to SQL")
    {
        gq2::select sel;
        sel.from_entity = "DATA_OBJECT";
        sel.projections.push_back(gq2::column{"DATA_NAME"});

        gq2::statement stmt = sel;
        REQUIRE(std::holds_alternative<gq2::select>(stmt));
        CHECK(std::get_if<gq2::select>(&stmt) != nullptr);
        CHECK(std::get_if<gq2::insert>(&stmt) == nullptr);

        const auto [sql, params] = gq2::to_sql(stmt, opts);
        CHECK(!sql.empty());
        CHECK(sql.find("select") != std::string::npos);
    }

    SECTION("Statement variant holds insert and compiles to SQL")
    {
        auto ins = gq2::builder::insert_into("DATA_OBJECT")
            .set("DATA_ID", "10001")
            .set("DATA_NAME", "foo.txt")
            .build();

        gq2::statement stmt = ins;
        REQUIRE(std::holds_alternative<gq2::insert>(stmt));
        CHECK(std::get_if<gq2::select>(&stmt) == nullptr);

        const auto [sql, params] = gq2::to_sql(stmt, opts);
        CHECK(sql.find("INSERT INTO") != std::string::npos);
        CHECK(params.size() == 2);
    }

    SECTION("Statement variant holds update and compiles to SQL")
    {
        auto upd = gq2::builder::update("DATA_OBJECT")
            .set("DATA_SIZE", "2048")
            .where(col("DATA_ID") == "10001")
            .build();

        gq2::statement stmt = upd;
        REQUIRE(std::holds_alternative<gq2::update>(stmt));
        CHECK(std::get_if<gq2::select>(&stmt) == nullptr);

        const auto [sql, params] = gq2::to_sql(stmt, opts);
        CHECK(sql.find("UPDATE") != std::string::npos);
        CHECK(params.size() == 2);
    }

    SECTION("Statement variant holds remove and compiles to SQL")
    {
        auto rem = gq2::builder::remove_from("DATA_OBJECT")
            .where(col("DATA_ID") == "10001")
            .build();

        gq2::statement stmt = rem;
        REQUIRE(std::holds_alternative<gq2::remove>(stmt));
        CHECK(std::get_if<gq2::select>(&stmt) == nullptr);

        const auto [sql, params] = gq2::to_sql(stmt, opts);
        CHECK(sql.find("DELETE FROM") != std::string::npos);
        CHECK(params.size() == 1);
    }

    SECTION("chl_execute_genquery2 invocation signature and fallback handling")
    {
        RsComm comm{};
        char* output = nullptr;

        gq2::select sel;
        sel.from_entity = "DATA_OBJECT";
        sel.projections.push_back(gq2::column{"DATA_NAME"});

        gq2::statement stmt = sel;

        // chl_execute_genquery2 is declared and callable.
        const int ec = chl_execute_genquery2(comm, stmt, opts, &output);
        CHECK(ec < 0);
        CHECK(output == nullptr);
    }
}
