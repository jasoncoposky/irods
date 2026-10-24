#include <catch2/catch_all.hpp>

#include "irods/private/genquery2_ast_types.hpp"

#include <string>
#include <variant>
#include <vector>

TEST_CASE("GenQuery2 DML AST Node Construction and Visitation", "[genquery2][ast]")
{
    namespace gq2 = irods::experimental::genquery2;

    SECTION("insert node structure")
    {
        gq2::insert ins;
        ins.target_entity = "DATA_OBJECT";
        ins.assignments.emplace_back("DATA_NAME", "foo.txt");
        ins.assignments.emplace_back("DATA_SIZE", "1024");

        REQUIRE(ins.target_entity == "DATA_OBJECT");
        REQUIRE(ins.assignments.size() == 2);
        REQUIRE(ins.assignments[0].first == "DATA_NAME");
        REQUIRE(ins.assignments[0].second == "foo.txt");

        gq2::statement stmt = ins;
        REQUIRE(std::holds_alternative<gq2::insert>(stmt));
    }

    SECTION("update node structure")
    {
        gq2::update upd;
        upd.target_entity = "DATA_OBJECT";
        upd.assignments.emplace_back("DATA_SIZE", "2048");

        gq2::statement stmt = upd;
        REQUIRE(std::holds_alternative<gq2::update>(stmt));
    }

    SECTION("remove node structure")
    {
        gq2::remove rem;
        rem.target_entity = "DATA_OBJECT";

        gq2::statement stmt = rem;
        REQUIRE(std::holds_alternative<gq2::remove>(stmt));
    }

    SECTION("select node in statement variant")
    {
        gq2::select sel;
        sel.distinct = true;

        gq2::statement stmt = sel;
        REQUIRE(std::holds_alternative<gq2::select>(stmt));
    }

    SECTION("recursive_box<condition_operator_not> copy, move, and std::visit")
    {
        gq2::condition_operator_not op_not{gq2::condition_equal{"foo"}};
        gq2::condition_expression expr = gq2::recursive_box<gq2::condition_operator_not>{op_not};

        // Copy construction and assignment
        gq2::condition_expression expr_copy = expr;
        REQUIRE(std::holds_alternative<gq2::recursive_box<gq2::condition_operator_not>>(expr_copy));

        const auto& boxed = std::get<gq2::recursive_box<gq2::condition_operator_not>>(expr_copy);
        REQUIRE(std::holds_alternative<gq2::condition_equal>(boxed.get().expression));
        REQUIRE(std::get<gq2::condition_equal>(boxed.get().expression).string_literal == "foo");

        // Move construction
        gq2::condition_expression expr_moved = std::move(expr_copy);
        bool visited_not = false;
        std::visit(
            [&visited_not](const auto& node) {
                using T = std::decay_t<decltype(node)>;
                if constexpr (std::is_same_v<T, gq2::recursive_box<gq2::condition_operator_not>>) {
                    visited_not = std::holds_alternative<gq2::condition_equal>(node.get().expression);
                }
            },
            expr_moved);
        REQUIRE(visited_not);
    }
}
