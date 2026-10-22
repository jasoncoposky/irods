#include <catch2/catch_all.hpp>

#include "irods/client_connection.hpp"
#include "irods/filesystem.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/getRodsEnv.h"
#include "irods/rcConnect.h"
#include "irods/rodsClient.h"
#include "irods/rodsGenQuery.h"

#include <cctype>
#include <vector>
#include <string>
#include <algorithm>

namespace ix = irods::experimental;
namespace fs = irods::filesystem;
namespace gq2 = irods::experimental::genquery2;

TEST_CASE("query builder")
{
    rodsEnv env;
    _getRodsEnv(env);

    load_client_api_plugins();

    ix::client_connection conn{env.rodsHost, env.rodsPort, {env.rodsUserName, env.rodsZone}};
    auto& comm = static_cast<RcComm&>(conn);

    const fs::path user_home = env.rodsHome;

    SECTION("general query")
    {
        const auto q = gq2::builder::select(gq2::builder::count("COLL_NAME"))
                           .where(gq2::builder::col("COLL_NAME") == user_home.string());
        const auto query = gq2::execute(comm, q, env.rodsZone);

        REQUIRE(!query.empty());
    }

    SECTION("general query supports additional options")
    {
        const auto q = gq2::builder::select({"COLL_ID", "COLL_NAME"})
                           .where(gq2::builder::col("COLL_NAME") == user_home.string())
                           .order_by("COLL_NAME", true)
                           .limit(10, 0);
        const auto query = gq2::execute(comm, q, env.rodsZone);

        REQUIRE(!query.empty());
    }

    SECTION("parent_of condition builder")
    {
        const auto q = gq2::builder::select({"COLL_NAME"})
                           .where(gq2::builder::col("COLL_NAME").parent_of(user_home.string()));
        const auto query = gq2::execute(comm, q, env.rodsZone);

        REQUIRE(!query.empty());
    }
}

