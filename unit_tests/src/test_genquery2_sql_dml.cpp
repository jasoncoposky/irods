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

    SECTION("Select USER_GROUP_NAME and USER_GROUP_ID joins R_USER_GROUP and R_USER_MAIN")
    {
        auto sel = gq2::builder::select({"USER_GROUP_NAME", "USER_GROUP_ID"})
            .where(col("USER_NAME") == "alice" && col("USER_ZONE") == "tempZone")
            .build();

        auto [sql, params] = gq2::to_sql(sel, opts);
        REQUIRE(sql.find("ug_grp.user_name") != std::string::npos);
        REQUIRE(sql.find("ug_grp.user_id") != std::string::npos);
        REQUIRE(sql.find("inner join R_USER_GROUP ug_map on") != std::string::npos);
        REQUIRE(sql.find("inner join R_USER_MAIN ug_grp on ug_map.group_user_id = ug_grp.user_id and ug_grp.user_type_name = 'rodsgroup'") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "alice");
        REQUIRE(params[1] == "tempZone");
    }

    SECTION("Select newly mapped columns DATA_RESC_NAME, DATA_OWNER_NAME, RESC_LOC, RESC_PARENT")
    {
        auto sel_data = gq2::builder::select({"DATA_ID", "DATA_RESC_NAME", "DATA_OWNER_NAME", "DATA_OWNER_ZONE", "COLL_OWNER_NAME", "COLL_OWNER_ZONE"})
            .where(col("COLL_NAME") == "/tempZone/home/alice")
            .build();

        auto [sql_data, params_data] = gq2::to_sql(sel_data, opts);
        REQUIRE(sql_data.find("resc_name") != std::string::npos);
        REQUIRE(sql_data.find("data_owner_name") != std::string::npos);
        REQUIRE(sql_data.find("data_owner_zone") != std::string::npos);
        REQUIRE(sql_data.find("coll_owner_name") != std::string::npos);
        REQUIRE(sql_data.find("coll_owner_zone") != std::string::npos);
        REQUIRE(params_data.size() == 1);
        REQUIRE(params_data[0] == "/tempZone/home/alice");

        auto sel_resc = gq2::builder::select({"RESC_ID", "RESC_LOC", "RESC_PARENT"})
            .where(col("RESC_NAME") == "demoResc")
            .build();

        auto [sql_resc, params_resc] = gq2::to_sql(sel_resc, opts);
        REQUIRE(sql_resc.find("resc_net") != std::string::npos);
        REQUIRE(sql_resc.find("resc_parent") != std::string::npos);
        REQUIRE(params_resc.size() == 1);
        REQUIRE(params_resc[0] == "demoResc");
    }

    SECTION("Select TOKEN_ID and RESC_ACCESS_TYPE / DATA_ACCESS_TYPE / COLL_ACCESS_TYPE")
    {
        auto sel_token = gq2::builder::select({"TOKEN_ID"})
            .where(col("TOKEN_NAMESPACE") == "access_type" && col("TOKEN_NAME") == "own")
            .build();

        auto [sql_token, params_token] = gq2::to_sql(sel_token, opts);
        REQUIRE(sql_token.find("R_TOKN_MAIN") != std::string::npos);
        REQUIRE(sql_token.find("token_id") != std::string::npos);
        REQUIRE(params_token.size() == 2);
        REQUIRE(params_token[0] == "access_type");
        REQUIRE(params_token[1] == "own");

        auto sel_resc_acl = gq2::builder::select({"RESC_ID"})
            .where(col("RESC_NAME") == "demoResc" && col("RESC_ACCESS_USER_ID") == "10001" && col("RESC_ACCESS_TYPE") >= "1120")
            .build();

        auto [sql_resc_acl, params_resc_acl] = gq2::to_sql(sel_resc_acl, opts);
        REQUIRE(sql_resc_acl.find("join R_OBJT_ACCESS proa on") != std::string::npos);
        REQUIRE(sql_resc_acl.find("proa.user_id = ?") != std::string::npos);
        REQUIRE(sql_resc_acl.find("proa.access_type_id >= ?") != std::string::npos);
        REQUIRE(params_resc_acl.size() == 3);
    }

    SECTION("Select SLD_*, QUOTA_*, TOKEN_*_TIME, and RULE_EXEC_* columns")
    {
        auto sel_sld = gq2::builder::select({"SLD_RESC_NAME", "SLD_LOAD_FACTOR"})
            .project(gq2::builder::max("SLD_CREATE_TIME"))
            .build();
        auto [sql_sld, params_sld] = gq2::to_sql(sel_sld, opts);
        REQUIRE(sql_sld.find("R_SERVER_LOAD_DIGEST") != std::string::npos);
        REQUIRE(sql_sld.find("load_factor") != std::string::npos);
        REQUIRE(sql_sld.find("max(") != std::string::npos);
        REQUIRE(params_sld.empty());

        auto sel_quota = gq2::builder::select({"QUOTA_USER_NAME", "QUOTA_USER_ZONE", "QUOTA_RESC_NAME", "QUOTA_LIMIT", "QUOTA_OVER", "QUOTA_MODIFY_TIME"})
            .where(col("QUOTA_USER_NAME") == "alice" && col("QUOTA_USER_ZONE") == "tempZone" && col("QUOTA_RESC_ID") != "0")
            .build();
        auto [sql_quota, params_quota] = gq2::to_sql(sel_quota, opts);
        REQUIRE(sql_quota.find("R_USER_MAIN") != std::string::npos);
        REQUIRE(sql_quota.find("R_QUOTA_MAIN") != std::string::npos);
        REQUIRE(sql_quota.find("R_RESC_MAIN") != std::string::npos);
        REQUIRE(sql_quota.find("quota_limit") != std::string::npos);
        REQUIRE(params_quota.size() == 3);

        auto sel_rule = gq2::builder::select({"RULE_EXEC_ID", "RULE_EXEC_NAME"})
            .where(col("RULE_EXEC_NAME").like("%abc%"))
            .build();
        auto [sql_rule, params_rule] = gq2::to_sql(sel_rule, opts);
        REQUIRE(sql_rule.find("R_RULE_EXEC") != std::string::npos);
        REQUIRE(sql_rule.find("rule_exec_id") != std::string::npos);
        REQUIRE(params_rule.size() == 1);
        REQUIRE(params_rule[0] == "%abc%");

        auto sel_quota_usage = gq2::builder::select({"QUOTA_USAGE_MODIFY_TIME", "QUOTA_RESC_NAME", "QUOTA_USER_NAME", "QUOTA_USER_ZONE", "QUOTA_USAGE"})
            .where(col("QUOTA_USER_NAME") == "alice" && col("QUOTA_USER_ZONE") == "tempZone")
            .build();
        auto [sql_qu, params_qu] = gq2::to_sql(sel_quota_usage, opts);
        REQUIRE(sql_qu.find("R_QUOTA_USAGE") != std::string::npos);
        REQUIRE(sql_qu.find("R_RESC_MAIN") != std::string::npos);
        REQUIRE(sql_qu.find("R_USER_MAIN") != std::string::npos);
        REQUIRE(sql_qu.find("quota_usage") != std::string::npos);
        REQUIRE(params_qu.size() == 2);
    }

    SECTION("Select TICKET_* columns, TICKET_DATA_COLL_NAME join, and non-admin ticket ownership check")
    {
        auto sel_ticket = gq2::builder::select({
                "TICKET_ID", "TICKET_STRING", "TICKET_TYPE", "TICKET_OBJECT_TYPE",
                "TICKET_OWNER_NAME", "TICKET_OWNER_ZONE", "TICKET_USES_COUNT", "TICKET_USES_LIMIT",
                "TICKET_WRITE_FILE_COUNT", "TICKET_WRITE_FILE_LIMIT",
                "TICKET_WRITE_BYTE_COUNT", "TICKET_WRITE_BYTE_LIMIT",
                "TICKET_EXPIRY_TS", "TICKET_DATA_NAME", "TICKET_DATA_COLL_NAME"
            })
            .where(col("TICKET_STRING") == "my_ticket_123")
            .build();

        auto [sql_admin, params_admin] = gq2::to_sql(sel_ticket, opts);
        REQUIRE(sql_admin.find("R_TICKET_MAIN") != std::string::npos);
        REQUIRE(sql_admin.find("R_USER_MAIN") != std::string::npos);
        REQUIRE(sql_admin.find("R_DATA_MAIN") != std::string::npos);
        REQUIRE(sql_admin.find("inner join R_COLL_MAIN td_coll on") != std::string::npos);
        REQUIRE(sql_admin.find("td_coll.coll_name") != std::string::npos);
        REQUIRE(params_admin.size() == 1);
        REQUIRE(params_admin[0] == "my_ticket_123");

        gq2::options non_admin_opts{};
        non_admin_opts.admin_mode = false;
        non_admin_opts.user_name = "alice";
        non_admin_opts.user_zone = "tempZone";

        auto [sql_user, params_user] = gq2::to_sql(sel_ticket, non_admin_opts);
        REQUIRE(sql_user.find("from R_USER_MAIN u where u.user_id =") != std::string::npos);
        REQUIRE(params_user.size() == 3);
        REQUIRE(params_user[0] == "my_ticket_123");
        REQUIRE(params_user[1] == "alice");
        REQUIRE(params_user[2] == "alice");

        auto sel_host = gq2::builder::select({"TICKET_ALLOWED_HOST"})
            .where(col("TICKET_ALLOWED_HOST_TICKET_ID") == "999")
            .build();
        auto [sql_host, params_host] = gq2::to_sql(sel_host, non_admin_opts);
        REQUIRE(sql_host.find("R_TICKET_ALLOWED_HOSTS") != std::string::npos);
        REQUIRE(sql_host.find("from R_TICKET_MAIN tm inner join R_USER_MAIN u on tm.user_id = u.user_id") != std::string::npos);
        REQUIRE(params_host.size() == 2);
        REQUIRE(params_host[0] == "999");
        REQUIRE(params_host[1] == "alice");
    }

    SECTION("Select USER_CRED_* and USER_SESSION_* columns requires admin_mode")
    {
        auto sel_cred = gq2::builder::select({
                "USER_CRED_HASHED_PASSWORD", "USER_CRED_SALT",
                "USER_CRED_HASHING_ALGORITHM", "USER_CRED_HASHING_PARAMETERS"
            })
            .where(col("USER_NAME") == "alice" && col("USER_ZONE") == "tempZone")
            .build();

        auto [sql_cred, params_cred] = gq2::to_sql(sel_cred, opts);
        REQUIRE(sql_cred.find("R_USER_CREDENTIALS") != std::string::npos);
        REQUIRE(sql_cred.find("R_USER_MAIN") != std::string::npos);
        REQUIRE(sql_cred.find("hashed_password") != std::string::npos);
        REQUIRE(params_cred.size() == 2);

        auto sel_sess = gq2::builder::select({
                "USER_SESSION_KEY", "USER_SESSION_EXPIRY_TIME", "USER_SESSION_SALT"
            })
            .where(col("USER_NAME") == "alice" && col("USER_ZONE") == "tempZone")
            .build();

        auto [sql_sess, params_sess] = gq2::to_sql(sel_sess, opts);
        REQUIRE(sql_sess.find("R_USER_SESSION_KEY") != std::string::npos);
        REQUIRE(sql_sess.find("session_key") != std::string::npos);
        REQUIRE(params_sess.size() == 2);

        gq2::options non_admin_opts{};
        non_admin_opts.admin_mode = false;
        auto [sql_cred_denied, _1] = gq2::to_sql(sel_cred, non_admin_opts);
        REQUIRE(sql_cred_denied.empty());
        auto [sql_sess_denied, _2] = gq2::to_sql(sel_sess, non_admin_opts);
        REQUIRE(sql_sess_denied.empty());
    }
}


