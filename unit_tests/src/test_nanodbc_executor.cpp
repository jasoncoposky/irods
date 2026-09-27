#include <catch2/catch_all.hpp>

#include "irods/private/nanodbc_executor.hpp"
#include "irods/private/database_session.hpp"
#include "irods/private/genquery2_builder.hpp"
#include "irods/icatDefines.h"
#include <nanodbc/nanodbc.h>

#include <thread>

TEST_CASE("nanodbc_executor statement cache LRU semantics", "[database][nanodbc_executor]")
{
    using namespace irods::experimental::catalog;

    SECTION("statement_cache initialization and capacity")
    {
        statement_cache cache(3);
        REQUIRE(cache.size() == 0);
        REQUIRE(cache.capacity() == 3);
    }

    SECTION("statement_cache LRU eviction order")
    {
        statement_cache cache(2);

        nanodbc::statement s1;
        nanodbc::statement s2;
        nanodbc::statement s3;

        cache.put_for_testing("SELECT 1", s1);
        cache.put_for_testing("SELECT 2", s2);

        REQUIRE(cache.size() == 2);
        REQUIRE(cache.has("SELECT 1"));
        REQUIRE(cache.has("SELECT 2"));

        // Inserting third statement should evict the oldest ("SELECT 1")
        cache.put_for_testing("SELECT 3", s3);

        REQUIRE(cache.size() == 2);
        REQUIRE_FALSE(cache.has("SELECT 1"));
        REQUIRE(cache.has("SELECT 2"));
        REQUIRE(cache.has("SELECT 3"));
    }

    SECTION("statement_cache clear")
    {
        statement_cache cache(10);
        nanodbc::statement s;
        cache.put_for_testing("SELECT 1", s);
        cache.put_for_testing("SELECT 2", s);

        REQUIRE(cache.size() == 2);
        cache.clear();
        REQUIRE(cache.size() == 0);
        REQUIRE_FALSE(cache.has("SELECT 1"));
    }

    SECTION("statement_cache connection affinity: statement from different connection is evicted")
    {
        statement_cache cache(5);
        nanodbc::statement dummy;
        cache.put_for_testing("SELECT 1", dummy);
        REQUIRE(cache.has("SELECT 1"));

        nanodbc::connection other_conn; // unconnected, different handle
        REQUIRE_THROWS_AS(cache.get(other_conn, "SELECT 1"), nanodbc::database_error);
        REQUIRE_FALSE(cache.has("SELECT 1"));
    }

    SECTION("nanodbc_executor instantiation and cache access")
    {
        nanodbc_executor executor(64);
        REQUIRE(executor.cache().size() == 0);
        REQUIRE(executor.cache().capacity() == 64);
    }
}

TEST_CASE("nanodbc_executor statement dispatch", "[database][nanodbc_executor]")
{
    using namespace irods::experimental::catalog;
    namespace gq2 = irods::experimental::genquery2;

    SECTION("execution_result struct semantics")
    {
        execution_result res;
        REQUIRE(res.affected_rows == 0);
        REQUIRE_FALSE(res.query_result.has_value());

        res.affected_rows = 42;
        REQUIRE(res.affected_rows == 42);
    }

    SECTION("executing insert statement variant compiles and lowers")
    {
        gq2::insert ins;
        ins.target_entity = "DATA_OBJECT";
        ins.assignments.emplace_back("DATA_ID", "10001");
        ins.assignments.emplace_back("DATA_NAME", "foo.txt");

        gq2::statement stmt = ins;
        gq2::options opts;
        opts.admin_mode = true;

        auto [sql, params] = gq2::to_sql(stmt, opts);
        REQUIRE(sql.find("INSERT INTO") != std::string::npos);
        REQUIRE(params.size() == 2);
        REQUIRE(params[0] == "10001");
        REQUIRE(params[1] == "foo.txt");
    }

    SECTION("dispatching to execute() on unconnected connection attempts prepare and throws database_error")
    {
        nanodbc_executor executor(16);
        nanodbc::connection conn; // not connected

        gq2::insert ins;
        ins.target_entity = "DATA_OBJECT";
        ins.assignments.emplace_back("DATA_ID", "10001");
        gq2::statement stmt = ins;
        gq2::options opts;
        opts.admin_mode = true;

        REQUIRE_THROWS_AS(execute(executor, conn, stmt, opts), nanodbc::database_error);
    }
}

TEST_CASE("execute rejects DML without admin_mode", "[catalog][executor][security]")
{
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    irods::experimental::catalog::nanodbc_executor exec;
    nanodbc::connection conn; // dummy connection for invariant validation

    gq2::statement upd = gq2::builder::update("DATA_OBJECT")
        .set("DATA_SIZE", "512")
        .where(col("DATA_NAME") == "bar.txt")
        .build();

    gq2::options unprivileged_opts;
    unprivileged_opts.admin_mode = false;

    SECTION("rejects unprivileged execution")
    {
        CHECK_THROWS_WITH(
            irods::experimental::catalog::execute(exec, conn, upd, unprivileged_opts),
            Catch::Matchers::ContainsSubstring("Execution of GenQuery2 modification statements requires rodsadmin privileges"));
    }

    SECTION("execute_catalog automatically applies administrative privileges")
    {
        CHECK_THROWS_AS(
            irods::experimental::catalog::execute_catalog(exec, conn, upd),
            nanodbc::database_error);
    }
}

TEST_CASE("nanodbc_executor portable sequence sql generation", "[database][nanodbc_executor][sequence]")
{
    using namespace irods::experimental::catalog;

    SECTION("postgresql sequence sql")
    {
        CHECK(get_next_sequence_sql("R_ObjectID", "PostgreSQL Unicode") == "select nextval('R_ObjectID')");
        CHECK(get_current_sequence_sql("R_ObjectID", "PostgreSQL Unicode") == "select currval('R_ObjectID')");
        CHECK(get_next_sequence_sql("R_ObjectID", "postgres") == "select nextval('R_ObjectID')");
        CHECK(get_current_sequence_sql("R_ObjectID", "postgres") == "select currval('R_ObjectID')");
        CHECK(get_next_sequence_sql("R_ObjectID", DB_TYPE_POSTGRES) == "select nextval('R_ObjectID')");
        CHECK(get_current_sequence_sql("R_ObjectID", DB_TYPE_POSTGRES) == "select currval('R_ObjectID')");
    }

    SECTION("oracle sequence sql")
    {
        CHECK(get_next_sequence_sql("R_ObjectID", "Oracle") == "select R_ObjectID.nextval from DUAL");
        CHECK(get_current_sequence_sql("R_ObjectID", "Oracle") == "select R_ObjectID.currval from DUAL");
        CHECK(get_next_sequence_sql("R_ObjectID", "oracle") == "select R_ObjectID.nextval from DUAL");
        CHECK(get_current_sequence_sql("R_ObjectID", "oracle") == "select R_ObjectID.currval from DUAL");
        CHECK(get_next_sequence_sql("R_ObjectID", DB_TYPE_ORACLE) == "select R_ObjectID.nextval from DUAL");
        CHECK(get_current_sequence_sql("R_ObjectID", DB_TYPE_ORACLE) == "select R_ObjectID.currval from DUAL");
    }

    SECTION("mysql sequence sql")
    {
        CHECK(get_next_sequence_sql("R_ObjectID", "MySQL") == "select R_ObjectID_nextval()");
        CHECK(get_current_sequence_sql("R_ObjectID", "MySQL") == "select R_ObjectID_currval()");
        CHECK(get_next_sequence_sql("R_ObjectID", "mysql") == "select R_ObjectID_nextval()");
        CHECK(get_current_sequence_sql("R_ObjectID", "mysql") == "select R_ObjectID_currval()");
        CHECK(get_next_sequence_sql("R_ObjectID", DB_TYPE_MYSQL) == "select R_ObjectID_nextval()");
        CHECK(get_current_sequence_sql("R_ObjectID", DB_TYPE_MYSQL) == "select R_ObjectID_currval()");
    }

    SECTION("case-insensitive and driver alias name resolution")
    {
        CHECK(get_db_type_from_name("PostgreSQL") == DB_TYPE_POSTGRES);
        CHECK(get_db_type_from_name("PostgreSQL Unicode") == DB_TYPE_POSTGRES);
        CHECK(get_db_type_from_name("MySQL") == DB_TYPE_MYSQL);
        CHECK(get_db_type_from_name("MariaDB") == DB_TYPE_MYSQL);
        CHECK(get_db_type_from_name("Oracle") == DB_TYPE_ORACLE);
        CHECK(get_db_type_from_name("ORACLE") == DB_TYPE_ORACLE);
    }
}

TEST_CASE("nanodbc_executor sequence and scalar queries on unconnected connection throw", "[database][nanodbc_executor][queries]")
{
    using namespace irods::experimental::catalog;

    nanodbc_executor executor(16);
    nanodbc::connection conn; // not connected

    CHECK_THROWS_AS(executor.get_next_sequence_value(conn, "R_ObjectID"), nanodbc::database_error);
    CHECK_THROWS_AS(executor.get_next_sequence_value(conn, "R_ObjectID", DB_TYPE_POSTGRES), nanodbc::database_error);
    CHECK_THROWS_AS(executor.get_current_sequence_value(conn, "R_ObjectID"), nanodbc::database_error);
    CHECK_THROWS_AS(executor.get_current_sequence_value(conn, "R_ObjectID", DB_TYPE_MYSQL), nanodbc::database_error);
    CHECK_THROWS_AS(executor.query_integer(conn, "select 1"), nanodbc::database_error);
    CHECK_THROWS_AS(executor.query_string(conn, "select 1"), nanodbc::database_error);
    CHECK_THROWS_AS(executor.query_strings(conn, "select 1"), nanodbc::database_error);
}

TEST_CASE("nanodbc_executor bind_parameters closes open cursors and handles empty parameters safely", "[database][nanodbc_executor][binding]")
{
    using namespace irods::experimental::catalog;

    nanodbc_executor executor(16);
    nanodbc::statement stmt;

    // Verifies bind_parameters handles null handle and empty parameters without exception
    CHECK_NOTHROW(executor.bind_parameters(stmt, {}));
}

TEST_CASE("database_session thread-local persistence and cache reuse", "[database][database_session]")
{
    using namespace irods::experimental::catalog;

    SECTION("thread_local session identity per thread")
    {
        auto& main_session = get_database_session();
        auto& main_session_again = get_database_session();
        CHECK(&main_session == &main_session_again);

        database_session* thread_session_ptr = nullptr;
        std::thread t{[&thread_session_ptr]() {
            auto& t_sess = get_database_session();
            thread_session_ptr = &t_sess;
        }};
        t.join();

        REQUIRE(thread_session_ptr != nullptr);
        CHECK(&main_session != thread_session_ptr);
    }

    SECTION("session statement cache persistence and reset")
    {
        database_session session(10);
        CHECK_FALSE(session.is_connected());

        nanodbc::statement dummy;
        session.executor().cache().put_for_testing("SELECT 100", dummy);
        session.executor().cache().put_for_testing("SELECT 200", dummy);

        CHECK(session.executor().cache().size() == 2);
        CHECK(session.executor().cache().has("SELECT 100"));
        CHECK(session.executor().cache().has("SELECT 200"));

        session.reset();
        CHECK(session.executor().cache().size() == 0);
        CHECK_FALSE(session.executor().cache().has("SELECT 100"));
    }

    SECTION("database_session custom connection injection and cached execution")
    {
        database_session session(10);
        nanodbc::connection dummy_conn;

        session.set_connection(dummy_conn, "postgres");
        CHECK(session.db_instance_name() == "postgres");
        CHECK(session.db_type() == DB_TYPE_POSTGRES);

        session.set_connection(dummy_conn, "mysql");
        CHECK(session.db_instance_name() == "mysql");
        CHECK(session.db_type() == DB_TYPE_MYSQL);

        session.set_connection(dummy_conn, "oracle");
        CHECK(session.db_instance_name() == "oracle");
        CHECK(session.db_type() == DB_TYPE_ORACLE);

        nanodbc::statement s;
        session.executor().cache().put_for_testing("select * from r_coll_main", s);
        CHECK(session.executor().cache().has("select * from r_coll_main"));
        CHECK(session.executor().cache().size() == 1);

        session.reset();
        CHECK(session.executor().cache().size() == 0);
        CHECK_FALSE(session.executor().cache().has("select * from r_coll_main"));
    }

    SECTION("structured binding proxy handles on unconnected environment throw on connection attempt")
    {
        auto& session = get_database_session();
        session.reset();
        CHECK_THROWS_AS(get_session_connection(), std::exception);
        CHECK_THROWS_AS(get_session(), std::exception);
    }

    SECTION("query_catalog helpers attempt prepare and throw database_error on unconnected connection")
    {
        namespace gq2 = irods::experimental::genquery2;
        nanodbc_executor executor(16);
        nanodbc::connection conn; // not connected

        gq2::statement sel = gq2::builder::select({"COLL_ID"})
            .from("COLLECTION")
            .where(gq2::builder::col("COLL_NAME") == "/tempZone/home")
            .build();

        CHECK_THROWS_AS(query_catalog_integer(executor, conn, sel), nanodbc::database_error);
        CHECK_THROWS_AS(query_catalog_string(executor, conn, sel), nanodbc::database_error);
        CHECK_THROWS_AS(query_catalog_strings(executor, conn, sel), nanodbc::database_error);
    }
}


