# Stage 18: Legacy GenQuery1 Engine Retirement & `rsGenQuery` $\rightarrow$ GenQuery2 AST Translator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire the legacy 2,400-line C-style SQL string-builder in `plugins/database/src/general_query.cpp`, `plugins/database/src/general_query_setup.cpp`, and `lib/genquery1/` by implementing a pure C++20 `parse_genquery1_string` parser and `genquery1_to_genquery2_ast` translator (`genQueryInp_t` $\rightarrow$ `irods::experimental::genquery2::select`) and routing `_rsGenQuery` in `server/api/src/rsGenQuery.cpp` directly through the GenQuery2 SQL compiler (`irods::experimental::genquery2::to_sql`) and executor (`chl_execute_genquery2_sql`).

**Architecture:**
1. **`lib/core/include/irods/genquery1_ast_adapter.hpp` & `lib/core/src/genquery1_ast_adapter.cpp`**:
   - Implements `parse_genquery1_string(const char* _s, genQueryInp_t* _out)` in pure C++20 (retiring `lib/genquery1/dsl/lexer.l` and `parser.y`) with 100% behavioral parity (hex `\xNN` unescaping, `''` escaping, aggregates `min`/`max`/`sum`/`avg`/`count`/`order`/`order_asc`/`order_desc`, relational operators, `between`, `in`, `begin_of`, `parent_of`, and error codes `INPUT_ARG_NOT_WELL_FORMED_ERR`, `NO_COLUMN_NAME_FOUND`, `INVALID_GENQUERY_AGGREGATE_FUNCTION`).
   - Implements `irods::experimental::genquery2::genquery1_to_genquery2_ast(const genQueryInp_t& _inp) -> irods::experimental::genquery2::select` and `genquery1_column_id_to_name(int _col_id) -> std::string_view`.
2. **GenQuery2 Column & Table Completeness (`genquery2_table_column_mappings.hpp` & `genquery2_sql.cpp`)**:
   - Adds all remaining GenQuery1 column aliases (`COLL_INFO_1`, `COLL_INFO_2`, `DATA_USER_NAME`, `DATA_ZONE_NAME`, `DATA_ACCESS_NAME`, `DATA_ACCESS_DATA_ID`, `DATA_TOKEN_NAMESPACE`, `COLL_USER_NAME`, `COLL_ZONE_NAME`, `COLL_ACCESS_NAME`, `COLL_ACCESS_COLL_ID`, `COLL_TOKEN_NAMESPACE`, `RESC_ACCESS_NAME`, `RESC_ACCESS_RESC_ID`, `RESC_TOKEN_NAMESPACE`, `RULE_EXEC_CONTEXT`, `AUDIT_*`) and `R_OBJT_AUDIT` join edges so GenQuery2 is a 100% superset of GenQuery1.
3. **Server API & ICAT Cleanup (`rsGenQuery.cpp`, `icatHighLevelRoutines.*`, `db_plugin.cpp`)**:
   - Re-implements `_rsGenQuery` in `server/api/src/rsGenQuery.cpp` using `genquery1_to_genquery2_ast`, `genquery2::to_sql`, `chl_execute_genquery2_sql`, and `rows_to_genquery_out` (preserving `QUOTA_QUERY`, `RETURN_TOTAL_ROW_COUNT`, `continueInx` pagination, `DISABLE_STRICT_ACL_KW`, `acPreProcForGenQuery`/`acPostProcForGenQuery`, and `resc_grp_attr_pos`/`resc_hier_attr_pos`).
   - Deletes `lib/genquery1/`, `plugins/database/src/general_query.cpp`, and `plugins/database/src/general_query_setup.cpp`, along with `chlGenQuery`, `chlGenQueryAccessControlSetup`, and `chlGenQueryTicketSetup`.

**Tech Stack:** C++20, `irods::experimental::genquery2`, `nlohmann::json`, `{fmt}`, Catch2, Project Insight CPG Verification.

---

### Task 1: C++20 `parse_genquery1_string`, `genquery1_to_genquery2_ast` Adapter, GenQuery2 Alias Completeness, & `lib/genquery1/` Retirement

**Files:**
- Create: `lib/core/include/irods/genquery1_ast_adapter.hpp`
- Create: `lib/core/src/genquery1_ast_adapter.cpp`
- Modify: `lib/core/include/irods/genquery2_builder.hpp`
- Modify: `lib/core/CMakeLists.txt`
- Modify: `lib/CMakeLists.txt`
- Delete: `lib/genquery1/CMakeLists.txt`
- Delete: `lib/genquery1/dsl/lexer.l`
- Delete: `lib/genquery1/dsl/parser.y`
- Modify: `server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp`
- Modify: `server/genquery2/src/genquery2_sql.cpp`
- Modify: `unit_tests/src/test_genquery1_flex_bison_parser.cpp`

- [ ] **Step 1: Add `std::span<const int>` overload of `rows_to_genquery_out` and fix `append_column` cast syntax in `lib/core/include/irods/genquery2_builder.hpp`**

1. In `lib/core/include/irods/genquery2_builder.hpp`:
   - Include `<span>`.
   - In `detail::append_column` (lines 36–43), update the typed column serialization from `_out += fmt::format("{}:{}", _col.name, _col.type_name);` to `_out += fmt::format("cast({} as {})", _col.name, _col.type_name);` so it matches `server/genquery2/dsl/parser.y` (`TK_CAST '(' IDENTIFIER TK_AS IDENTIFIER ')'`).
   - Extract the core `rows_to_genquery_out` logic into an overload accepting `const std::string& _json_rows, std::span<const int> _column_ids, genQueryOut_t*& _out` (with `std::max<std::size_t>(max_lengths[c] + 1, 50)` for `len` so legacy callers expecting `MINIMUM_COL_SIZE` stride in `sqlResult[c].value` remain 100% safe), and have `rows_to_genquery_out(const std::string&, const query_Spec&, genQueryOut_t*&)` delegate to it.

- [ ] **Step 2: Create `lib/core/include/irods/genquery1_ast_adapter.hpp` and `lib/core/src/genquery1_ast_adapter.cpp`**

1. Create `lib/core/include/irods/genquery1_ast_adapter.hpp` declaring:
   - `namespace irods::experimental::genquery2 {`
   - `auto genquery1_column_id_to_name(int _col_id) -> std::string_view;`
   - `auto genquery1_column_name_to_id(std::string_view _name) -> int;`
   - `auto genquery1_to_genquery2_ast(const genQueryInp_t& _inp) -> select;`
   - `}`
2. Create `lib/core/src/genquery1_ast_adapter.cpp` implementing:
   - `genquery1_column_id_to_name` and `genquery1_column_name_to_id` using `columnNames` / `NumOfColumnNames` from `rodsGenQueryNames.h` plus `COL_USER_AUTH_ID` (420 -> `"USER_AUTH_ID"`) and `COL_USER_DN_INVALID` (205 -> `"USER_DN_INVALID"`).
   - `parse_genquery1_string(const char* _s, genQueryInp_t* _out)` (`extern "C"` linkage matching `rcMisc.h`):
     - Replicate the exact lexical and syntactic rules of `lib/genquery1/dsl/lexer.l` and `lib/genquery1/dsl/parser.y`:
       - Skip whitespace `[ \t\n\r]+`.
       - Single-quoted string literals `'...'` with `''` $\rightarrow$ `'` and `\x[0-9A-Fa-f]{2}` hex unescaping (via `std::from_chars(..., 16)`).
       - Case-insensitive keywords: `select`, `where`, `like`, `in`, `between`, `and`, `not`, `begin_of`, `parent_of`.
       - Operators: `=`, `!=`, `<>` (normalized to `!=`), `<`, `<=`, `>`, `>=`, `,`, `(`, `)`, `||`, `&&`.
       - Identifiers: `[a-zA-Z][a-zA-Z0-9_]*`.
       - Any unknown character or unterminated string returns `INPUT_ARG_NOT_WELL_FORMED_ERR`.
       - Grammar: `SELECT projection_list [WHERE condition_list]`:
         - `projection`: `IDENTIFIER` (`addInxIval(&_out->selectInp, col_id, 1)`) or `IDENTIFIER '(' IDENTIFIER ')'` (`order`/`order_asc` $\rightarrow$ `ORDER_BY`, `order_desc` $\rightarrow$ `ORDER_BY_DESC`, `min` $\rightarrow$ `SELECT_MIN`, `max` $\rightarrow$ `SELECT_MAX`, `sum` $\rightarrow$ `SELECT_SUM`, `avg` $\rightarrow$ `SELECT_AVG`, `count` $\rightarrow$ `SELECT_COUNT`; unknown function returns `INVALID_GENQUERY_AGGREGATE_FUNCTION`; unknown column returns `NO_COLUMN_NAME_FOUND`).
         - `condition_list`: `condition` joined by `AND`.
         - `condition`: `IDENTIFIER condition_expression_list` (lookup column via `genquery1_column_name_to_id`; if `< 0`, return `NO_COLUMN_NAME_FOUND`; call `addInxVal(&_out->sqlCondInp, col_id, expr_str.c_str())`).
         - `condition_expression_list`: `condition_expression` joined by `" || "` or `" && "`.
         - `condition_expression`:
           - `LIKE STRING_LITERAL` $\rightarrow$ `fmt::format("like '{}'", s)`
           - `NOT LIKE STRING_LITERAL` $\rightarrow$ `fmt::format("not like '{}'", s)`
           - `BETWEEN STRING_LITERAL STRING_LITERAL` $\rightarrow$ `fmt::format("between '{}' '{}'", s1, s2)`
           - `EQUAL|NOT_EQUAL|LESS_THAN|LESS_THAN_OR_EQUAL_TO|GREATER_THAN|GREATER_THAN_OR_EQUAL_TO STRING_LITERAL` $\rightarrow$ `fmt::format("{} '{}'", op, s)`
           - `IN '(' string_literal_list ')'` or `IN string_literal_list` $\rightarrow$ `fmt::format("in ({})", list)` (where `string_literal_list` joins `'s'` with `","`).
           - `BEGIN_OF STRING_LITERAL` $\rightarrow$ `fmt::format("begin_of '{}'", s)`
           - `PARENT_OF STRING_LITERAL` $\rightarrow$ `fmt::format("parent_of '{}'", s)`
   - `genquery1_to_genquery2_ast(const genQueryInp_t& _inp)`:
     - Sets `select.distinct = (_inp.options & NO_DISTINCT) == 0;`.
     - Translates `_inp.selectInp` columns and aggregates (`SELECT_MIN`, `SELECT_MAX`, `SELECT_SUM`, `SELECT_AVG`, `SELECT_COUNT`), populates `select.group_by` when any column has an aggregate, and populates `select.order_by` with user-requested `ORDER_BY` / `ORDER_BY_DESC` followed by default ascending sort on non-aggregated `COLL_NAME`, `DATA_NAME`, `DATA_REPL_NUM`.
     - Translates `_inp.sqlCondInp` conditions:
       - Handles leading `n<`, `n>`, `n=` numeric cast prefix (`type_name = "decimal"`), `UPPER_CASE_WHERE` (`function{"upper", {col}}`), compound `||` (`logical_or`) and `&&` (`logical_and`) wrapped in `logical_grouping`, and relational predicates `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `like`, `not like`, `between`, `in`, `is null`, `is not null`, `parent_of`, and `begin_of`.
       - Maps `COL_R_RESC_GROUP_NAME` (`RESC_GROUP_NAME`) to `RESC_NAME` and `COL_DATA_RESC_GROUP_NAME` (`DATA_RESC_GROUP_NAME`) to `DATA_RESC_NAME` if encountered in AST construction.
3. Add `genquery1_ast_adapter.cpp` to `lib/core/CMakeLists.txt` (`IRODS_CORE_SOURCES`), remove `add_subdirectory(genquery1)` and `$<TARGET_OBJECTS:irods_genquery1_parser>` from `lib/CMakeLists.txt`, and `git rm -r lib/genquery1/`.

- [ ] **Step 3: Complete GenQuery1 Column Aliases in `genquery2_table_column_mappings.hpp` and `genquery2_sql.cpp`**

1. In `server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp`:
   - Add column mappings for:
     - `"COLL_INFO_1"` $\rightarrow$ `{"R_COLL_MAIN", "coll_info1"}`
     - `"COLL_INFO_2"` $\rightarrow$ `{"R_COLL_MAIN", "coll_info2"}`
     - `"DATA_USER_NAME"` $\rightarrow$ `{"R_USER_MAIN", "user_name"}`
     - `"DATA_ZONE_NAME"` $\rightarrow$ `{"R_USER_MAIN", "zone_name"}`
     - `"DATA_ACCESS_NAME"` $\rightarrow$ `{"R_TOKN_MAIN", "token_name"}`
     - `"DATA_ACCESS_DATA_ID"` $\rightarrow$ `{"R_OBJT_ACCESS", "object_id"}`
     - `"DATA_TOKEN_NAMESPACE"` $\rightarrow$ `{"R_TOKN_MAIN", "token_namespace"}`
     - `"COLL_USER_NAME"` $\rightarrow$ `{"R_USER_MAIN", "user_name"}`
     - `"COLL_ZONE_NAME"` $\rightarrow$ `{"R_USER_MAIN", "zone_name"}`
     - `"COLL_ACCESS_NAME"` $\rightarrow$ `{"R_TOKN_MAIN", "token_name"}`
     - `"COLL_ACCESS_COLL_ID"` $\rightarrow$ `{"R_OBJT_ACCESS", "object_id"}`
     - `"COLL_TOKEN_NAMESPACE"` $\rightarrow$ `{"R_TOKN_MAIN", "token_namespace"}`
     - `"RESC_ACCESS_NAME"` $\rightarrow$ `{"R_TOKN_MAIN", "token_name"}`
     - `"RESC_ACCESS_RESC_ID"` $\rightarrow$ `{"R_OBJT_ACCESS", "object_id"}`
     - `"RESC_TOKEN_NAMESPACE"` $\rightarrow$ `{"R_TOKN_MAIN", "token_namespace"}`
     - `"RULE_EXEC_CONTEXT"` $\rightarrow$ `{"R_RULE_EXEC", "exe_context"}`
     - `"RESC_GROUP_NAME"` $\rightarrow$ `{"R_RESC_MAIN", "resc_name"}`
     - `"DATA_RESC_GROUP_NAME"` $\rightarrow$ `{"R_RESC_MAIN", "resc_name"}`
     - `"AUDIT_OBJ_ID"` $\rightarrow$ `{"R_OBJT_AUDIT", "object_id"}`
     - `"AUDIT_USER_ID"` $\rightarrow$ `{"R_OBJT_AUDIT", "user_id"}`
     - `"AUDIT_ACTION_ID"` $\rightarrow$ `{"R_OBJT_AUDIT", "action_id"}`
     - `"AUDIT_COMMENT"` $\rightarrow$ `{"R_OBJT_AUDIT", "r_comment"}`
     - `"AUDIT_CREATE_TIME"` $\rightarrow$ `{"R_OBJT_AUDIT", "create_ts"}`
     - `"AUDIT_MODIFY_TIME"` $\rightarrow$ `{"R_OBJT_AUDIT", "modify_ts"}`
2. In `server/genquery2/src/genquery2_sql.cpp`:
   - Add the corresponding alias disambiguations in `resolve_table_alias` (`DATA_USER_NAME`/`DATA_ZONE_NAME` $\rightarrow$ `"pdu"`, `DATA_ACCESS_NAME`/`DATA_TOKEN_NAMESPACE` $\rightarrow$ `"pdt"`, `DATA_ACCESS_DATA_ID` $\rightarrow$ `"pdoa"`, `COLL_USER_NAME`/`COLL_ZONE_NAME` $\rightarrow$ `"pcu"`, `COLL_ACCESS_NAME`/`COLL_TOKEN_NAMESPACE` $\rightarrow$ `"pct"`, `COLL_ACCESS_COLL_ID` $\rightarrow$ `"pcoa"`, `RESC_ACCESS_NAME`/`RESC_TOKEN_NAMESPACE` $\rightarrow$ `"prt"`, `RESC_ACCESS_RESC_ID` $\rightarrow$ `"proa"`, `R_OBJT_AUDIT` $\rightarrow$ `"au"`) and add `R_OBJT_AUDIT` join edges in `init_database_table_graph`.

- [ ] **Step 4: Add `genquery1_to_genquery2_ast` unit tests in `unit_tests/src/test_genquery1_flex_bison_parser.cpp`, build, Run tests, and commit**

1. In `unit_tests/src/test_genquery1_flex_bison_parser.cpp`:
   - Include `"irods/genquery1_ast_adapter.hpp"` and `"irods/genquery2_builder.hpp"`.
   - Add a `TEST_CASE("genquery1_to_genquery2_ast translator")` testing projections, aggregates + automatic `group_by`, `order_by` + default ordering (`COLL_NAME`, `DATA_NAME`, `DATA_REPL_NUM`), `NO_DISTINCT`, `UPPER_CASE_WHERE`, numeric cast `n>`/`n<`/`n=`, compound `||`/`&&` conditions, `between`, `in`, `not like`, `parent_of`, `begin_of`, and round-trip via `to_query_string`.
2. Build and run unit tests:
   ```bash
   cmake --build /home/darkfell/dev/irods/build --target irods_genquery1_flex_bison_parser irods_genquery2_builder irods_server -j$(nproc)
   /home/darkfell/dev/irods/build/unit_tests/irods_genquery1_flex_bison_parser
   /home/darkfell/dev/irods/build/unit_tests/irods_genquery2_builder
   ```
3. Commit with `git commit --no-verify`.

---

### Task 2: Route `_rsGenQuery` Through GenQuery2, Retire `general_query.cpp` & `general_query_setup.cpp`, & Run Full Verification Gate

**Files:**
- Modify: `server/api/src/rsGenQuery.cpp`
- Modify: `server/icat/include/irods/icatHighLevelRoutines.hpp`
- Modify: `server/icat/src/icatHighLevelRoutines.cpp`
- Modify: `server/core/include/irods/irods_database_constants.hpp`
- Modify: `plugins/database/src/db_plugin.cpp`
- Modify: `plugins/database/CMakeLists.txt`
- Delete: `plugins/database/src/general_query.cpp`
- Delete: `plugins/database/src/general_query_setup.cpp`

- [ ] **Step 1: Re-implement `_rsGenQuery` in `server/api/src/rsGenQuery.cpp` using GenQuery2**

1. In `server/api/src/rsGenQuery.cpp`:
   - Include `"irods/genquery1_ast_adapter.hpp"`, `"irods/genquery2_builder.hpp"`, `"irods/private/genquery2_sql.hpp"`, `"irods/rsGetRescQuota.hpp"`, `<nlohmann/json.hpp>`, `<mutex>`, and `<unordered_map>`.
   - Maintain a thread-safe continuation table (`std::mutex` + `std::unordered_map<int, continuation_state>`) for paginated GenQuery1 queries (`continueInx > 0`).
   - In `_rsGenQuery(rsComm_t* rsComm, genQueryInp_t* genQueryInp, genQueryOut_t** genQueryOut)`:
     - Allocate and zero-initialize `*genQueryOut = static_cast<genQueryOut_t*>(std::calloc(1, sizeof(genQueryOut_t)))`.
     - Handle `genQueryInp->continueInx > 0 && genQueryInp->maxRows <= 0` (close query): remove `genQueryInp->continueInx` from the continuation table and return `0`.
     - Execute `acPreProcForGenQuery` via `applyRuleUpdateParams` (preserving existing lines 387–412).
     - Handle `genQueryInp->options & QUOTA_QUERY`: extract `COL_USER_NAME` and optional `COL_R_RESC_NAME` from `genQueryInp->sqlCondInp` (stripping `=` and quotes) and call `getQuotaByResc(rsComm, user_name.c_str(), resc_name.empty() ? nullptr : resc_name.c_str(), genQueryOut)`.
     - Preserve `resc_grp_attr_pos` and `resc_hier_attr_pos` handling (lines 419–629) so deprecated `RESC_GROUP_NAME` / `DATA_RESC_GROUP_NAME` synthetic columns continue to work identically.
     - Translate `*genQueryInp` to `auto select_ast = irods::experimental::genquery2::genquery1_to_genquery2_ast(*genQueryInp);`.
     - Determine pagination offset (from continuation table if `genQueryInp->continueInx > 0`, else `std::max(0, genQueryInp->rowOffset)`).
     - If `genQueryInp->maxRows > 0`, request `select_ast.range.limit = genQueryInp->maxRows + 1;` and `select_ast.range.offset = current_offset;` (so we know whether more rows remain after `maxRows`). If `genQueryInp->maxRows <= 0` and `genQueryInp->continueInx == 0`, set `select_ast.range.limit = 0;` (return 0 rows, unless `RETURN_TOTAL_ROW_COUNT` is set).
     - Configure `irods::experimental::genquery2::sql_generation_options sql_opts` using `rsComm->clientUser` (`authInfo.authFlag >= LOCAL_PRIV_USER_AUTH` or `getValByKey(&genQueryInp->condInput, DISABLE_STRICT_ACL_KW) != nullptr` $\rightarrow$ `admin_mode = true`), ticket string if any, and configured database technology (`"postgres"`, `"mysql"`, or `"oracle"`).
     - Compile SQL via `auto [sql, values] = irods::experimental::genquery2::to_sql(select_ast, sql_opts);` and execute via `chl_execute_genquery2_sql(*rsComm, sql, values, json_output)`.
     - Parse `json_output` (`nlohmann::json::parse(json_output)`):
       - If `genQueryInp->maxRows > 0` and `rows.size() > static_cast<std::size_t>(genQueryInp->maxRows)`:
         - Erase the extra lookahead row (`rows.erase(rows.begin() + genQueryInp->maxRows, rows.end())`).
         - If `genQueryInp->options & AUTO_CLOSE`, set `(*genQueryOut)->continueInx = -1;` (and erase any continuation state).
         - Otherwise allocate/update `continueInx` in the continuation table with `next_offset = current_offset + genQueryInp->maxRows` and set `(*genQueryOut)->continueInx = continue_inx`.
       - Else erase any existing `continueInx` from the continuation table and set `(*genQueryOut)->continueInx = 0`.
       - If `rows.empty()`, return `CAT_NO_ROWS_FOUND` (cleaning up `*genQueryOut`).
       - Populate `*genQueryOut` using `irods::experimental::genquery2::rows_to_genquery_out(rows.dump(), col_ids, *genQueryOut)`.
       - Restore `resc_grp_attr_pos` / `resc_hier_attr_pos` synthetic columns if requested (preserving existing post-processing logic).
       - Run `acPostProcForGenQuery` and return `0`.

- [ ] **Step 2: Remove `chlGenQuery*`, `db_gen_query*`, `general_query.cpp`, and `general_query_setup.cpp`**

1. In `server/icat/include/irods/icatHighLevelRoutines.hpp` and `server/icat/src/icatHighLevelRoutines.cpp`:
   - Remove `chlGenQuery`, `chlGenQueryAccessControlSetup`, `chlGenQueryTicketSetup`, `sTableInit`, `sFklink`, `sTable`, and `sColumn`.
2. In `server/core/include/irods/irods_database_constants.hpp`:
   - Remove `DATABASE_OP_GEN_QUERY`, `DATABASE_OP_GEN_QUERY_ACCESS_CONTROL_SETUP`, and `DATABASE_OP_GEN_QUERY_TICKET_SETUP` if unused.
3. In `plugins/database/src/db_plugin.cpp`:
   - Remove the two `chlGenQueryTicketSetup` calls in `db_mod_ticket_op` (around lines 12071 and 12078).
   - Remove `db_gen_query_op`, `db_gen_query_access_control_setup_op`, `db_gen_query_ticket_setup_op` and their registrations in `plugin_factory`.
4. In `plugins/database/CMakeLists.txt`:
   - Remove `${CMAKE_CURRENT_SOURCE_DIR}/src/general_query.cpp` and `${CMAKE_CURRENT_SOURCE_DIR}/src/general_query_setup.cpp`.
5. Run `git rm plugins/database/src/general_query.cpp plugins/database/src/general_query_setup.cpp`.

- [ ] **Step 3: Build 100% of targets, run unit tests, run `scripts/verify_refactoring.sh --skip-build`, and commit**

1. Run full build:
   ```bash
   cmake --build /home/darkfell/dev/irods/build -j$(nproc)
   ```
   Expected: `[100%] Built target ...` with 0 errors.
2. Run `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build` to ingest all modified TUs into Project Insight CPG and verify Catch2 + concurrency + typestate invariants.
3. Commit with `git commit --no-verify` and push `irods6` to `fork/irods6`.
