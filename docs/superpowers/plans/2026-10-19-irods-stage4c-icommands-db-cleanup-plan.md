# iRODS Stage 4C: iCommands, Core Utilities, & Database Plugin Cleanup Plan

> **For agentic workers:** Execute each task in order, verifying compilation and unit tests before committing with `--no-verify`.

---

### Task 1: Support Ticket, User Credentials, and Session Token Columns in GenQuery2

**Files:**
- Modify: `server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp`
- Modify: `server/genquery2/src/genquery2_sql.cpp`
- Modify: `unit_tests/src/test_genquery2_sql_dml.cpp`

**Steps:**
1. Enable `R_TICKET_MAIN`, `R_TICKET_ALLOWED_HOSTS`, `R_TICKET_ALLOWED_USERS`, and `R_TICKET_ALLOWED_GROUPS` columns in `genquery2_table_column_mappings.hpp`, including `TICKET_DATA_COLL_NAME` (`{"R_COLL_MAIN", "coll_name"}`).
2. Add `R_USER_CREDENTIALS` (`USER_CRED_HASHED_PASSWORD`, `USER_CRED_SALT`, `USER_CRED_HASHING_ALGORITHM`, `USER_CRED_HASHING_PARAMETERS`) and `R_USER_SESSION_KEY` (`USER_SESSION_KEY`, `USER_SESSION_EXPIRY_TIME`, `USER_SESSION_SALT`) to `genquery2_table_column_mappings.hpp`.
3. Update `genquery2_sql.cpp`:
   - Add `R_TICKET_MAIN`, `R_TICKET_ALLOWED_HOSTS`, `R_TICKET_ALLOWED_USERS`, `R_TICKET_ALLOWED_GROUPS`, `R_USER_CREDENTIALS`, and `R_USER_SESSION_KEY` to `table_names` and `table_edges`.
   - Handle `TICKET_DATA_COLL_NAME` in `process_select` / `process_condition` so `R_COLL_MAIN` joins via `R_DATA_MAIN.coll_id` when querying ticket data objects.
   - Enforce `admin_mode` for `R_USER_CREDENTIALS` and `R_USER_SESSION_KEY`.
   - Enforce ticket owner filtering in `generate_condition_clause` when `!_opts.admin_mode` and `R_TICKET_MAIN` or `R_TICKET_ALLOWED_*` is queried.
4. Add unit tests in `unit_tests/src/test_genquery2_sql_dml.cpp` and verify `irods_genquery2_sql_dml` passes.

---

### Task 2: Database Plugin Auth, Rebalance, and Path Splitting Cleanup

**Files:**
- Modify: `plugins/database/src/db_plugin.cpp`

**Steps:**
1. Refactor `db_check_password_op` and `db_check_session_token_op` to use `gq2::builder` ASTs and `execute_catalog` with `get_session()`.
2. Replace `fmt::format` string interpolation in `db_get_distinct_data_objs_missing_from_child_given_parent_op`, `db_get_repl_list_for_leaf_bundles_op`, and `db_get_repl_list_for_leaf_bundles_offset_op` with parameterized bind values via `session.execute_cached`.
3. Replace remaining `splitPathByKey` calls in `db_plugin.cpp` with `fs::path::parent_path_view()` and `fs::path::object_name_view()`.
4. Build `irods_database_plugin-postgres` and verify.

---

### Task 3: Migrate `fsckUtil` and Server/Core `splitPathByKey` Calls

**Files:**
- Modify: `lib/core/src/fsckUtil.cpp`
- Modify: `lib/core/src/chksumUtil.cpp`
- Modify: `lib/core/src/cpUtil.cpp`
- Modify: `lib/core/src/getUtil.cpp`
- Modify: `lib/core/src/trimUtil.cpp`
- Modify: `server/api/src/rsDataObjCopy.cpp`
- Modify: `server/api/src/rsDataObjRename.cpp`
- Modify: `server/api/src/rsRmColl.cpp`
- Modify: `server/core/src/dataObjOpr.cpp`
- Modify: `server/core/src/fileOpr.cpp`
- Modify: `server/core/src/objMetaOpr.cpp`
- Modify: `server/core/src/physPath.cpp`

**Steps:**
1. Migrate `chkObjConsistency` in `lib/core/src/fsckUtil.cpp` from `rcGenQuery` to `gq2::builder` and `std::from_chars`.
2. Migrate `splitPathByKey` calls in `server/api`, `server/core`, and `lib/core` to `fs::path` zero-copy views.
3. Build `irods_client`, `irods_server`, and `ifsck`.

---

### Task 4: Migrate Remaining 8 iCommands to GenQuery2 ASTs

**Files:**
- Modify: `clients/icommands/src/icd.cpp`
- Modify: `clients/icommands/src/iqdel.cpp`
- Modify: `clients/icommands/src/igroupadmin.cpp`
- Modify: `clients/icommands/src/isysmeta.cpp`
- Modify: `clients/icommands/src/ilsresc.cpp`
- Modify: `clients/icommands/src/iquota.cpp`
- Modify: `clients/icommands/src/iticket.cpp`
- Modify: `clients/icommands/src/imeta.cpp`

**Steps:**
1. Migrate all catalog queries in `icd.cpp`, `iqdel.cpp`, `igroupadmin.cpp`, `isysmeta.cpp`, `ilsresc.cpp`, `iquota.cpp`, `iticket.cpp`, and `imeta.cpp` onto `gq2::builder` ASTs and `gq2::try_execute`.
2. Build all 8 icommands and run all GenQuery2 unit tests.
