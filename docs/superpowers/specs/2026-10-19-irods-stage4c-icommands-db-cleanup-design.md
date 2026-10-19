# iRODS Stage 4C: iCommands, Core Utilities, & Database Plugin Cleanup Design

## 1. Executive Summary
Following the completion of Stage 4A (`server/core` and `server/api`) and Stage 4B (plugins, rule engines, `lib/core`, and `iuserinfo`/`iadmin`/`iqstat`), **Stage 4C** completes the codebase-wide migration to the **GenQuery2 Fluent Builder (`gq2::builder`)**, direct AST execution (`gq2::try_execute` / `gq2::execute` / `execute_catalog`), zero-copy `irods::experimental::filesystem::path` views, and zero-allocation `std::from_chars` numeric parsing:

1. **GenQuery2 Catalog Schema Completion (`R_TICKET_*`, `R_USER_CREDENTIALS`, `R_USER_SESSION_KEY`)**:
   - Enable `R_TICKET_MAIN`, `R_TICKET_ALLOWED_HOSTS`, `R_TICKET_ALLOWED_USERS`, and `R_TICKET_ALLOWED_GROUPS` columns and joins in [`genquery2_table_column_mappings.hpp`](../../../server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp) and [`genquery2_sql.cpp`](../../../server/genquery2/src/genquery2_sql.cpp), including `TICKET_DATA_COLL_NAME` (which joins `R_COLL_MAIN` to `R_DATA_MAIN.coll_id` rather than `R_TICKET_MAIN.object_id`) and non-admin ticket ownership filtering.
   - Add admin-only `R_USER_CREDENTIALS` (`USER_CRED_*`) and `R_USER_SESSION_KEY` (`USER_SESSION_*`) mappings and joins so `db_check_password_op` and `db_check_session_token_op` in `db_plugin.cpp` can execute via `gq2::builder` and the session's LRU prepared statement cache.
2. **Database Plugin Final Cleanup (`plugins/database/src/db_plugin.cpp`)**:
   - Migrate `db_check_password_op` and `db_check_session_token_op` from raw `nanodbc::prepare` SQL strings to `gq2::builder` ASTs executed through `execute_catalog`.
   - Parameterize `*_invocation_timestamp`, `*_parent`, and `*_child` in the replication rebalance queries (`db_get_distinct_data_objs_missing_from_child_given_parent_op`, `db_get_repl_list_for_leaf_bundles_op`, `db_get_repl_list_for_leaf_bundles_offset_op`), eliminating `fmt::format` SQL interpolation.
   - Replace remaining `splitPathByKey` calls in `db_plugin.cpp` with zero-copy `fs::path::parent_path_view()` and `fs::path::object_name_view()`.
3. **Server & Core Utilities (`fsckUtil.cpp`, `rsRmColl.cpp`, and `splitPathByKey` Modernization)**:
   - Migrate `chkObjConsistency` in [`lib/core/src/fsckUtil.cpp`](../../../lib/core/src/fsckUtil.cpp) from legacy `genQueryInp_t` (`COL_D_DATA_PATH`, `COL_DATA_SIZE`, `COL_D_DATA_CHECKSUM`, `COL_DATA_NAME`, `COL_COLL_NAME`, `COL_D_RESC_HIER`) to `gq2::builder` ASTs and `std::from_chars`.
   - Replace `splitPathByKey` buffer copies across `server/api`, `server/core`, and `lib/core` with `irods::experimental::filesystem::path` zero-copy views (`parent_path_view()` and `object_name_view()`).
4. **Remaining iCommands Migration (`icd`, `iqdel`, `igroupadmin`, `isysmeta`, `ilsresc`, `iquota`, `iticket`, `imeta`)**:
   - Migrate all remaining iCommands (`icd.cpp`, `iqdel.cpp`, `igroupadmin.cpp`, `isysmeta.cpp`, `ilsresc.cpp`, `iquota.cpp`, `iticket.cpp`, `imeta.cpp`) from `rcGenQuery` and string-formatted GenQuery1 queries onto `gq2::builder` ASTs and `gq2::try_execute`.

---

## 2. Commit Decomposition
All work is organized into 5 atomic commits on `feature/filesystem-modernization`:
1. `docs: add Stage 4C icommands, core utilities, and db_plugin cleanup spec and plan`
2. `feat(genquery2): support ticket, user credentials, and session token columns and joins`
3. `refactor(db_plugin): migrate auth queries, rebalance queries, and path splitting to GenQuery2 and fs::path`
4. `refactor(server,core): migrate fsckUtil and path splitting to GenQuery2 ASTs and zero-copy views`
5. `refactor(icommands): migrate icd, iqdel, igroupadmin, isysmeta, ilsresc, iquota, iticket, and imeta to GenQuery2 ASTs`
