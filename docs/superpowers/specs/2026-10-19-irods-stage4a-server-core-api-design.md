# iRODS 5.1 Modernization — Stage 4A: Server Core & Data Object APIs Design Specification

## 1. Overview & Goals
Stage 4A refactors the remaining legacy GenQuery1 (`rsGenQuery`, `irods::query`) and manual C buffer/memory patterns across `server/core` and `server/api` onto the GenQuery2 Fluent Builder (`irods::experimental::genquery2::builder`), direct Server AST execution (`gq2::try_execute` / `gq2::execute`), zero-copy `irods::experimental::filesystem::path` views (`parent_path_view()`, `object_name_view()`), `std::from_chars`, and deterministic RAII.

Every logical subsystem is committed as an atomic, self-contained commit so changes can be reviewed, cherry-picked, or rebased independently.

## 2. Architecture & Component Design

### 2.1 GenQuery2 SQL Lowering Extensions (`server/genquery2`)
- **Token Table Mappings (`R_TOKN_MAIN`)**: Enable `TOKEN_ID`, `TOKEN_NAMESPACE`, `TOKEN_NAME`, `TOKEN_VALUE`, `TOKEN_VALUE2`, `TOKEN_VALUE3`, and `TOKEN_COMMENT` in `genquery2_table_column_mappings.hpp`.
- **Resource ACL Mappings & Access Type Aliases**:
  - Add `RESC_ACCESS_PERM_ID`, `RESC_ACCESS_TYPE` (`{"R_OBJT_ACCESS", "access_type_id"}`), and `RESC_ACCESS_USER_ID` (`{"R_OBJT_ACCESS", "user_id"}`), joining `R_OBJT_ACCESS proa on <R_RESC_MAIN>.resc_id = proa.object_id` in `generate_joins_for_permissions` and `get_special_table_alias_for_column`.
  - Add `DATA_ACCESS_TYPE` (alias for `DATA_ACCESS_PERM_ID`) and `COLL_ACCESS_TYPE` (alias for `COLL_ACCESS_PERM_ID`).

### 2.2 `server/core` Modernization
- **`initServer.cpp` (`initZone`)**:
  - Replace `rsGenQuery` with `gq2::builder::select({"ZONE_NAME", "ZONE_TYPE", "ZONE_CONNECTION", "ZONE_COMMENT"})` and `gq2::try_execute`, eliminating 4 `genQueryOut` memory leaks on error paths and removing `#include "irods/rsGenQuery.hpp"`.
- **`dataObjOpr.cpp` (`chkOrphanFile`, `getDataObjInfo`)**:
  - Migrate `chkOrphanFile` to `gq2::builder::select({"DATA_ID", "COLL_NAME", "DATA_NAME", "DATA_REPL_NUM", "DATA_RESC_ID"})` executed under `irods::experimental::scoped_privileged_client`, removing global `AGENT_CONN_KW` server property mutation and fixing 4 `genQueryOut` memory leaks on error returns.
  - Guard `genQueryOut != nullptr` before accessing `genQueryOut->continueInx` on error in `getDataObjInfo`.
- **`objMetaOpr.cpp` (`isData`, `getPhyPath`, `isColl`, `isUser`, `isResc`, `getTokenId`, `getUserId`, `checkPermitForDataObject`, `checkPermitForCollection`, `checkPermitForResource`, `checkPermissionByObjType`)**:
  - Replace `splitPathByKey` into 1024-byte stack buffers with zero-copy `fs::path` (`parent_path_view()`, `object_name_view()`).
  - Replace all 11 `rsGenQuery` call sites with `gq2::builder::select` ASTs + `gq2::try_execute` + `std::from_chars`.
  - Fix `genQueryOut`/`genQueryInp` memory leaks in `isData`, `getPhyPath`, and `isColl`.
  - Eliminate the N+1 group lookup in `checkPermissionByObjType` by selecting `USER_GROUP_ID` directly in a single `gq2::builder::select({"USER_GROUP_ID"})` query.
- **`collection.cpp`, `specColl.cpp`, and `physPath.cpp`**:
  - Migrate `checkCollAccessPerm` and `collStat` in `collection.cpp` to `gq2::builder::select` ASTs, adding a JSON-backed helper for `queueSpecCollCache` while preserving the C ABI of `queueSpecCollCache(rsComm_t*, genQueryOut_t*, const char*)` and fixing `genQueryOut` / `*rodsObjStatOut` leaks.
  - Migrate `syncCollPhyPath` in `physPath.cpp` to `gq2::builder::select` + `gq2::try_execute` + `std::from_chars`, fixing 7 `genQueryOut` leaks on error paths and removing `#include "irods/rsGenQuery.hpp"`.

### 2.3 `server/api` Modernization
- **`rsDataObjOpen.cpp`, `rsDataObjRepl.cpp`, `rsDataObjUnlink.cpp`, `rsDataObjChksum.cpp`**:
  - Remove unused `#include "irods/irods_query.hpp"` from `rsDataObjOpen.cpp`.
  - Collapse the 3 catalog queries in `user_has_permission_to_replicate_object` (`rsDataObjRepl.cpp`) into a single `gq2::builder::select({"DATA_ACCESS_PERM_ID"})` AST query using `DATA_ACCESS_USER_NAME` and `DATA_ACCESS_USER_ZONE` (which automatically expands group memberships via `R_USER_GROUP`).
  - Migrate `getNumSubfilesInBunfileObj` (`rsDataObjUnlink.cpp`) to a single `gq2::builder::select(gq2::builder::count("DATA_ID"))` AST query.
  - Migrate `verifyVaultSizeEqualsDatabaseSize` (`rsDataObjChksum.cpp`) to `gq2::builder::select({"DATA_SIZE"})` + `gq2::try_execute` + `std::from_chars`.
- **`rsObjStat.cpp`, `rsRmColl.cpp`, `rsGeneralAdmin.cpp`**:
  - Migrate `dataObjStat` (`rsObjStat.cpp`) to zero-copy `fs::path` views and `gq2::builder::select`, fixing 10 `genQueryOut` memory leaks.
  - Migrate `rsMvCollToTrash` (`rsRmColl.cpp`) to `gq2::builder::select` + `gq2::try_execute`.
  - Migrate `_check_rebalance_timestamp_avu_on_resource` and `_remove_rebalance_timestamp_avu_from_resource` (`rsGeneralAdmin.cpp`) to `gq2::builder::select` + `gq2::try_execute`, fixing the `gen_out`/`gen_inp` leak and eliminating 12 `strdup`/`free` heap allocations in `_set_rebalance_timestamp_avu_on_resource` and `_remove_rebalance_timestamp_avu_from_resource`.
