# iRODS 5.1 Modernization — Stage 4A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `server/core` (`initServer.cpp`, `dataObjOpr.cpp`, `objMetaOpr.cpp`, `collection.cpp`, `specColl.cpp`, `physPath.cpp`) and `server/api` (`rsDataObjOpen.cpp`, `rsDataObjRepl.cpp`, `rsDataObjUnlink.cpp`, `rsDataObjChksum.cpp`, `rsObjStat.cpp`, `rsRmColl.cpp`, `rsGeneralAdmin.cpp`) onto `gq2::builder` ASTs, zero-copy `fs::path` views, and RAII in cleanly separated, rebase-ready commits.

---

### Task 1: Extend GenQuery2 SQL Lowering (`R_TOKN_MAIN`, Resource ACLs, Access Type Aliases)
**Files:**
- Modify: `server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp`
- Modify: `server/genquery2/src/genquery2_sql.cpp`
- Modify: `unit_tests/src/test_genquery2_sql_dml.cpp`
- [ ] Enable `TOKEN_*` mappings, add `RESC_ACCESS_PERM_ID` / `RESC_ACCESS_TYPE` / `RESC_ACCESS_USER_ID` (`proa` join), and add `DATA_ACCESS_TYPE` / `COLL_ACCESS_TYPE` aliases.
- [ ] Add unit tests in `test_genquery2_sql_dml.cpp` and verify `irods_genquery2_sql_dml` passes.
- [ ] Commit as a standalone `feat(genquery2):` commit.

### Task 2: Refactor `initServer.cpp` and `dataObjOpr.cpp`
**Files:**
- Modify: `server/core/src/initServer.cpp`
- Modify: `server/core/src/dataObjOpr.cpp`
- [ ] Refactor `initZone` in `initServer.cpp` and `chkOrphanFile` + `getDataObjInfo` null-guard in `dataObjOpr.cpp`.
- [ ] Commit as a standalone `refactor(initServer,dataObjOpr):` commit.

### Task 3: Refactor `objMetaOpr.cpp`
**Files:**
- Modify: `server/core/src/objMetaOpr.cpp`
- [ ] Refactor `isData`, `getPhyPath`, `isColl`, `isUser`, `isResc`, `getTokenId`, `getUserId`, `checkPermitForDataObject`, `checkPermitForCollection`, `checkPermitForResource`, and `checkPermissionByObjType` onto `gq2::builder::select` ASTs, zero-copy `fs::path` views, and single-query `USER_GROUP_ID` lookup.
- [ ] Commit as a standalone `refactor(objMetaOpr):` commit.

### Task 4: Refactor `collection.cpp`, `specColl.cpp`, and `physPath.cpp`
**Files:**
- Modify: `server/core/src/collection.cpp`
- Modify: `server/core/src/specColl.cpp`
- Modify: `server/core/src/physPath.cpp`
- [ ] Refactor `checkCollAccessPerm`, `collStat`, and `syncCollPhyPath` onto `gq2::builder::select` ASTs + `gq2::try_execute`.
- [ ] Commit as a standalone `refactor(collection,physPath):` commit.

### Task 5: Refactor `rsDataObjOpen.cpp`, `rsDataObjRepl.cpp`, `rsDataObjUnlink.cpp`, and `rsDataObjChksum.cpp`
**Files:**
- Modify: `server/api/src/rsDataObjOpen.cpp`
- Modify: `server/api/src/rsDataObjRepl.cpp`
- Modify: `server/api/src/rsDataObjUnlink.cpp`
- Modify: `server/api/src/rsDataObjChksum.cpp`
- [ ] Remove `#include "irods/irods_query.hpp"` and refactor `user_has_permission_to_replicate_object`, `getNumSubfilesInBunfileObj`, and `verifyVaultSizeEqualsDatabaseSize` onto `gq2::builder::select` ASTs.
- [ ] Commit as a standalone `refactor(server/api):` commit for data object operations.

### Task 6: Refactor `rsObjStat.cpp`, `rsRmColl.cpp`, and `rsGeneralAdmin.cpp`
**Files:**
- Modify: `server/api/src/rsObjStat.cpp`
- Modify: `server/api/src/rsRmColl.cpp`
- Modify: `server/api/src/rsGeneralAdmin.cpp`
- [ ] Refactor `dataObjStat`, `rsMvCollToTrash`, and rebalance AVU helpers onto `gq2::builder::select` ASTs, zero-copy `fs::path` views, and stack/non-strdup `modAVUMetadataInp_t` initialization.
- [ ] Build `irods_server`, run all unit tests, and commit as a standalone `refactor(rsObjStat,rsRmColl,rsGeneralAdmin):` commit.
