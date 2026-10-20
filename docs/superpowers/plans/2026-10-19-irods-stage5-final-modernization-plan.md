# iRODS 5.1 Stage 5 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate the remaining internal GenQuery1 (`rsGenQuery` / `rcGenQuery`) and `splitPathByKey` call sites across `server/api`, `server/core`, `plugins/rule_engines`, `lib/core`, `lib/api`, `server/re`, and `lib/filesystem`, and upgrade collection iteration onto stateless GenQuery2 ASTs.

---

### Task 1: GenQuery2 Rule/Microservice Tables, `col::parent_of`, and `rows_to_genquery_out`
- [ ] Add `RULE_*`, `RULE_BASE_MAP_*`, `DVM_*`, `DVM_BASE_MAP_*`, `FNM_*`, `FNM_BASE_MAP_*`, and `MSRVC_*` column mappings to `server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp`.
- [ ] Add `R_RULE_MAIN`, `R_RULE_BASE_MAP`, `R_RULE_DVM`, `R_RULE_DVM_MAP`, `R_RULE_FNM`, `R_RULE_FNM_MAP`, `R_MICROSRVC_MAIN`, and `R_MICROSRVC_VER` to `table_names`, `table_edges`, and `table_joins` in `server/genquery2/src/genquery2_sql.cpp`.
- [ ] Add `col::parent_of(std::string_view)` and `rows_to_genquery_out` to `lib/core/include/irods/genquery2_builder.hpp`.
- [ ] Add unit tests in `unit_tests/src/test_genquery2_sql_dml.cpp` and `unit_tests/src/test_genquery2_builder.cpp` and verify they pass.

### Task 2: Migrate Server API, Server Core, and Native Rule Engine `rsGenQuery` Callers
- [ ] Migrate `getQuotaByResc` in `server/api/src/rsGetRescQuota.cpp` to `gq2::builder` ASTs.
- [ ] Migrate `querySpecColl` and `getSpecCollCache` in `server/core/src/specColl.cpp` to `gq2::builder` ASTs using `col("COLL_NAME").parent_of(objPath)`.
- [ ] Migrate `rsQueryCollInColl`, `rsQueryDataObjInCollReCur`, and `rsMvCollToTrash` in `server/core/src/collection.cpp` and `server/api/src/rsRmColl.cpp` to `gq2::builder` ASTs.
- [ ] Add direct GenQuery2 fast-path in `getDataObjInfo` (`server/core/src/dataObjOpr.cpp`) when no legacy `accessPerm` / `TICKET_KW` condInput check is requested.
- [ ] Migrate all 9 `rsGenQuery` calls in `plugins/rule_engines/irods_rule_language/src/nre.reLib1.cpp` to `gq2::builder` ASTs.

### Task 3: Modernize Collection Iteration (`miscUtil.cpp`, `lsUtil.cpp`, `collection_iterator`)
- [ ] Migrate `queryDataObjAcl`, `queryCollAcl`, `queryCollInheritance`, `getZoneType`, `queryCollInColl`, `queryDataObjInColl`, `getNextCollMetaInfo`, and `getNextDataObjMetaInfo` in `lib/core/src/miscUtil.cpp` to GenQuery2 ASTs.
- [ ] Migrate `lsDataObjUtilLong` in `lib/core/src/lsUtil.cpp` to GenQuery2 ASTs.
- [ ] Add direct GenQuery2 batched catalog iteration in `lib/filesystem/src/collection_iterator.cpp` and `lib/filesystem/include/irods/filesystem/collection_iterator.hpp` for standard and linked collections while preserving special collection fallback.

### Task 4: Eliminate Final `splitPathByKey` Call Sites
- [ ] Replace `splitPathByKey` with `irods::experimental::filesystem::path` zero-copy views (`parent_path_view()` / `object_name_view()`) in `lib/core/src/lsUtil.cpp`, `lib/core/src/miscUtil.cpp`, `lib/core/src/rsyncUtil.cpp`, `lib/api/src/rcExecMyRule.cpp`, `server/re/src/irods_re_structs.cpp`, and `server/re/src/reDataObjOpr.cpp`.
