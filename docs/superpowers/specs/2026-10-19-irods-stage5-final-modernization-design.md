# iRODS 5.1 Stage 5: Final GenQuery1 & `splitPathByKey` Elimination and Collection Iteration Modernization

## 1. Overview & Motivation
Following Stages 1–4C, only a small number of internal `rsGenQuery` / `rcGenQuery` call sites, `queryHandle_t` collection iteration routines, and `splitPathByKey` stack-buffer splits remain across the iRODS codebase. Stage 5 completes the sweep in four atomic commits:

1. **GenQuery2 Rule Base, DVM, FNM, Microservice, `parent_of`, and `genQueryOut_t` Conversion Support**:
   - Extend [`genquery2_table_column_mappings.hpp`](../../../../server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp) and [`genquery2_sql.cpp`](../../../../server/genquery2/src/genquery2_sql.cpp) with column mappings and inner join edges for `R_RULE_MAIN` / `R_RULE_BASE_MAP`, `R_RULE_DVM` / `R_RULE_DVM_MAP`, `R_RULE_FNM` / `R_RULE_FNM_MAP`, and `R_MICROSRVC_MAIN` / `R_MICROSRVC_VER`.
   - Add `col::parent_of(std::string_view)` to [`genquery2_builder.hpp`](../../../../lib/core/include/irods/genquery2_builder.hpp) to construct ancestor path `IN (...)` predicates without Boost string splitting or heap-heavy regexes, plus `gq2::rows_to_genquery_out` to bridge GenQuery2 JSON row sets to legacy `genQueryOut_t*` C ABI boundaries where required.
   - Add unit tests in [`test_genquery2_sql_dml.cpp`](../../../../unit_tests/src/test_genquery2_sql_dml.cpp) and [`test_genquery2_builder.cpp`](../../../../unit_tests/src/test_genquery2_builder.cpp).

2. **Server API, Server Core, and Native Rule Engine Final `rsGenQuery` Elimination**:
   - [`server/api/src/rsGetRescQuota.cpp`](../../../../server/api/src/rsGetRescQuota.cpp): Migrate `getQuotaByResc` and `_rsGetRescQuota` to `gq2::builder` ASTs (`R_USER_MAIN`, `R_USER_GROUP`, `R_QUOTA_MAIN`, `R_RESC_MAIN`).
   - [`server/core/src/specColl.cpp`](../../../../server/core/src/specColl.cpp): Migrate `querySpecColl` and `getSpecCollCache` onto `gq2::builder` with `col("COLL_NAME").parent_of(objPath)`.
   - [`server/core/src/collection.cpp`](../../../../server/core/src/collection.cpp) & [`server/api/src/rsRmColl.cpp`](../../../../server/api/src/rsRmColl.cpp): Migrate `rsQueryCollInColl`, `rsQueryDataObjInCollReCur`, and `rsMvCollToTrash` pagination onto `gq2::builder` ASTs.
   - [`server/core/src/dataObjOpr.cpp`](../../../../server/core/src/dataObjOpr.cpp): Add a direct GenQuery2 fast-path in `getDataObjInfo` when no legacy `accessPerm` / `TICKET_KW` condInput check is requested.
   - [`plugins/rule_engines/irods_rule_language/src/nre.reLib1.cpp`](../../../../plugins/rule_engines/irods_rule_language/src/nre.reLib1.cpp): Migrate `readRuleSetFromDB`, `readRuleStructFromDB`, `readDVMapStructFromDB`, `readFNMapStructFromDB`, and `readMsrvcStructFromDB` (all 9 `rsGenQuery` calls) onto `gq2::builder` ASTs.

3. **Collection Iteration (`collection_iterator`, `miscUtil.cpp`, `lsUtil.cpp`) Modernization**:
   - [`lib/core/src/miscUtil.cpp`](../../../../lib/core/src/miscUtil.cpp): Migrate `queryCollInColl`, `queryDataObjInColl`, `getNextCollMetaInfo`, `getNextDataObjMetaInfo`, `queryDataObjAcl`, `queryCollAcl`, `queryCollInheritance`, and `getZoneType` from `rcGenQuery` / `queryHandle->genQuery` onto stateless GenQuery2 AST execution (`gq2::try_execute`), eliminating server statement-table cursor leaks during collection traversal.
   - [`lib/core/src/lsUtil.cpp`](../../../../lib/core/src/lsUtil.cpp): Migrate `lsDataObjUtilLong` from `rcGenQuery` to `gq2::builder` AST execution.
   - [`lib/filesystem/include/irods/filesystem/collection_iterator.hpp`](../../../../lib/filesystem/include/irods/filesystem/collection_iterator.hpp) & [`lib/filesystem/src/collection_iterator.cpp`](../../../../lib/filesystem/src/collection_iterator.cpp): Add a direct GenQuery2 batched catalog iteration fast-path for standard and linked collections (`COLL_TYPE` empty or `LINK_POINT_COLL_STR`), bypassing `collEnt_t` / `genQueryOut_t` C struct allocations while preserving `rsOpenCollection` / `rclOpenCollection` fallback for mounted/tar special collections.

4. **Final `splitPathByKey` Elimination**:
   - Replace the final `splitPathByKey` stack-buffer calls in [`lib/core/src/lsUtil.cpp`](../../../../lib/core/src/lsUtil.cpp), [`lib/core/src/miscUtil.cpp`](../../../../lib/core/src/miscUtil.cpp), [`lib/core/src/rsyncUtil.cpp`](../../../../lib/core/src/rsyncUtil.cpp), [`lib/api/src/rcExecMyRule.cpp`](../../../../lib/api/src/rcExecMyRule.cpp), [`server/re/src/irods_re_structs.cpp`](../../../../server/re/src/irods_re_structs.cpp), and [`server/re/src/reDataObjOpr.cpp`](../../../../server/re/src/reDataObjOpr.cpp) with zero-copy [`irods::experimental::filesystem::path`](../../../../lib/filesystem/include/irods/filesystem/path.hpp) views (`parent_path_view()` / `object_name_view()`).
