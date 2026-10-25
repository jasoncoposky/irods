# Stage 13: Client Utilities (`lib/core/src/*Util.cpp`) & iCommands (`clients/icommands/`) Modernization Design Specification

**Date:** 2026-10-24
**Status:** Approved for Execution
**Target Branch:** `simply_collections`
**Master Roadmap Reference:** Stage 13 (`docs/superpowers/specs/2026-10-23-irods-master-modernization-roadmap.md`)

---

## 1. Objective & Scope

Modernize the 20 client utility implementation files in [`lib/core/src/*Util.cpp`](../../../lib/core/src/miscUtil.cpp) and the 51 CLI tool implementations in [`clients/icommands/src/`](../../../clients/icommands/src/iadmin.cpp) to eliminate resource leaks (`collHandle_t`, `keyValPair_t`, `genQueryOut_t`, `dataObjInp_t`, heap buffers, and file descriptors), migrate legacy C numeric parsing (`atoi`, `strtoll`) to C++17 `std::from_chars`, and replace unsafe C buffer manipulations (`bzero`, `strcat`, `strncat`, `strcpy`, `strdup`) with RAII and modern C++20 / `fmt` idioms while preserving 100% of external C/C++ ABI signatures in `lib/core/include/irods/*Util.h`.

---

## 2. Audit Findings

### 2.1 Already Completed in Prior Stages
- **Filesystem Iteration**: Local directory traversal in `miscUtil.cpp`, `putUtil.cpp`, `rsyncUtil.cpp`, `scanUtil.cpp`, and `fsckUtil.cpp` already uses `std::filesystem::directory_iterator`.
- **GenQuery2 Adoption**: Catalog queries in `miscUtil.cpp`, `lsUtil.cpp`, `scanUtil.cpp`, `fsckUtil.cpp`, `iadmin.cpp`, `imeta.cpp`, `iticket.cpp`, `igroupadmin.cpp`, `isysmeta.cpp`, and `iqstat.cpp` already use `irods::experimental::genquery2::builder`.

### 2.2 Resource Leaks & Typestate Bugs in `lib/core/src/*Util.cpp`
1. **[`miscUtil.cpp`](../../../lib/core/src/miscUtil.cpp)**:
   - `getCollSizeForProgStat` (lines 2095–2133): Opens `collHandle` via `rclOpenCollection` and never calls `rclCloseCollection(&collHandle)` on either normal completion or recursive early return.
   - `get_resc_hier_from_leaf_id` (lines 1701–1721): Allocates `get_hier_out_t* out = nullptr;` via `_query_handle->getHierForId(...)`, copies `out->hier_` with `rstrcpy`, and never frees `out`.
   - `queryCollAclSpecific` (lines 747–772): Pre-allocates `*genQueryOut = (genQueryOut_t*)malloc(sizeof(genQueryOut_t))` before `rcSpecificQuery(conn, &specificQueryInp, genQueryOut)` overwrites the pointer (leaking the `malloc`), and never calls `clearKeyVal(&specificQueryInp.condInput)`.
2. **[`chksumUtil.cpp`](../../../lib/core/src/chksumUtil.cpp)**:
   - `chksumDataObjUtil` (lines 95–130): Leaks `chksumStr` when `status == CHECK_VERIFICATION_RESULTS` (line 117) or when `status < 0` and `chksumStr != nullptr`.
   - `chksumCollUtil` (lines 213–248): Re-opens `collHandle` on line 213 and never calls `rclCloseCollection(&collHandle)` after the loop or on early return at line 235.
3. **[`cpUtil.cpp`](../../../lib/core/src/cpUtil.cpp)**:
   - `cpCollUtil` (lines 300–388): Early return at line 361 leaks `collHandle` without calling `rclCloseCollection(&collHandle)`.
4. **[`getUtil.cpp`](../../../lib/core/src/getUtil.cpp)**:
   - `getUtil` (lines 40–203): Early returns at lines 73 and 148 leak `rodsRestart.fd` when opened by `initCondForGet`.
   - `getCollUtil` (lines 384–529): Early return at line 488 leaks `collHandle` without calling `rclCloseCollection(&collHandle)`.
5. **[`putUtil.cpp`](../../../lib/core/src/putUtil.cpp)**:
   - `putUtil` (lines 148–379): Early returns leak `rodsRestart.fd` when opened by `initCondForPut`.
   - `bulkPutDirUtil` (lines 908–940): `bulkOprInfo.bytesBuf.buf = malloc(BULK_OPR_BUF_SIZE)` is leaked if `putDirUtil` returns `< 0` on line 916 or if `bulkOprInfo.count == 0` on line 919 (`freeBBuf` is only called inside `sendBulkPut`).
   - `bulkPutFileUtil` (lines 976–1021): File descriptor `in_fd` is leaked on error return at line 995.
6. **[`phymvUtil.cpp`](../../../lib/core/src/phymvUtil.cpp)**:
   - `phymvUtil` (lines 20–70): Never calls `clearDataObjInp(&dataObjInp)` on exit.
   - `phymvCollUtil` (lines 150–211): Guard `collHandle` with `irods::at_scope_exit`.
7. **[`replUtil.cpp`](../../../lib/core/src/replUtil.cpp)**:
   - `replUtil` (lines 23–99): Never calls `clearDataObjInp(&dataObjInp)` or closes `rodsRestart.fd` on exit.
   - `replCollUtil` (lines 213–305): Early return at line 274 leaks `collHandle`.
8. **[`rsyncUtil.cpp`](../../../lib/core/src/rsyncUtil.cpp)**:
   - `rsyncIrrToLocalUtil` (lines 684–804) & `rsyncCollToCollUtil` (lines 1011–1131): Early returns at lines 759 and 1087 leak `collHandle` without calling `rclCloseCollection(&collHandle)`.
9. **[`trimUtil.cpp`](../../../lib/core/src/trimUtil.cpp)**:
   - `trimUtil` (lines 19–68): Never calls `clearDataObjInp(&dataObjInp)` on exit.
   - `trimCollUtil` (lines 180–255): Early return at line 248 leaks `collHandle`.
10. **[`bunUtil.cpp`](../../../lib/core/src/bunUtil.cpp)** & **[`regUtil.cpp`](../../../lib/core/src/regUtil.cpp)**:
    - `bunUtil` (lines 11–73): Never calls `clearKeyVal(&structFileExtAndRegInp.condInput)` on exit.
    - `regUtil` (lines 11–68): Never calls `clearDataObjInp(&dataObjOprInp)` on exit.

### 2.3 Resource Leaks & Legacy Patterns in `clients/icommands/src/*.cpp`
1. **[`iadmin.cpp`](../../../clients/icommands/src/iadmin.cpp)**:
   - `list_logical_quotas` (line 1062): Replace `std::strtoll` with `std::from_chars`.
   - `setScrambledPw` (line 1370) / `igroupadmin.cpp` (line 236): Clean up password buffer padding.
2. **[`imeta.cpp`](../../../clients/icommands/src/imeta.cpp)**:
   - `modCopyAVUMetadata` (lines 761–867) and `modAVUMetadata` (lines 873–934): Guard `modAVUMetadataInp.condInput` with `irods::at_scope_exit{[&modAVUMetadataInp] { clearKeyVal(&modAVUMetadataInp.condInput); }}`.
   - `modCopyAVUMetadata` (lines 840–846): Replace off-by-one `strncat(tempName, myEnv.rodsZone, MAX_NAME_LEN - strlen(tempName))` with `const auto tempName = fmt::format("/{}", myEnv.rodsZone);`.
3. **[`iticket.cpp`](../../../clients/icommands/src/iticket.cpp)**:
   - `doTicketOp` (lines 350–394): Eliminate `strdup`/`free` for `arg1..arg5` and guard `ticketAdminInp.condInput` with `irods::at_scope_exit{[&ticketAdminInp] { clearKeyVal(&ticketAdminInp.condInput); }}`.
   - `printResultsAndSubQuery` (lines 105–117): Print `"none"` or `to_utc_timestamp(val)` directly without copying into a fixed `char localTime[TIME_LEN]` via `strcpy`.
4. **[`isysmeta.cpp`](../../../clients/icommands/src/isysmeta.cpp)**:
   - `doMod`, `doModDatatype`, `doModComment` (lines 195–273): Guard `regParam` with `irods::at_scope_exit{[&regParam] { clearKeyVal(&regParam); }}`.
   - `main` (line 408): Replace `atoi(cmdToken[3])` with `std::from_chars`.
5. **[`irule.cpp`](../../../clients/icommands/src/irule.cpp)**:
   - `main` (lines 174–545): Guard `execMyRuleInp.condInput`, `rodsPathInp` (`freeRodsPathInpMembers`), `myargv[0]`, and `fptr` (`std::fclose`) with RAII scope guards (`irods::at_scope_exit`) so local rule files are closed and temporary strings/paths are freed.
   - `parseParameters` (lines 585–757): Guard `strArray` (`std::free(strArray.value)`) with `irods::at_scope_exit`.
6. **[`irsync.cpp`](../../../clients/icommands/src/irsync.cpp)**:
   - `main` (lines 57–83): Replace `strdup` and in-place `strcpy(argv[...], ".")` with pointer adjustment (`argv[...] += 2` and `const_cast<char*>(".")`) so zero heap memory is leaked.
7. **[`ips.cpp`](../../../clients/icommands/src/ips.cpp)**:
   - `main` (lines 78–86): Guard `procStatInp.condInput` with `irods::at_scope_exit{[&procStatInp] { clearKeyVal(&procStatInp.condInput); }}`.
   - `printProcStat` (line 183): Replace `atoi` with `std::from_chars`.
   - `initCondForProcStat` (line 249): Replace deprecated POSIX `bzero` with `std::memset`.
8. **[`ierror.cpp`](../../../clients/icommands/src/ierror.cpp)**:
   - `main` (line 29): Replace `std::atoi` with `std::from_chars`.
9. **[`iqmod.cpp`](../../../clients/icommands/src/iqmod.cpp)**:
   - `modDelayedRule` (lines 21–51): Guard `ruleExecModInp.condInput` with `irods::at_scope_exit{[&ruleExecModInp] { clearKeyVal(&ruleExecModInp.condInput); }}`.
10. **[`iquest.cpp`](../../../clients/icommands/src/iquest.cpp)**:
    - `queryAndShowStrCond` (lines 192–216): Call `freeGenQueryOut(&genQueryOut)` before each pagination `rcGenQuery` call in the `while` loop so intermediate pages are not leaked.
    - `execAndShowSpecificQuery` (lines 223–311): Guard `specificQueryInp.condInput` (`clearKeyVal`) and `genQueryOut` (`freeGenQueryOut`) with `irods::at_scope_exit`, and free `genQueryOut` before each pagination `rcSpecificQuery` call in the `while` loop.

---

## 3. Architecture & Verification Strategy

1. **Task Decomposition**:
   - **Task 1**: `miscUtil.cpp` & `lsUtil.cpp` RAII leak fixes (`getCollSizeForProgStat`, `get_resc_hier_from_leaf_id`, `queryCollAclSpecific`, `lsCollUtil`) and `std::from_chars` numeric parsing migration (`atoi`/`strtoll`).
   - **Task 2**: Remaining `*Util.cpp` files (`bunUtil.cpp`, `chksumUtil.cpp`, `cpUtil.cpp`, `getUtil.cpp`, `phymvUtil.cpp`, `putUtil.cpp`, `regUtil.cpp`, `replUtil.cpp`, `rsyncUtil.cpp`, `trimUtil.cpp`) — RAII `irods::at_scope_exit` guards for `collHandle_t`, `dataObjInp_t`, `keyValPair_t`, `chksumStr`, `bulkOprInfo.bytesBuf.buf`, and file descriptors (`rodsRestart.fd`, `in_fd`), plus `std::from_chars` migration in `rsyncUtil.cpp`.
   - **Task 3**: iCommands Part 1 (`iadmin.cpp`, `imeta.cpp`, `iticket.cpp`, `isysmeta.cpp`) — RAII `keyValPair_t` guards, `strdup` elimination, `std::from_chars` migration, and buffer safety.
   - **Task 4**: iCommands Part 2 (`irule.cpp`, `irsync.cpp`, `ips.cpp`, `ierror.cpp`, `iqmod.cpp`, `iquest.cpp`) + Full Build, Unit Tests, and CPG Verification Gate (`scripts/verify_refactoring.sh`).
2. **Verification Gate**:
   - Full Ninja build of `irods_client`, `irods_server`, and all 51 iCommands binaries.
   - Execution of standalone Catch2 unit tests.
   - Execution of `/home/darkfell/dev/irods/scripts/verify_refactoring.sh` (Project Insight CPG ingestion, concurrency check, and typestate check).
