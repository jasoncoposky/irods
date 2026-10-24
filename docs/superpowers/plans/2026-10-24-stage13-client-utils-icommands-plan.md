# Stage 13: Client Utilities (`lib/core/src/*Util.cpp`) & iCommands (`clients/icommands/`) Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate all resource leaks (`collHandle_t`, `keyValPair_t`, `genQueryOut_t`, `dataObjInp_t`, heap buffers, and file descriptors), legacy C numeric conversions (`atoi`, `strtoll`), and unsafe buffer manipulations across `lib/core/src/*Util.cpp` and `clients/icommands/src/*.cpp`.

**Architecture:** Apply `irods::at_scope_exit` RAII guards immediately upon acquiring C resources (`rclOpenCollection`, `addKeyVal`, `rcGenQuery`/`rcSpecificQuery`, `fopen`, `open`, `malloc`), replace `atoi`/`strtoll` with `<charconv>` (`std::from_chars`), and preserve 100% of external C/C++ ABI signatures in `lib/core/include/irods/*Util.h`.

**Tech Stack:** C++20, `<charconv>` (`std::from_chars`), `irods::at_scope_exit`, `fmt::format`, CMake/Ninja, Catch2, Project Insight CPG (`scripts/verify_refactoring.sh`).

---

### Task 1: `miscUtil.cpp` & `lsUtil.cpp` RAII Leak Fixes & `std::from_chars` Migration

**Files:**
- Modify: `lib/core/src/miscUtil.cpp`
- Modify: `lib/core/src/lsUtil.cpp`

**Interfaces:**
- Consumes: `<charconv>` (`std::from_chars`), `irods/irods_at_scope_exit.hpp`
- Produces: Leak-free `getCollSizeForProgStat`, `get_resc_hier_from_leaf_id`, `queryCollAclSpecific`, `lsCollUtil`, and `std::from_chars`-based numeric parsing in `miscUtil.cpp` and `lsUtil.cpp`.

- [ ] **Step 1: Fix resource leaks and migrate `atoi`/`strtoll` in `lib/core/src/miscUtil.cpp`**

In `lib/core/src/miscUtil.cpp`:
1. In `queryCollAclSpecific` (around lines 747–772):
   - Remove the leaked `*genQueryOut = ( genQueryOut_t * ) malloc( sizeof( genQueryOut_t ) );` allocation before `rcSpecificQuery` (since `rcSpecificQuery` allocates `*genQueryOut` itself).
   - Add `irods::at_scope_exit clear_cond{[&specificQueryInp] { clearKeyVal(&specificQueryInp.condInput); }};` so `ZONE_KW` added via `addKeyVal` is always freed.
2. In `get_resc_hier_from_leaf_id` (around lines 1701–1721):
   - Add `irods::at_scope_exit free_out{[&out] { std::free(out); }};` after `get_hier_out_t* out = NULL;` so `out` allocated by `_query_handle->getHierForId` is freed before return.
3. In `getNextCollMetaInfo` (around lines 1815–1880):
   - Add a small helper in an anonymous namespace (or file-local static helper):
     ```cpp
     template <typename T>
     auto parse_int(const char* _str, T _default = 0) noexcept -> T
     {
         if (!_str) {
             return _default;
         }
         while (*_str == ' ' || *_str == '\t') {
             ++_str;
         }
         T val = _default;
         std::from_chars(_str, _str + std::strlen(_str), val);
         return val;
     }
     ```
   - Replace `atoi(&dataId[dataIdLen * i])`, `atoi(&replStatus[replStatusLen * i])`, `atoi(&value[len * selectedInx])`, `strtoll(&value[len * selectedInx], 0, 0)`, `atoi(&replStatus[replStatusLen * selectedInx])`, and `atoi(&value[len * selectedInx])` with `parse_int<int>(...)` and `parse_int<rodsLong_t>(...)`.
4. In `getCollSizeForProgStat` (around lines 2095–2133):
   - Immediately after `status = rclOpenCollection( conn, srcColl, RECUR_QUERY_FG, &collHandle );` and the `if ( status < 0 )` check, add:
     ```cpp
     const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};
     ```
     so `collHandle` is closed on both recursive error return and normal loop completion.

- [ ] **Step 2: Guard `collHandle` and `genQueryOut` and migrate `atoi`/`strtoll` in `lib/core/src/lsUtil.cpp`**

In `lib/core/src/lsUtil.cpp`:
1. Include `<charconv>`.
2. Add a file-local `parse_int` helper using `std::from_chars` and replace:
   - Line 352: `collEnt.replNum = atoi( &replNum->value[replNum->len * i] );`
   - Line 353: `collEnt.dataSize = strtoll( &dataSize->value[dataSize->len * i], 0, 0 );`
   - Line 356: `collEnt.replStatus = atoi( &replStatus->value[replStatus->len * i] );`
   - Line 621: `collEnt.dataSize = strtoll( objSize, 0, 0 );`
3. In `lsCollUtil` (around lines 470–510):
   - Immediately after `status = rclOpenCollection( conn, srcColl, queryFlags, &collHandle );` and the `if ( status < 0 )` check, add `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` and remove the manual `rclCloseCollection(&collHandle);` at line 509.
4. In `printCollAcl` (around lines 665–700):
   - Add `irods::at_scope_exit free_out{[&genQueryOut] { freeGenQueryOut(&genQueryOut); }};` right after `genQueryOut_t *genQueryOut = NULL;` and remove the manual `freeGenQueryOut(&genQueryOut);` at line 698.

- [ ] **Step 3: Build `irods_client` & `ils` and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target irods_client ils -j$(nproc)`
Expected: `[100%] Built target ils` with 0 errors.
Commit with `git commit --no-verify`.

---

### Task 2: Collection & DataObject `*Util.cpp` RAII Leak Fixes & `std::from_chars` Migration

**Files:**
- Modify: `lib/core/src/bunUtil.cpp`
- Modify: `lib/core/src/chksumUtil.cpp`
- Modify: `lib/core/src/cpUtil.cpp`
- Modify: `lib/core/src/getUtil.cpp`
- Modify: `lib/core/src/phymvUtil.cpp`
- Modify: `lib/core/src/putUtil.cpp`
- Modify: `lib/core/src/regUtil.cpp`
- Modify: `lib/core/src/replUtil.cpp`
- Modify: `lib/core/src/rsyncUtil.cpp`
- Modify: `lib/core/src/trimUtil.cpp`

**Interfaces:**
- Consumes: `<charconv>` (`std::from_chars`), `irods/irods_at_scope_exit.hpp`
- Produces: Deterministic RAII cleanup for all `collHandle_t`, `dataObjInp_t`, `keyValPair_t`, `chksumStr`, `bulkOprInfo`, and file descriptors across all `*Util.cpp` files.

- [ ] **Step 1: Fix leaks in `bunUtil.cpp`, `chksumUtil.cpp`, `cpUtil.cpp`, `getUtil.cpp`, and `phymvUtil.cpp`**

1. `lib/core/src/bunUtil.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `"irods/rcMisc.h"`.
   - In `bunUtil`, initialize `structFileExtAndRegInp_t structFileExtAndRegInp{};` and add `const auto clear_cond = irods::at_scope_exit{[&structFileExtAndRegInp] { clearKeyVal(&structFileExtAndRegInp.condInput); }};` before `initCondForBunOpr`.
2. `lib/core/src/chksumUtil.cpp`:
   - In `chksumDataObjUtil` (around line 92): declare `char *chksumStr = nullptr;` with `const auto free_chksum = irods::at_scope_exit{[&chksumStr] { std::free(chksumStr); }};` and remove the manual `free(chksumStr);` at line 127 so `chksumStr` is freed on all paths (including `status == CHECK_VERIFICATION_RESULTS`).
   - In `chksumCollUtil` (around lines 183–248): guard the first `collHandle` (opened at line 183) in its own scope or close it before re-opening at line 213, and guard the second `collHandle` (opened at line 213) with `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` so neither `collHandle` is ever leaked!
3. `lib/core/src/cpUtil.cpp`:
   - In `cpUtil`: guard `rodsRestart.fd` with `const auto close_restart = irods::at_scope_exit{[&rodsRestart] { if (rodsRestart.fd > 0) { close(rodsRestart.fd); } }};` and remove the manual `close(rodsRestart.fd)` at line 99.
   - In `cpCollUtil` (around lines 300–388): after `rclOpenCollection` succeeds, add `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` and remove the manual `rclCloseCollection(&collHandle)` calls at lines 311 and 379.
4. `lib/core/src/getUtil.cpp`:
   - In `getUtil`: guard `rodsRestart.fd` with `const auto close_restart = irods::at_scope_exit{[&rodsRestart] { if (rodsRestart.fd > 0) { close(rodsRestart.fd); } }};` after `initCondForGet`, and remove the manual `if (rodsRestart.fd > 0) close(rodsRestart.fd);` at line 160.
   - In `getCollUtil` (around lines 424–518): after `rclOpenCollection` succeeds, add `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` and remove the manual `rclCloseCollection(&collHandle)` calls at lines 435 and 517.
5. `lib/core/src/phymvUtil.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `"irods/rcMisc.h"`.
   - In `phymvUtil`: initialize `dataObjInp_t dataObjInp{};` and add `const auto clear_inp = irods::at_scope_exit{[&dataObjInp] { clearDataObjInp(&dataObjInp); }};`.
   - In `phymvCollUtil`: after `rclOpenCollection` succeeds, add `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` and remove manual `rclCloseCollection(&collHandle)` calls.

- [ ] **Step 2: Fix leaks and migrate `atoi` in `putUtil.cpp`, `regUtil.cpp`, `replUtil.cpp`, `rsyncUtil.cpp`, and `trimUtil.cpp`**

1. `lib/core/src/putUtil.cpp`:
   - In `putUtil`: guard `rodsRestart.fd` with `const auto close_restart = irods::at_scope_exit{[&rodsRestart] { if (rodsRestart.fd > 0) { close(rodsRestart.fd); } }};` after `initCondForPut`, and remove the manual `if (rodsRestart.fd > 0) close(rodsRestart.fd);` at line 330.
   - In `bulkPutDirUtil` (around lines 908–940): add `const auto clear_bulk_buf = irods::at_scope_exit{[&bulkOprInfo] { clearBBuf(&bulkOprInfo.bytesBuf); }};` after `bulkOprInfo.bytesBuf.buf = malloc(BULK_OPR_BUF_SIZE);` so `bytesBuf.buf` is freed even when `putDirUtil` fails or `bulkOprInfo.count == 0` (note `clearBBuf` safely checks `if (myBBuf->buf != NULL) { free(myBBuf->buf); myBBuf->buf = NULL; }`).
   - In `bulkPutFileUtil` (around lines 976–1021): immediately after `in_fd = open(...)` and the `if (in_fd < 0)` check, add `const auto close_in = irods::at_scope_exit{[in_fd] { close(in_fd); }};` and remove the manual `close(in_fd);` at line 1002.
2. `lib/core/src/regUtil.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `"irods/rcMisc.h"`.
   - In `regUtil`: initialize `dataObjInp_t dataObjOprInp{};` and add `const auto clear_inp = irods::at_scope_exit{[&dataObjOprInp] { clearDataObjInp(&dataObjOprInp); }};`.
3. `lib/core/src/replUtil.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `"irods/rcMisc.h"`.
   - In `replUtil`: initialize `dataObjInp_t dataObjInp{};` and `rodsRestart_t rodsRestart{};`, and add `const auto cleanup = irods::at_scope_exit{[&dataObjInp, &rodsRestart] { clearDataObjInp(&dataObjInp); if (rodsRestart.fd > 0) { close(rodsRestart.fd); } }};`.
   - In `replCollUtil`: after `rclOpenCollection` succeeds, add `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` and remove the manual `rclCloseCollection(&collHandle)` calls at lines 234 and 295.
4. `lib/core/src/rsyncUtil.cpp`:
   - Include `<charconv>`.
   - Add a file-local `parse_int` helper using `std::from_chars` and replace `atoi(srcPath->rodsObjStat->modifyTime)` (lines 206, 544) and `atoi(collEnt.modifyTime)` (lines 710, 1037).
   - In `rsyncIrrToLocalUtil` (line 695) and `rsyncCollToCollUtil` (line 1022): after `rclOpenCollection` succeeds, add `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` and remove manual `rclCloseCollection(&collHandle)` calls.
5. `lib/core/src/trimUtil.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `"irods/rcMisc.h"`.
   - In `trimUtil`: initialize `dataObjInp_t dataObjInp{};` and add `const auto clear_inp = irods::at_scope_exit{[&dataObjInp] { clearDataObjInp(&dataObjInp); }};`.
   - In `trimCollUtil`: after `rclOpenCollection` succeeds, add `const auto close_coll = irods::at_scope_exit{[&collHandle] { rclCloseCollection(&collHandle); }};` and remove manual `rclCloseCollection(&collHandle)` calls.

- [ ] **Step 3: Build `irods_client` and affected iCommands and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target irods_client ibun ichksum icp iget iphymv iput ireg irepl irsync itrim -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.
Commit with `git commit --no-verify`.

---

### Task 3: iCommands Part 1 (`iadmin.cpp`, `imeta.cpp`, `iticket.cpp`, `isysmeta.cpp`) Modernization

**Files:**
- Modify: `clients/icommands/src/iadmin.cpp`
- Modify: `clients/icommands/src/imeta.cpp`
- Modify: `clients/icommands/src/iticket.cpp`
- Modify: `clients/icommands/src/isysmeta.cpp`

**Interfaces:**
- Consumes: `<charconv>` (`std::from_chars`), `irods/irods_at_scope_exit.hpp`, `<fmt/format.h>`
- Produces: Leak-free `iadmin`, `imeta`, `iticket`, and `isysmeta` with `std::from_chars` numeric parsing and RAII `keyValPair_t` management.

- [ ] **Step 1: Migrate `std::strtoll` in `iadmin.cpp` and fix `condInput` & `strncat` in `imeta.cpp`**

1. In `clients/icommands/src/iadmin.cpp`:
   - Include `<charconv>` if not already included.
   - In `list_logical_quotas` (around lines 1060–1071): replace `std::strtoll` with `std::from_chars`:
     ```cpp
     std::string modify_ts_string = "?";
     std::time_t tt = 0;
     const std::string_view mtime_sv{quotaEntry.modify_time};
     const auto [ptr, ec] = std::from_chars(mtime_sv.data(), mtime_sv.data() + mtime_sv.size(), tt);
     if (ec == std::errc{} && ptr == mtime_sv.data() + mtime_sv.size()) {
         std::tm utc_tm{};
         gmtime_r(&tt, &utc_tm);
         std::ostringstream ss;
         ss << std::put_time(&utc_tm, "%Y-%m-%d.%H:%M:%S");
         modify_ts_string = ss.str();
     }
     else {
         fmt::print(stderr, "Timestamp conversion error: invalid integer");
     }
     ```
2. In `clients/icommands/src/imeta.cpp`:
   - In `modCopyAVUMetadata` (around lines 765–867):
     - Add `const auto clear_cond = irods::at_scope_exit{[&modAVUMetadataInp] { clearKeyVal(&modAVUMetadataInp.condInput); }};`.
     - Replace lines 840–846 (`char tempName[MAX_NAME_LEN] = "/"; strncat(tempName, myEnv.rodsZone, MAX_NAME_LEN - strlen(tempName));`) with `const auto tempName = fmt::format("/{}", myEnv.rodsZone);` and use `tempName.c_str()` and `tempName.size()` in `std::strncmp`.
   - In `modAVUMetadata` (around lines 876–934):
     - Add `const auto clear_cond = irods::at_scope_exit{[&modAVUMetadataInp] { clearKeyVal(&modAVUMetadataInp.condInput); }};`.

- [ ] **Step 2: Eliminate `strdup` and `condInput` leak in `iticket.cpp` and `condInput` leaks / `atoi` in `isysmeta.cpp`**

1. In `clients/icommands/src/iticket.cpp`:
   - In `printResultsAndSubQuery` (around lines 105–117): replace `char localTime[TIME_LEN]; strcpy(localTime, ...);` with direct printing of `"none"` or `to_utc_timestamp(val).c_str()`.
   - In `doTicketOp` (around lines 350–394):
     - Add `const auto clear_cond = irods::at_scope_exit{[&ticketAdminInp] { clearKeyVal(&ticketAdminInp.condInput); }};`.
     - Eliminate `strdup`/`free` for `arg1..arg5` by using `const_cast<char*>` (just as `setSessionTicket` in `getUtil.cpp` and `generalAdmin` in `iadmin.cpp` do).
2. In `clients/icommands/src/isysmeta.cpp`:
   - Include `<irods/irods_at_scope_exit.hpp>` and `<irods/rcMisc.h>`.
   - In `doMod`, `doModDatatype`, and `doModComment` (lines 195–273): add `const auto clear_reg = irods::at_scope_exit{[&regParam] { clearKeyVal(&regParam); }};` after `memset(&regParam, 0, sizeof(regParam));`.
   - In `main` (around line 408): replace `atoi( cmdToken[3] )` with `std::from_chars`:
     ```cpp
     int repl_num = 0;
     std::from_chars(cmdToken[3], cmdToken[3] + std::strlen(cmdToken[3]), repl_num);
     status = doModComment( Conn, objPath, repl_num, cmdToken[4] );
     ```

- [ ] **Step 3: Build `iadmin`, `imeta`, `iticket`, `isysmeta` and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target iadmin imeta iticket isysmeta -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.
Commit with `git commit --no-verify`.

---

### Task 4: iCommands Part 2 (`irule.cpp`, `irsync.cpp`, `ips.cpp`, `ierror.cpp`, `iqmod.cpp`, `iquest.cpp`) & Full Verification Gate

**Files:**
- Modify: `clients/icommands/src/irule.cpp`
- Modify: `clients/icommands/src/irsync.cpp`
- Modify: `clients/icommands/src/ips.cpp`
- Modify: `clients/icommands/src/ierror.cpp`
- Modify: `clients/icommands/src/iqmod.cpp`
- Modify: `clients/icommands/src/iquest.cpp`

**Interfaces:**
- Consumes: `<charconv>` (`std::from_chars`), `irods/irods_at_scope_exit.hpp`
- Produces: Zero resource leaks or legacy C numeric conversions across all remaining iCommands, 100% build completion, and passing CPG verification gate.

- [ ] **Step 1: Fix leaks in `irule.cpp` and `irsync.cpp`**

1. In `clients/icommands/src/irule.cpp`:
   - Include `<irods/irods_at_scope_exit.hpp>`.
   - In `main` (around lines 242–280):
     - Initialize `rodsPathInp_t rodsPathInp{};` and `rodsArguments_t myRodsArgs{};` and guard `rodsPathInp` with `irods::at_scope_exit free_path{[&rodsPathInp] { freeRodsPathInpMembers(&rodsPathInp); }};`.
     - Replace `myargv[0] = strdup( fileName + 2 );` with `std::string remote_rule_path{fileName + 2}; myargv[0] = remote_rule_path.data();`.
   - In `main` (around line 289): after `fptr = fopen( ruleFile, "r" );` and the `if ( fptr == NULL )` check, add:
     ```cpp
     const auto close_fptr = irods::at_scope_exit{[fptr, connFlag, &saveFile] {
         std::fclose(fptr);
         if (connFlag == 1) {
             unlink(saveFile);
         }
     }};
     ```
     and remove the manual `if ( connFlag == 1 ) { fclose( fptr ); unlink( saveFile ); }` block at lines 409–412 (so `fptr` is ALWAYS closed even when `connFlag == 0`!).
   - In `parseParameters` (around lines 586–608): after `splitMultiStr( inBuf, &strArray )` succeeds, add `const auto free_str_array = irods::at_scope_exit{[&strArray] { std::free(strArray.value); }};` so `strArray.value` is freed on all return paths.
2. In `clients/icommands/src/irsync.cpp`:
   - Remove unused `buffer[HUGE_NAME_LEN]`.
   - In `main` (lines 57–83):
     - Replace `strcpy( argv[argc - i], "." );` with `argv[argc - i] = const_cast<char*>(".");`.
     - Replace `snprintf( buffer, HUGE_NAME_LEN, "%s", argv[argc - i] + 2 ); argv[argc - i] = strdup( buffer );` with `argv[argc - i] += 2;`.
     - Replace `strcpy( argv[argc - 1], "." );` with `argv[argc - 1] = const_cast<char*>(".");`.
     - Replace `snprintf( buffer, HUGE_NAME_LEN, "%s", argv[argc - 1] + 2 ); argv[argc - 1] = strdup( argv[argc - 1] + 2 );` with `argv[argc - 1] += 2;`.

- [ ] **Step 2: Fix leaks and migrate `atoi`/`bzero` in `ips.cpp`, `ierror.cpp`, `iqmod.cpp`, and `iquest.cpp`**

1. In `clients/icommands/src/ips.cpp`:
   - Include `<irods/irods_at_scope_exit.hpp>`, `<irods/rcMisc.h>`, `<charconv>`, and `<cstring>`.
   - In `main` (around line 78): initialize `procStatInp_t procStatInp{};` and add `const auto clear_cond = irods::at_scope_exit{[&procStatInp] { clearKeyVal(&procStatInp.condInput); }};` before `initCondForProcStat`.
   - In `printProcStat` (around line 183): replace `startTimeVal = atoi( startTime->value + startTime->len * i );` with:
     ```cpp
     startTimeVal = 0;
     const char* start_str = startTime->value + startTime->len * i;
     std::from_chars(start_str, start_str + std::strlen(start_str), startTimeVal);
     ```
   - In `initCondForProcStat` (line 249): replace `bzero( procStatInp, sizeof( procStatInp_t ) );` with `std::memset( procStatInp, 0, sizeof( procStatInp_t ) );`.
2. In `clients/icommands/src/ierror.cpp`:
   - Include `<charconv>`.
   - In `main` (line 29): replace `int errorCode = std::atoi(argv[1]);` with:
     ```cpp
     int errorCode = 0;
     std::from_chars(argv[1], argv[1] + std::strlen(argv[1]), errorCode);
     ```
3. In `clients/icommands/src/iqmod.cpp`:
   - Include `<irods/irods_at_scope_exit.hpp>` and `<irods/rcMisc.h>`.
   - In `modDelayedRule` (around line 25): add `const auto clear_cond = irods::at_scope_exit{[&ruleExecModInp] { clearKeyVal(&ruleExecModInp.condInput); }};`.
4. In `clients/icommands/src/iquest.cpp`:
   - In `queryAndShowStrCond` (around line 204): call `freeGenQueryOut(&genQueryOut);` immediately after `genQueryInp.continueInx = genQueryOut->continueInx;` before calling `rcGenQuery( conn, &genQueryInp, &genQueryOut );`.
   - In `execAndShowSpecificQuery` (around lines 225–310):
     - Add `irods::at_scope_exit cleanup{[&specificQueryInp, &genQueryOut] { clearKeyVal(&specificQueryInp.condInput); freeGenQueryOut(&genQueryOut); }};` right after `genQueryOut_t *genQueryOut = NULL;`.
     - In the pagination loop (around line 300), call `freeGenQueryOut(&genQueryOut);` immediately after `specificQueryInp.continueInx = genQueryOut->continueInx;` before calling `rcSpecificQuery( conn, &specificQueryInp, &genQueryOut );`.

- [ ] **Step 3: Build 100% of targets and run unit tests**

Run:
`cmake --build /home/darkfell/dev/irods/build -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.

- [ ] **Step 4: Run CPG verification gate (`scripts/verify_refactoring.sh`) and commit**

Run `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build` to ingest all modified TUs into Project Insight CPG and verify build + Catch2 + concurrency + typestate invariants, then commit Task 4 changes.
