# Stage 14: Server Core & Server API Handlers Modernization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate dead `setjmp`/`longjmp` (`jmp_buf Jenv`), fix CPG-identified file descriptor, socket, pipe, collection handle, and heap buffer leaks across `server/core/`, `server/api/`, and `plugins/api/`, migrate all `atoi`/`strtol`/`strtoll` calls in `server/core/` and `server/api/` to `std::from_chars`, and modernize `rsApiHandler.cpp` logging to `irods::experimental::log`.

**Architecture:** Use `irods::at_scope_exit` for deterministic RAII cleanup of file descriptors, sockets, pipes, `bytesBuf_t`, `keyValPair_t`, `dataObjInfo_t*`, and server collection handles (`rsCloseCollection`), and C++20 `<charconv>` (`std::from_chars`) for locale-independent integer parsing.

**Tech Stack:** C++20 (`<charconv>`, `<vector>`, `<string_view>`), `irods::at_scope_exit`, `irods::experimental::log`.

**Spec:** [`docs/superpowers/specs/2026-10-24-stage14-server-core-api-handlers-design.md`](file:///home/darkfell/dev/irods/docs/superpowers/specs/2026-10-24-stage14-server-core-api-handlers-design.md)

---

### Global Constraints

- **C++ Standard**: C++20 across all targets.
- **Comment & Documentation Integrity**: Preserve all existing comments and docstrings that are unrelated to the code changes.
- **Verification Gate**: Must pass `/home/darkfell/dev/irods/scripts/verify_refactoring.sh`.

---

### Task 1: `rsApiHandler` `setjmp`/`longjmp` Removal, `server/core/src/` Typestate Leak Fixes, & `std::from_chars` Migration

**Files:**
- Modify: `server/core/include/irods/rsApiHandler.hpp`
- Modify: `server/core/src/rsApiHandler.cpp`
- Modify: `server/core/src/server_utilities.cpp`
- Modify: `server/core/src/miscServerFunct.cpp`
- Modify: `server/core/src/dataObjOpr.cpp`
- Modify: `server/core/src/objDesc.cpp`
- Modify: `server/core/src/objMetaOpr.cpp`
- Modify: `server/core/src/physPath.cpp`
- Modify: `server/core/src/specColl.cpp`

- [ ] **Step 1: Remove `csetjmp`/`Jenv`/`readTimeoutHandler`, guard buffers with `irods::at_scope_exit`, and migrate `rodsLog` in `rsApiHandler.hpp` & `rsApiHandler.cpp`**

1. In `server/core/include/irods/rsApiHandler.hpp`:
   - Remove `void readTimeoutHandler(int sig);` (line 50).
2. In `server/core/src/rsApiHandler.cpp`:
   - Remove `#include <csetjmp>` and `jmp_buf Jenv;` (lines 20–22), and remove `readTimeoutHandler` (lines 776–791).
   - Include `"irods/irods_at_scope_exit.hpp"`.
   - In `rsApiHandler`:
     - Guard `myInStruct` and `myOutStruct` with `irods::at_scope_exit` right after `char *myInStruct = nullptr;` so they are cleaned up on every return path (including `!api_entry.get()`).
   - In `readAndProcClientMsg`:
     - Value-initialize `bytesBuf_t inputStructBBuf{}, bsBBuf{}, errorBBuf{};` and add `const auto clear_bufs = irods::at_scope_exit{[&inputStructBBuf, &bsBBuf, &errorBBuf] { clearBBuf(&inputStructBBuf); clearBBuf(&bsBBuf); clearBBuf(&errorBBuf); }};` right after declaration, removing the manual `clearBBuf` calls at lines 631–633.
   - Replace remaining `rodsLog` / `rodsLogError` calls in `rsApiHandler.cpp` with `log_agent::error`, `log_agent::notice`, `log_agent::debug`, or `log_server::error`.

- [ ] **Step 2: Fix CPG-detected FD and socket leaks in `server_utilities.cpp` and `miscServerFunct.cpp`**

1. In `server/core/src/server_utilities.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"`.
   - In `create_pid_file` (around lines 84–134):
     - Immediately after `if (fd == -1)` check, add:
       ```cpp
       bool keep_fd_open = false;
       const auto close_fd = irods::at_scope_exit{[fd, &keep_fd_open] {
           if (!keep_fd_open) {
               close(fd);
           }
       }};
       ```
     - Set `keep_fd_open = true;` right before `return 0;` at line 133 so `fd` is closed on all error paths while remaining open when the lock succeeds.
2. In `server/core/src/miscServerFunct.cpp`:
   - In `reconnManager` (around lines 2070–2105):
     - If `!ret.ok()` after `irods::network_factory(rsComm, net_obj)` (line 2073), call `close(newSock);` before `return;`.
     - Before `rsComm->reconnectedSock = newSock;` (line 2099), if `rsComm->reconnectedSock > 0`, call `close(rsComm->reconnectedSock);`.

- [ ] **Step 3: Migrate `atoi` and `strtoll` in `dataObjOpr.cpp`, `objDesc.cpp`, `objMetaOpr.cpp`, `physPath.cpp`, and `specColl.cpp` to `std::from_chars`**

1. `server/core/src/dataObjOpr.cpp` (line 722): replace `replNum = atoi( tmpStr );` with `std::from_chars`.
2. `server/core/src/objDesc.cpp` (line 299): replace `L1desc[l1descInx].openType = std::atoi(open_type);` with `std::from_chars`.
3. `server/core/src/objMetaOpr.cpp` (lines 126–141): guard `rodsObjStatOut` with `irods::at_scope_exit{[&rodsObjStatOut] { freeRodsObjStat(rodsObjStatOut); }}` and replace `*collId = strtoll( rodsObjStatOut->dataId, 0, 0 );` with `std::from_chars`.
4. `server/core/src/physPath.cpp` (line 577): replace `return atoi( myValue );` with `std::from_chars`.
5. `server/core/src/specColl.cpp` (line 538): replace `myDataObjInfo->dataId = strtoll( rodsObjStatOut->dataId, 0, 0 );` with `std::from_chars`.

- [ ] **Step 4: Build `irods_server` & `irods_server_utilities` and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target irods_server irods_server_utilities -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.
Commit with `git commit --no-verify`.

---

### Task 2: Server API Collection & Bundle Handler RAII Leak Fixes (`rsRmColl.cpp`, `rsStructFileBundle.cpp`, `rsUnbunAndRegPhyBunfile.cpp`)

**Files:**
- Modify: `server/api/src/rsRmColl.cpp`
- Modify: `server/api/src/rsStructFileBundle.cpp`
- Modify: `server/api/src/rsUnbunAndRegPhyBunfile.cpp`

- [ ] **Step 1: Fix collection handle & `condInput` leaks and migrate `atoi` in `rsRmColl.cpp`**

In `server/api/src/rsRmColl.cpp`:
- Include `"irods/irods_at_scope_exit.hpp"` and `<charconv>`.
- In `_rsPhyRmColl` (around lines 280–435):
  - After `handleInx = rsOpenCollection( rsComm, &openCollInp );` and `if ( handleInx < 0 )`, add:
    ```cpp
    bool coll_closed = false;
    const auto close_coll = irods::at_scope_exit{[rsComm, &handleInx, &coll_closed] {
        if (!coll_closed) {
            rsCloseCollection(rsComm, &handleInx);
        }
    }};
    ```
    and set `coll_closed = true;` after the explicit `rsCloseCollection( rsComm, &handleInx );` before `svrUnregColl` (around line 417), removing the redundant `rsCloseCollection` at line 307.
  - Value-initialize `dataObjInp_t dataObjInp{};` and `collInp_t tmpCollInp{};` and guard both `condInput`s with:
    ```cpp
    const auto clear_conds = irods::at_scope_exit{[&dataObjInp, &tmpCollInp] {
        clearKeyVal(&dataObjInp.condInput);
        clearKeyVal(&tmpCollInp.condInput);
    }};
    ```
    removing the manual `clearKeyVal` calls at lines 431–432.
  - Replace `atoi( tmpValue )` and `atoi( CollHandle[handleInx].rodsObjStat->modifyTime )` (lines 303–305) with `std::from_chars`.
  - Remove the duplicate `if ( ( tmpValue = getValByKey( &rmCollInp->condInput, AGE_KW ) ) != NULL )` block at lines 317–320.

- [ ] **Step 2: Fix `condInput`, `specColl`, and `l1descInx` leaks in `rsStructFileBundle.cpp`**

In `server/api/src/rsStructFileBundle.cpp`:
- Include `"irods/irods_at_scope_exit.hpp"`.
- In `phyBundle` (lines 40–90):
  - Initialize `structFileOprInp_t structFileOprInp{};` and `specColl_t specColl{};`, set `structFileOprInp.specColl = &specColl;` (eliminating `malloc`/`free` of `specColl_t`).
  - Guard `structFileOprInp.condInput` with `const auto clear_cond = irods::at_scope_exit{[&structFileOprInp] { clearKeyVal(&structFileOprInp.condInput); }};`.
  - Remove the duplicate `addKeyVal( &structFileOprInp.condInput, RESC_HIER_STR_KW, dataObjInfo->rescHier );` at line 66.
- In `_rsStructFileBundle` (lines 92–335):
  - Guard `dataObjInp.condInput` with `irods::at_scope_exit{[&dataObjInp] { clearKeyVal(&dataObjInp.condInput); }}`.
  - Guard `chkObjPermAndStatInp.condInput` with `irods::at_scope_exit{[&chkObjPermAndStatInp] { clearKeyVal(&chkObjPermAndStatInp.condInput); }}`.
  - Guard `collInp.condInput` with `irods::at_scope_exit{[&collInp] { clearKeyVal(&collInp.condInput); }}` and, if `rsOpenCollection` fails at line 218, close `l1descInx` via `rsDataObjClose` before returning `handleInx`!

- [ ] **Step 3: Fix `condInput` and `dataObjInfoHead` leaks in `rsUnbunAndRegPhyBunfile.cpp`**

In `server/api/src/rsUnbunAndRegPhyBunfile.cpp`:
- Include `"irods/irods_at_scope_exit.hpp"`.
- In `regUnbunPhySubfiles` (lines 128–233):
  - Value-initialize `dataObjInp_t dataObjInp{};` and `dataObjInp_t dataObjUnlinkInp{};`, and guard both with:
    ```cpp
    const auto clear_conds = irods::at_scope_exit{[&dataObjInp, &dataObjUnlinkInp] {
        clearKeyVal(&dataObjInp.condInput);
        clearKeyVal(&dataObjUnlinkInp.condInput);
    }};
    ```
  - Inside the `for ( directory_iterator itr( srcDirPath ); itr != end_itr; ++itr )` loop, right after `status = getDataObjInfo( rsComm, &dataObjInp, &dataObjInfoHead, NULL, 1 );` succeeds, guard `dataObjInfoHead` with:
    ```cpp
    const auto free_doi = irods::at_scope_exit{[&dataObjInfoHead] {
        freeAllDataObjInfo(dataObjInfoHead);
        dataObjInfoHead = nullptr;
    }};
    ```
    and remove the manual `freeAllDataObjInfo( dataObjInfoHead );` at line 223 so `dataObjInfoHead` is freed even when line 191 executes `continue;`!

- [ ] **Step 4: Build `irods_server` and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target irods_server -j$(nproc)`
Expected: `[100%] Built target irods_server` with 0 errors.
Commit with `git commit --no-verify`.

---

### Task 3: Server API Process/Auth/Bulk/DataObj Handler Leak Fixes, `std::from_chars` Migration, & Full Verification Gate

**Files:**
- Modify: `server/api/src/rsExecCmd.cpp`
- Modify: `server/api/src/rsPamAuthRequest.cpp`
- Modify: `server/api/src/rsBulkDataObjPut.cpp`
- Modify: `server/api/src/rsBulkDataObjReg.cpp`
- Modify: `server/api/src/rsDataObjCopy.cpp`
- Modify: `server/api/src/rsDataObjUnlink.cpp`
- Modify: `server/api/src/rsFileSyncToArch.cpp`
- Modify: `server/api/src/rsGetMiscSvrInfo.cpp`
- Modify: `server/api/src/rsGetRescQuota.cpp`
- Modify: `server/api/src/rsModDataObjMeta.cpp`
- Modify: `server/api/src/rsProcStat.cpp`
- Modify: `plugins/api/src/register_physical_path.cpp`

- [ ] **Step 1: Fix pipe, buffer, and `dataObjInfoHead` leaks in `rsExecCmd.cpp`, `rsPamAuthRequest.cpp`, and `register_physical_path.cpp`**

1. In `server/api/src/rsExecCmd.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"`.
   - In `rsExecCmd` (around lines 80–151):
     - Guard `dataObjInp.condInput` with `const auto clear_cond = irods::at_scope_exit{[&dataObjInp] { clearKeyVal(&dataObjInp.condInput); }};`.
     - Guard `dataObjInfoHead` with `const auto free_doi = irods::at_scope_exit{[&dataObjInfoHead] { freeAllDataObjInfo(dataObjInfoHead); dataObjInfoHead = nullptr; }};` after `getDataObjInfo`, and remove the manual `freeAllDataObjInfo( dataObjInfoHead );` at line 149.
   - In `_rsExecCmd` (around lines 231–365):
     - If `pipe(stderrFd) < 0`, close `stdoutFd[0]` and `stdoutFd[1]`.
     - If `pipe(statusFd) < 0`, close `stdoutFd[0]`, `stdoutFd[1]`, `stderrFd[0]`, and `stderrFd[1]`.
     - If `childPid < 0`, close all 6 pipe descriptors (`stdoutFd[0..1]`, `stderrFd[0..1]`, `statusFd[0..1]`).
     - At lines 346–352: call `clearBBuf(&statusBuf);` unconditionally after checking `if (statusBuf.len == sizeof(int) + 1 && statusBuf.buf != nullptr) { myExecCmdOut->status = *static_cast<int*>(statusBuf.buf); }` so `statusBuf.buf` is never leaked when the child exits normally!
2. In `server/api/src/rsPamAuthRequest.cpp`:
   - Remove the duplicate `#include "irods/miscServerFunct.hpp"` at line 10.
   - In `runPamAuthCheck` (lines 79–125):
     - If `fork()` returns `-1`, close `p2cp[0]` and `p2cp[1]` before returning `SYS_FORK_ERROR`.
     - In the parent branch (`if (pid)`), close `p2cp[0]` immediately so the parent's read-end descriptor is not leaked!
     - In the child branch (`else`), close `p2cp[0]` after `dup2(p2cp[0], 0)`.
3. In `plugins/api/src/register_physical_path.cpp`:
   - In `readPathnamePatternsFromFile` (lines 139–171):
     - Guard `fd` with `irods::at_scope_exit` calling `rsFileClose(_comm, &fileCloseInp)` immediately after `fd = rsFileOpen(_comm, &fileOpenInp)` succeeds.
     - Guard `fileReadBuf` with `const auto clear_read_buf = irods::at_scope_exit{[&fileReadBuf] { clearBBuf(&fileReadBuf); }};` so `fileReadBuf.buf` is freed on both error return and normal return after `readPathnamePatterns`!
   - In `filePathRegRepl` (lines 219–235): move `irods::at_scope_exit free_data_obj_info_head` immediately after `getDataObjInfo` succeeds (before `sortObjInfoForOpen`).

- [ ] **Step 2: Migrate all remaining `atoi`, `strtol`, `strtoll` calls in `server/api/src/` to `std::from_chars`**

Migrate `atoi`, `strtol`, and `strtoll` to `std::from_chars` (`#include <charconv>`) in:
1. `server/api/src/rsBulkDataObjPut.cpp` (lines 194, 922, 925)
2. `server/api/src/rsBulkDataObjReg.cpp` (lines 184, 187, 198)
3. `server/api/src/rsDataObjCopy.cpp` (line 326)
4. `server/api/src/rsDataObjUnlink.cpp` (lines 297, 298)
5. `server/api/src/rsFileSyncToArch.cpp` (line 137)
6. `server/api/src/rsGetMiscSvrInfo.cpp` (line 77)
7. `server/api/src/rsGetRescQuota.cpp` (lines 281, 282, 284)
8. `server/api/src/rsModDataObjMeta.cpp` (line 69)
9. `server/api/src/rsProcStat.cpp` (line 283)

Verify with `git grep -n "setjmp\|longjmp\|atoi(\|atof(\|strtol\|strtoll\|bzero(" server/core/ server/api/ plugins/api/` that 0 occurrences remain!

- [ ] **Step 3: Build 100% of targets and run unit tests**

Run:
`cmake --build /home/darkfell/dev/irods/build -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.

- [ ] **Step 4: Run CPG verification gate (`scripts/verify_refactoring.sh --skip-build`) and commit**

Run `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build` to ingest all modified Stage 13 review + Stage 14 TUs into Project Insight CPG and verify Catch2 + concurrency + typestate invariants (confirming `server_utilities.cpp` and `miscServerFunct.cpp` typestate leaks are gone!), then commit with `git commit --no-verify`.
