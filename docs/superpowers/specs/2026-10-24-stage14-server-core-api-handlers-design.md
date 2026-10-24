# Stage 14: Server Core & Server API Handlers (`server/core/`, `server/api/`, `plugins/api/`) Modernization — Design Specification

**Date**: 2026-10-24  
**Stage**: 14 of 17 ([Master Modernization Roadmap](file:///home/darkfell/dev/irods/docs/superpowers/specs/2026-10-23-irods-master-modernization-roadmap.md))  
**Repository**: `/home/darkfell/dev/irods`

---

## 1. Executive Summary & Audit Findings

An audit of `server/core/`, `server/api/`, and `plugins/api/` (combined with Project Insight CPG typestate analysis) revealed four high-priority modernization targets:

### 1.1 Dead `setjmp` / `longjmp` + `jmp_buf Jenv` Hazard (`rsApiHandler.cpp` / `rsApiHandler.hpp`)
- [`rsApiHandler.cpp`](file:///home/darkfell/dev/irods/server/core/src/rsApiHandler.cpp#L20-L22) defines a global `jmp_buf Jenv;` and [`readTimeoutHandler`](file:///home/darkfell/dev/irods/server/core/src/rsApiHandler.cpp#L776-L791) calls `longjmp(Jenv, L1DESC_INUSE)` / `longjmp(Jenv, READ_HEADER_TIMED_OUT)`.
- `readAndProcClientMsg` already uses socket timeout `struct timeval tv` in `readMsgHeader(net_obj, &myHeader, &tv)` and no longer calls `setjmp(Jenv)` anywhere in the codebase. Thus `Jenv` is permanently uninitialized, and calling `readTimeoutHandler` would trigger undefined behavior by jumping to an uninitialized `jmp_buf`.
- Removing `#include <csetjmp>`, `jmp_buf Jenv`, and `readTimeoutHandler` from `rsApiHandler.hpp` and `rsApiHandler.cpp` completely eliminates `<csetjmp>`/`longjmp` from the server.

### 1.2 CPG-Detected File Descriptor & Socket Leaks in `server/core/src/`
- **[`server_utilities.cpp`](file:///home/darkfell/dev/irods/server/core/src/server_utilities.cpp#L76-L134) (`create_pid_file`)**:
  - Opens `fd = open(pid_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR)` at line 84.
  - On success (`return 0`), `fd` must remain open for the lifetime of the daemon to hold the POSIX `fcntl(F_SETLK)` record lock.
  - However, on all five error return paths (`fcntl(F_GETFD)`, `fcntl(F_SETFD)`, `fcntl(F_SETLK)`, `ftruncate`, `write`), `create_pid_file` returns `-1` without closing `fd` (CPG typestate violation `RESOURCE_LEAK | fd | server_utilities.cpp:77`).
- **[`miscServerFunct.cpp`](file:///home/darkfell/dev/irods/server/core/src/miscServerFunct.cpp#L2048-L2105) (`reconnManager`)**:
  - Accepts `newSock = accept(...)` at line 2048.
  - If `irods::network_factory(rsComm, net_obj)` fails at line 2073, `reconnManager` returns without calling `close(newSock)` (CPG typestate violation `RESOURCE_LEAK | newSock | miscServerFunct.cpp:1971`).
  - At line 2099, `rsComm->reconnectedSock = newSock;` overwrites `rsComm->reconnectedSock` without closing any previously unconsumed socket (`if (rsComm->reconnectedSock > 0) { close(rsComm->reconnectedSock); }`).

### 1.3 Resource, Pipe, Collection Handle, & Buffer Leaks in `rsApiHandler.cpp`, `server/api/src/`, and `plugins/api/src/`
- **[`rsApiHandler.cpp`](file:///home/darkfell/dev/irods/server/core/src/rsApiHandler.cpp)**:
  - In `rsApiHandler` (lines 154–259): if `!api_entry.get()` at line 172, `myInStruct` unpacked at line 158 is leaked. Guard `myInStruct` and `myOutStruct` with `irods::at_scope_exit`.
  - In `readAndProcClientMsg` (lines 515–658): `inputStructBBuf`, `bsBBuf`, and `errorBBuf` populated by `readMsgBody` at line 616 are only cleared in the `RODS_API_REQ_T` branch (lines 631–633), leaking on `RODS_DISCONNECT_T`, `RODS_RECONNECT_T`, and unsupported message types. Guard all three `bytesBuf_t` with `irods::at_scope_exit`.
- **[`rsRmColl.cpp`](file:///home/darkfell/dev/irods/server/api/src/rsRmColl.cpp#L270-L435) (`_rsPhyRmColl`)**:
  - Opens collection `handleInx = rsOpenCollection(rsComm, &openCollInp)` at line 285 and populates `dataObjInp.condInput` and `tmpCollInp.condInput`.
  - Early error returns at lines 329, 337, 340, and 348 leak `handleInx` (never calling `rsCloseCollection`) and leak `dataObjInp.condInput` and `tmpCollInp.condInput`.
  - Lines 317–320 duplicate lines 311–313 (`addKeyVal` for `AGE_KW`).
- **[`rsStructFileBundle.cpp`](file:///home/darkfell/dev/irods/server/api/src/rsStructFileBundle.cpp)**:
  - In `phyBundle` (lines 40–90): `structFileOprInp.condInput` is populated with `addKeyVal` (including a duplicate `RESC_HIER_STR_KW` at line 66) and never cleared with `clearKeyVal(&structFileOprInp.condInput)`. Also replace heap `malloc`/`free` of `specColl_t` with a stack-allocated `specColl_t specColl{}`.
  - In `_rsStructFileBundle` (lines 92–335):
    - `dataObjInp.condInput` replicated at line 125 is leaked if line 135 (`OVERWRITE_WITHOUT_FORCE_FLAG`) or line 146 (`l1descInx < 0`) returns early.
    - `chkObjPermAndStatInp.condInput` is leaked if `rsChkObjPermAndStat` fails at line 187.
    - `collInp.condInput` has `RESC_ID_KW` added at line 209; if `rsOpenCollection` fails at line 218, `collInp.condInput` is leaked and `l1descInx` is not closed!
- **[`rsUnbunAndRegPhyBunfile.cpp`](file:///home/darkfell/dev/irods/server/api/src/rsUnbunAndRegPhyBunfile.cpp#L128-L233) (`regUnbunPhySubfiles`)**:
  - `dataObjInp.condInput` and `dataObjUnlinkInp.condInput` are not guarded by `irods::at_scope_exit` (`dataObjUnlinkInp.condInput` is never cleared even on normal return!).
  - At line 191 (`if (strcmp(dataObjInfoHead->rescName, BUNDLE_RESC) != 0)`), `continue` is executed without freeing `dataObjInfoHead` allocated by `getDataObjInfo` at line 172!
- **[`rsExecCmd.cpp`](file:///home/darkfell/dev/irods/server/api/src/rsExecCmd.cpp)**:
  - In `rsExecCmd` (lines 80–150): `dataObjInp.condInput` is never cleared (`clearKeyVal`), and `dataObjInfoHead` allocated at line 103 is leaked if `sortObjInfoForOpen`, `get_loc_for_hier_string`, or `get_resource_property` fails.
  - In `_rsExecCmd` (lines 222–365):
    - If `pipe(stderrFd)` fails, `stdoutFd[0..1]` are leaked; if `pipe(statusFd)` fails, `stdoutFd[0..1]` and `stderrFd[0..1]` are leaked; if `fork()` fails, all 6 pipe descriptors are leaked.
    - `readToByteBuf(statusFd[0], &statusBuf)` at line 347 always allocates `statusBuf.buf`, but `free(statusBuf.buf)` at line 351 is only called inside `if (statusBuf.len == sizeof(int) + 1)`, leaking `statusBuf.buf` on every normal command execution!
- **[`rsPamAuthRequest.cpp`](file:///home/darkfell/dev/irods/server/api/src/rsPamAuthRequest.cpp#L79-L125) (`runPamAuthCheck`)**:
  - If `fork()` fails at line 88, both `p2cp[0]` and `p2cp[1]` are leaked.
  - In the parent process (`if (pid)` at line 94), `p2cp[0]` (read end) is never closed, leaking a file descriptor on every PAM authentication check.
- **[`register_physical_path.cpp`](file:///home/darkfell/dev/irods/plugins/api/src/register_physical_path.cpp)**:
  - In `readPathnamePatternsFromFile` (lines 139–170): `fileReadBuf.buf` is leaked after `readPathnamePatterns` succeeds at line 168 (since `readPathnamePatterns` copies the buffer!), and `fd` (`rsFileClose`) is leaked if `malloc(buf_len)` fails at line 148. Replacing `fileReadBuf` allocation with a `std::vector<char>` and an `irods::at_scope_exit` guard for `rsFileClose` eliminates both leaks.
  - In `filePathRegRepl` (lines 219–235): move `free_data_obj_info_head` scope guard immediately after `getDataObjInfo` succeeds.

### 1.4 Legacy C Numeric Conversions (`atoi`, `strtol`, `strtoll`) & Structured Logging
- Replace all 22 `atoi`, `strtol`, and `strtoll` calls across `server/core/src/` (`dataObjOpr.cpp`, `objDesc.cpp`, `objMetaOpr.cpp`, `physPath.cpp`, `specColl.cpp`) and `server/api/src/` (`rsBulkDataObjPut.cpp`, `rsBulkDataObjReg.cpp`, `rsDataObjCopy.cpp`, `rsDataObjUnlink.cpp`, `rsFileSyncToArch.cpp`, `rsGetMiscSvrInfo.cpp`, `rsGetRescQuota.cpp`, `rsModDataObjMeta.cpp`, `rsProcStat.cpp`, `rsRmColl.cpp`) with `std::from_chars`.
- Migrate `rodsLog` / `rodsLogError` calls in `rsApiHandler.cpp`, `rsPamAuthRequest.cpp`, and `rsGetMiscSvrInfo.cpp` to `irods::experimental::log::agent`, `log::server`, and `log::api`.

---

## 2. Task Decomposition

1. **Task 1: `rsApiHandler` `setjmp`/`longjmp` Removal, `server/core/src/` Typestate Leak Fixes (`server_utilities.cpp`, `miscServerFunct.cpp`), & `std::from_chars` Migration**
   - Files: `server/core/include/irods/rsApiHandler.hpp`, `server/core/src/rsApiHandler.cpp`, `server/core/src/server_utilities.cpp`, `server/core/src/miscServerFunct.cpp`, `server/core/src/dataObjOpr.cpp`, `server/core/src/objDesc.cpp`, `server/core/src/objMetaOpr.cpp`, `server/core/src/physPath.cpp`, `server/core/src/specColl.cpp`.
2. **Task 2: Server API Collection & Bundle Handler RAII Leak Fixes (`rsRmColl.cpp`, `rsStructFileBundle.cpp`, `rsUnbunAndRegPhyBunfile.cpp`)**
   - Files: `server/api/src/rsRmColl.cpp`, `server/api/src/rsStructFileBundle.cpp`, `server/api/src/rsUnbunAndRegPhyBunfile.cpp`.
3. **Task 3: Server API Process/Auth/Bulk/DataObj Handler Leak Fixes & `std::from_chars` Migration (`rsExecCmd.cpp`, `rsPamAuthRequest.cpp`, `rsBulkDataObjPut.cpp`, `rsBulkDataObjReg.cpp`, `rsDataObjCopy.cpp`, `rsDataObjUnlink.cpp`, `rsFileSyncToArch.cpp`, `rsGetMiscSvrInfo.cpp`, `rsGetRescQuota.cpp`, `rsModDataObjMeta.cpp`, `rsProcStat.cpp`, `register_physical_path.cpp`) & Full Verification Gate**
   - Files: `server/api/src/rsExecCmd.cpp`, `server/api/src/rsPamAuthRequest.cpp`, `server/api/src/rsBulkDataObjPut.cpp`, `server/api/src/rsBulkDataObjReg.cpp`, `server/api/src/rsDataObjCopy.cpp`, `server/api/src/rsDataObjUnlink.cpp`, `server/api/src/rsFileSyncToArch.cpp`, `server/api/src/rsGetMiscSvrInfo.cpp`, `server/api/src/rsGetRescQuota.cpp`, `server/api/src/rsModDataObjMeta.cpp`, `server/api/src/rsProcStat.cpp`, `plugins/api/src/register_physical_path.cpp`.

---

## 3. Verification Gate
- 100% build of `irods_server`, `irods_client`, and all plugins/tests.
- Full verification gate via `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build` confirming `server_utilities.cpp` and `miscServerFunct.cpp` typestate violations are eliminated from the CPG report.
