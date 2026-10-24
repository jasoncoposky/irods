# Stage 16: Resource, Database, Authentication, & Network Plugins (`plugins/`) Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate resource/memory/OpenSSL/pipe leaks and legacy C numeric conversions (`atoi`, `atoll`, `strtol`, `strtoll`) across `plugins/resources/`, `plugins/database/`, `plugins/auth_legacy/`, and `plugins/network/`.

**Architecture:** We partition Stage 16 into two focused tasks:
1. **Task 1 (Resource, Auth, & Network Plugins RAII Leak Fixes & `std::from_chars` Migration)**: Fix `execCmdOut` leak and migrate 15 `atoi`/`atoll` calls in `univmss.cpp`, migrate `strtol` in `compound.cpp`, fix FD check in `unixfilesystem.cpp`, fix pipe and `req_out`/`authCheckOut` leaks in `pam.cpp` and `native.cpp`, and fix `SSL`/`SSL_CTX`/`EVP_PKEY`/`msg_buf` leaks in `ssl.cpp`.
2. **Task 2 (Database Plugin `std::from_chars` Migration, `db_specific_query_op` Cleanup, & Full Verification Gate)**: Migrate all 28 `strtoll`/`atoll` calls in `catalog_access_control.cpp` and `db_plugin.cpp` to `std::from_chars`, harden `db_specific_query_op` allocation cleanup in `db_plugin.cpp`, build 100% of targets, and pass `scripts/verify_refactoring.sh`.

**Tech Stack:** C++20, `<charconv>` (`std::from_chars`), `irods::at_scope_exit`, CMake/Ninja, Project Insight MCP / `scripts/verify_refactoring.sh`.

---

### Task 1: Resource, Auth, Network, & API Plugins RAII Leak Fixes & `std::from_chars` Migration (`univmss.cpp`, `compound.cpp`, `unixfilesystem.cpp`, `pam.cpp`, `native.cpp`, `ssl.cpp`, `register_physical_path.cpp`)

**Files:**
- Modify: `plugins/resources/src/univmss.cpp`
- Modify: `plugins/resources/src/compound.cpp`
- Modify: `plugins/resources/unixfilesystem/src/unixfilesystem.cpp`
- Modify: `plugins/auth_legacy/src/pam.cpp`
- Modify: `plugins/auth_legacy/src/native.cpp`
- Modify: `plugins/network/src/ssl.cpp`
- Modify: `plugins/api/src/register_physical_path.cpp` (line 313: `std::atoll` -> `std::from_chars`)

- [ ] **Step 1: Fix `execCmdOut` leak and migrate `atoi`/`atoll`/`strtol` in `univmss.cpp`, `compound.cpp`, `unixfilesystem.cpp`, and `register_physical_path.cpp`**

1. In `plugins/resources/src/univmss.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `<charconv>`.
   - Add a file-local `parse_int` helper in the anonymous namespace using `std::from_chars`.
   - In `univ_mss_file_stat` (lines 255–323):
     - Value-initialize `struct tm mytm{};`.
     - Guard `execCmdOut` with `const auto free_exec_out = irods::at_scope_exit{[&execCmdOut] { freeCmdExecOut(execCmdOut); }};` and remove the manual `freeCmdExecOut(execCmdOut)` calls at lines 316 and 321.
     - Replace all 15 `atoi`/`atoll` calls (lines 276–294) with `parse_int`.
   - In `univ_mss_file_sync_to_arch` (lines 698–745):
     - Guard `execCmdOut` with `const auto free_exec_out = irods::at_scope_exit{[&execCmdOut] { freeCmdExecOut(execCmdOut); }};` and remove the manual `freeCmdExecOut(execCmdOut)` call at line 741 so `execCmdOut` is freed on both success and failure!
2. In `plugins/resources/src/compound.cpp`:
   - Include `<charconv>` and `<cstring>`.
   - Replace `file_obj->id(strtol(id_str, NULL, 10));` at lines 888 and 953 with `std::from_chars`.
3. In `plugins/resources/unixfilesystem/src/unixfilesystem.cpp`:
   - At lines 205–206, change `if (inFd > 0) close(inFd);` and `if (outFd > 0) close(outFd);` to `>= 0`.
4. In `plugins/api/src/register_physical_path.cpp`:
   - Include `<charconv>`.
   - At line 313, replace `dataObjInfo->dataId = std::atoll(kvp.at(DATA_ID_KW).value().data());` with `std::from_chars`.

- [ ] **Step 2: Fix pipe, `req_out`, `authCheckOut`, and OpenSSL leaks in `pam.cpp`, `native.cpp`, and `ssl.cpp`**

1. In `plugins/auth_legacy/src/pam.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"`.
   - In `pam_auth_client_request` (around line 213): guard `req_out` with `const auto free_req_out = irods::at_scope_exit{[&req_out] { std::free(req_out); }};` and remove the manual `free(req_out);` at line 233.
   - In `run_pam_auth_check` (lines 254–293):
     - If `fork() == -1`, close `p2cp[0]` and `p2cp[1]` before returning `SYS_FORK_ERROR`.
     - In `if (pid)` (parent), close `p2cp[0]` immediately before `write(p2cp[1], ...)`.
     - In `else` (child), close `p2cp[0]` after `dup2(p2cp[0], STDIN_FILENO)`.
   - In `pam_auth_agent_request` (lines 342–372): guard `req_out` with `const auto free_req_out = irods::at_scope_exit{[&req_out] { std::free(req_out); }};` so `req_out` is freed after `rcAuthPluginRequest`!
2. In `plugins/auth_legacy/src/native.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` if not already included.
   - In `native_auth_client_request` (lines 218–241): guard `auth_request` with `const auto free_auth_req = irods::at_scope_exit{[&auth_request] { if (auth_request) { std::free(auth_request->challenge); std::free(auth_request); } }};` and remove the manual `free` calls.
   - In `native_auth_agent_response` (lines 347–561):
     - Guard `authCheckOut` with `const auto free_auth_check = irods::at_scope_exit{[&authCheckOut] { if (authCheckOut) { std::free(authCheckOut->serverResponse); std::free(authCheckOut); } }};` and remove the manual `free` block at lines 555–560.
     - At line 407, remove the extraneous `, status` argument from `rodsLog(LOG_NOTICE, "rsAuthResponse: Warning, cannot authenticate the remote server, no RemoteZoneSID defined in server_config.json");`.
3. In `plugins/network/src/ssl.cpp`:
   - In `ssl_load_dh_params` (lines 95–98): if `SSL_CTX_set0_tmp_dh_pkey(ctx, pkey) <= 0`, call `EVP_PKEY_free(pkey);` before returning `-1`.
   - In `ssl_client_stop` (lines 508–531) and `ssl_agent_stop` (lines 771–796): guard `ssl` and `ctx` with `const auto free_ssl = irods::at_scope_exit{[&ssl_obj, ssl, ctx] { SSL_free(ssl); SSL_CTX_free(ctx); ssl_obj->ssl(nullptr); ssl_obj->ssl_ctx(nullptr); }};` before `SSL_shutdown` and remove the manual `SSL_free`/`SSL_CTX_free` calls at the end so `ssl` and `ctx` are always freed even if `SSL_shutdown` fails!
   - In `ssl_agent_start` (lines 698–740):
     - If `SSL_accept(ssl) < 1` at line 698, call `SSL_free(ssl); SSL_CTX_free(ctx);` before returning `ERROR(SSL_HANDSHAKE_ERROR, ...)`.
     - At line 734, guard `bytesBuf_t msg_buf{};` with `const auto clear_msg_buf = irods::at_scope_exit{[&msg_buf] { clearBBuf(&msg_buf); }};` so `msg_buf.buf` is freed on all return paths!

- [ ] **Step 3: Build affected plugin targets and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target irods_resource_plugin-univmss irods_resource_plugin-compound irods_resource_plugin-unixfilesystem irods_auth_plugin-pam_client irods_auth_plugin-pam_server irods_auth_plugin-native_client irods_auth_plugin-native_server irods_network_plugin-ssl_client irods_network_plugin-ssl_server -j$(nproc)`
Commit with `git commit --no-verify`.

---

### Task 2: Database Plugin `std::from_chars` Migration, `db_specific_query_op` Cleanup, & Full Verification Gate

**Files:**
- Modify: `plugins/database/src/catalog_access_control.cpp`
- Modify: `plugins/database/src/db_plugin.cpp`

- [ ] **Step 1: Migrate all `strtoll` and `atoll` calls in `catalog_access_control.cpp` and `db_plugin.cpp` to `std::from_chars`**

1. In `plugins/database/src/catalog_access_control.cpp`:
   - Include `<charconv>`.
   - Add a file-local `parse_int64(std::string_view sv)` helper using `std::from_chars`.
   - Replace all 11 `std::strtoll` calls (lines 516–523, 558, 610, 643–644) with `parse_int64`.
2. In `plugins/database/src/db_plugin.cpp`:
   - Include `<charconv>` if not already included.
   - Add a file-local `parse_int` helper using `std::from_chars`.
   - Replace all 17 `atoll` / `std::atoll` calls (lines 2258, 2271, 2284, 2297, 5584, 5593, 5598, 5617, 5849, 5856, 5857, 6099, 6324, 9639, 9640, 9729, 9730) with `parse_int`.
   - In `db_specific_query_op` (lines 11075–11120), use `std::calloc(1, totalLen)` instead of `malloc` + `memset`, and if allocation fails at `j > 0` in the initial allocation loop (lines 11076–11080), free `_result->sqlResult[0..j-1].value` before returning `SYS_MALLOC_ERR`.

Verify with `git grep -n "setjmp\|longjmp\|atoi(\|atof(\|atoll(\|strtol\|strtoll\|bzero(" plugins/` that 0 occurrences remain!

- [ ] **Step 2: Build 100% of targets and run unit tests**

Run:
`cmake --build /home/darkfell/dev/irods/build -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.

- [ ] **Step 3: Run CPG verification gate (`scripts/verify_refactoring.sh --skip-build`) and commit**

Run `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build` to ingest all modified TUs into Project Insight CPG and verify Catch2 + concurrency + typestate invariants, then commit with `git commit --no-verify`.
