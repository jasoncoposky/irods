# Stage 16: Resource, Database, Authentication, & Network Plugins (`plugins/`) Modernization — Design Specification

## 1. Executive Summary & Objective
Stage 16 of the iRODS Master Modernization Roadmap modernizes the iRODS Plugin Ecosystem (`plugins/resources/`, `plugins/database/`, `plugins/auth_legacy/`, `plugins/authentication/`, and `plugins/network/`). Through a systematic audit of all plugin directories, we identified:
1. **Resource Leaks & `atoi`/`atoll`/`strtol` Conversions in Resource Plugins (`plugins/resources/`)**:
   - **`plugins/resources/src/univmss.cpp`**:
     - In `univ_mss_file_sync_to_arch` (lines 720–745), `status = _rsExecCmd(&execCmdInp, &execCmdOut);` only calls `freeCmdExecOut(execCmdOut)` inside the `else` (error) branch — leaking `execCmdOut` (and its `stdoutBuf` / `stderrBuf`) on **every successful `syncToArch` invocation**!
     - In `univ_mss_file_stat` (lines 270–322), `execCmdOut` can be guarded with `irods::at_scope_exit{[&execCmdOut] { freeCmdExecOut(execCmdOut); }}` and 15 `atoi`/`atoll` calls (lines 276–294) migrated to `std::from_chars` (`#include <charconv>`).
     - In `univ_mss_file_stat`, `mytm` is uninitialized on the stack before `mktime(&mytm)` (`tm_isdst` is indeterminate!). Value-initializing `struct tm mytm{};` fixes potential non-deterministic DST offsets.
   - **`plugins/resources/src/compound.cpp`**:
     - Lines 888 and 953 use `strtol(id_str, NULL, 10)`. Migrate to `std::from_chars`.
   - **`plugins/resources/unixfilesystem/src/unixfilesystem.cpp`**:
     - In `unix_file_copy` (lines 205–206), `if (inFd > 0) close(inFd);` and `if (outFd > 0) close(outFd);` should check `>= 0`.
2. **Legacy C Numeric Conversions (`strtoll`, `atoll`) & Memory Safety in Database Plugin (`plugins/database/`)**:
   - **`plugins/database/src/catalog_access_control.cpp`**:
     - Lines 516–523, 558, 610, 643–644 use `std::strtoll(..., nullptr, 10)` (and line 610 constructs a temporary `std::string{_data_size}` from `std::string_view` just to call `.c_str()`!). Migrate all 11 occurrences to a `std::from_chars` helper taking `std::string_view`.
   - **`plugins/database/src/db_plugin.cpp`**:
     - Lines 2258, 2271, 2284, 2297, 5584, 5593, 5598, 5617, 5849, 5856, 5857, 6099, 6324, 9639, 9640, 9729, 9730 use `atoll` / `std::atoll`. Migrate all 17 occurrences to `std::from_chars`.
     - In `db_specific_query_op` (lines 11075–11120), if `malloc(totalLen)` fails mid-loop at `j > 0`, earlier `_result->sqlResult[0..j-1].value` buffers are not freed, and `memset(tResult, 0, totalLen)` can be simplified with `std::calloc(1, totalLen)`.
3. **Pipe Descriptor, `authPluginReqOut_t`, `authCheckOut_t`, & OpenSSL Leaks in Auth & Network Plugins (`plugins/auth_legacy/`, `plugins/network/`)**:
   - **`plugins/auth_legacy/src/pam.cpp`**:
     - In `pam_auth_client_request` (lines 213–239), guard `req_out` with `irods::at_scope_exit{[&req_out] { std::free(req_out); }}`.
     - In `run_pam_auth_check` (lines 254–293), `p2cp` has the exact same pipe descriptor leaks we fixed in `rsPamAuthRequest.cpp`: on `fork() == -1`, `p2cp[0]` and `p2cp[1]` are leaked; in the parent branch (`if (pid)`), `p2cp[0]` is never closed; in the child branch, `p2cp[0]` is not closed after `dup2(p2cp[0], STDIN_FILENO)`.
     - In `pam_auth_agent_request` (lines 342–372), `authPluginReqOut_t* req_out = nullptr;` is **never freed** after `rcAuthPluginRequest` succeeds! Guard `req_out` with `irods::at_scope_exit{[&req_out] { std::free(req_out); }}`.
   - **`plugins/auth_legacy/src/native.cpp`**:
     - In `native_auth_client_request` (lines 218–240), guard `auth_request` (`std::free(auth_request->challenge); std::free(auth_request);`) with `irods::at_scope_exit`.
     - In `native_auth_agent_response` (lines 347–560), guard `authCheckOut` with `irods::at_scope_exit`, and fix line 407 where `rodsLog(LOG_NOTICE, "rsAuthResponse: Warning, cannot authenticate the remote server, no RemoteZoneSID defined in server_config.json", status);` passes an extra unused `status` argument.
   - **`plugins/network/src/ssl.cpp`**:
     - In `ssl_load_dh_params` (lines 79–99), if `SSL_CTX_set0_tmp_dh_pkey(ctx, pkey) <= 0` (note OpenSSL `SSL_CTX_set0_tmp_dh_pkey` returns `1` on success and `0` or negative on error!), `pkey` is not transferred to `ctx` and is leaked (`EVP_PKEY_free(pkey)`).
     - In `ssl_agent_start` (lines 690–760):
       - If `SSL_accept(ssl) < 1` at line 698, **neither `SSL_free(ssl)` nor `SSL_CTX_free(ctx)` is called**, leaking both the OpenSSL `SSL` session and `SSL_CTX`!
       - At line 734, `bytesBuf_t msg_buf{};` populated by `readMsgBody` is **never freed** (`clearBBuf(&msg_buf)` is missing), leaking the shared-secret message buffer on every agent TLS startup!
     - In `ssl_client_stop` (lines 513–531) and `ssl_agent_stop` (lines 778–796), if `SSL_shutdown` returns `!= 1`, the function returns early **without calling `SSL_free(ssl)` or `SSL_CTX_free(ctx)`**, permanently leaking the `SSL` and `SSL_CTX` objects! Using `irods::at_scope_exit` ensures `SSL_free(ssl)`, `SSL_CTX_free(ctx)`, `ssl_obj->ssl(nullptr)`, and `ssl_obj->ssl_ctx(nullptr)` always run even when `SSL_shutdown` reports an error.

---

## 2. Verification Criteria
1. Zero occurrences of `setjmp`, `longjmp`, `atoi(`, `atof(`, `atoll(`, `strtol`, `strtoll`, `bzero(` across all `plugins/` subdirectories.
2. 100% build of all resource, database, authentication, and network plugin targets.
3. Full pass of `scripts/verify_refactoring.sh` (Build + Catch2 + Project Insight CPG ingestion + concurrency + typestate verification).
