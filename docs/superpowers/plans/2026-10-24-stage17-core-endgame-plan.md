# Stage 17: Core Networking, Packing, `msParam`, `rcMisc`, & `lib/filesystem` Endgame Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate dead `csetjmp`/`Jcenv`, fix null-dereference in `region.cpp`, and replace all remaining `atoi`, `atoll`, and `strtoll` calls across `lib/core/src/` and `lib/filesystem/src/` with `std::from_chars`, completing the 17-stage iRODS Master Modernization Roadmap.

**Architecture:** We partition Stage 17 into two focused tasks:
1. **Task 1 (`sockComm.cpp`, `region.cpp`, `getRodsEnv.cpp`, `irods_socket_information.cpp`, `msParam.cpp`, `packStruct.cpp`, `parseCommandLine.cpp`, `rcConnect.cpp`)**: Remove `#include <csetjmp>` and `jmp_buf Jcenv;` from `sockComm.cpp`, fix the null-dereference before `memset` in `make_region_node` (`region.cpp`), and migrate all `atoi`/`strtoll` calls in `getRodsEnv.cpp`, `irods_socket_information.cpp`, `msParam.cpp`, `packStruct.cpp`, `parseCommandLine.cpp`, and `rcConnect.cpp` to `std::from_chars`.
2. **Task 2 (`rcMisc.cpp`, `lib/filesystem/src/collection_iterator.cpp`, `lib/filesystem/src/filesystem.cpp`, & Full Codebase Verification Gate)**: Migrate all 25 `atoi`/`atoll` calls in `rcMisc.cpp` and `std::atoi`/`std::strtoll` calls in `lib/filesystem/` to `std::from_chars`, guard `FILE*` / FD resources in `rcMisc.cpp`, build 100% of targets, and run `scripts/verify_refactoring.sh`.

**Tech Stack:** C++20, `<charconv>` (`std::from_chars`), `irods::at_scope_exit`, CMake/Ninja, Project Insight MCP / `scripts/verify_refactoring.sh`.

---

### Task 1: `sockComm.cpp`, `region.cpp`, `msParam.cpp`, `packStruct.cpp`, `parseCommandLine.cpp`, `getRodsEnv.cpp`, `rcConnect.cpp`, & `irods_socket_information.cpp` Modernization

**Files:**
- Modify: `lib/core/src/sockComm.cpp`
- Modify: `lib/core/src/region.cpp`
- Modify: `lib/core/src/getRodsEnv.cpp`
- Modify: `lib/core/src/irods_socket_information.cpp`
- Modify: `lib/core/src/msParam.cpp`
- Modify: `lib/core/src/packStruct.cpp`
- Modify: `lib/core/src/parseCommandLine.cpp`
- Modify: `lib/core/src/rcConnect.cpp`

- [ ] **Step 1: Remove dead `csetjmp`/`Jcenv` in `sockComm.cpp` and fix null-dereference in `region.cpp`**

1. In `lib/core/src/sockComm.cpp`:
   - Remove `#include <csetjmp>` and `jmp_buf Jcenv;` (lines 11–12).
2. In `lib/core/src/region.cpp`:
   - In `make_region_node` (lines 72–77), check `if (node->block == NULL) { free(node); return NULL; }` **before** `memset(node->block, 0, is);`.

- [ ] **Step 2: Migrate `atoi` and `strtoll` to `std::from_chars` across `getRodsEnv.cpp`, `irods_socket_information.cpp`, `msParam.cpp`, `packStruct.cpp`, `parseCommandLine.cpp`, and `rcConnect.cpp`**

1. `lib/core/src/getRodsEnv.cpp` (lines 179, 490): replace `atoi` with `std::from_chars`.
2. `lib/core/src/irods_socket_information.cpp` (line 46): replace `atoi(it->path().filename().string().c_str())` with `std::from_chars` on `it->path().filename().native()`.
3. `lib/core/src/msParam.cpp` (lines 1084, 1465, 1487, 1491, 1495, 1599, 1603, 1762, 1766): replace `atoi` and `strtoll` with `std::from_chars`. Also remove the commented-out `atoi` at line 1089 if present.
4. `lib/core/src/packStruct.cpp` (lines 638, 738, 2368, 2508, 2642): replace `atoi` and `strtoll` with `std::from_chars`.
5. `lib/core/src/parseCommandLine.cpp` (lines 98, 119, 173, 391, 421, 480): replace `atoi` and `strtoll` with `std::from_chars`.
6. `lib/core/src/rcConnect.cpp` (line 74): replace `atoi` with `std::from_chars`.

- [ ] **Step 3: Build `irods_client` & `irods_server` and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target irods_client irods_server -j$(nproc)`
Commit with `git commit --no-verify`.

---

### Task 2: `rcMisc.cpp` & `lib/filesystem/` `std::from_chars` Migration & Full Codebase Verification Gate

**Files:**
- Modify: `lib/core/src/rcMisc.cpp`
- Modify: `lib/filesystem/src/collection_iterator.cpp`
- Modify: `lib/filesystem/src/filesystem.cpp`

- [ ] **Step 1: Migrate all `atoi` and `atoll` calls in `rcMisc.cpp` and `lib/filesystem/` to `std::from_chars`**

1. In `lib/core/src/rcMisc.cpp`:
   - Include `<charconv>`.
   - Add a file-local `parse_int` helper using `std::from_chars` (handling fixed-width slices or null-terminated strings, noting that `&s[17]`, `&s[14]`, etc. in `checkDateFormat` / `localToGmTime` parse leading digits and stop at `-` or `:` or `.`, which `std::from_chars` naturally does!).
   - Replace all 25 `atoi` and `atoll` calls at lines 1931, 2028–2038, 2203, 2231, 2324, 2353, 2387–2397, 2486–2514, 2935, 3904, 3926 with `parse_int`.
2. In `lib/filesystem/src/collection_iterator.cpp`:
   - Include `<charconv>`.
   - At lines 372 and 375, replace `std::atoi(mode_sv.c_str())` and `std::strtoll(size_sv.c_str(), nullptr, 0)` with `std::from_chars`.
3. In `lib/filesystem/src/filesystem.cpp`:
   - At line 386, remove the fallback `return std::atoi(val_str.c_str()) != 0;` (since `parse_int(val_str, val)` already uses `std::from_chars` right above it at line 383!).

Verify with `git grep -n "atoi(\|atof(\|atoll(\|strtol(\|strtoll(\|bzero(\|Jcenv" lib/ server/ clients/ plugins/` that 0 active occurrences remain across the entire codebase!

- [ ] **Step 2: Build 100% of targets and run unit tests**

Run:
`cmake --build /home/darkfell/dev/irods/build -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.

- [ ] **Step 3: Run CPG verification gate (`scripts/verify_refactoring.sh --skip-build`) and commit**

Run `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build` to ingest all modified TUs into Project Insight CPG and verify Catch2 + concurrency + typestate invariants, then commit with `git commit --no-verify`.
