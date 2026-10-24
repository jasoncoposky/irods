# Stage 17: Core Networking, Packing, `msParam`, `rcMisc`, & `lib/filesystem` Endgame Refactoring — Design Specification

## 1. Executive Summary & Objective
Stage 17 is the final stage of the iRODS Master Modernization Roadmap (`lib/core/` and `lib/filesystem/`). After Stages 12–16, the only remaining legacy C numeric conversions (`atoi`, `atoll`, `strtoll`), dead `setjmp`/`longjmp` jump buffers (`jmp_buf Jcenv`, `jmp_buf *label`), and unguarded low-level resources in the entire codebase reside in `lib/core/` and `lib/filesystem/`:

1. **Dead `setjmp`/`longjmp` Elimination in `lib/core/src/sockComm.cpp`, `lib/core/include/irods/region.h`, and `lib/core/src/region.cpp`**:
   - **`lib/core/src/sockComm.cpp`**:
     - Lines 11–12 declare `#include <csetjmp>` and `jmp_buf Jcenv;`, which are 100% dead and unreferenced anywhere in the repository! Remove both.
   - **`lib/core/include/irods/region.h` & `lib/core/src/region.cpp`**:
     - Every caller of `make_region(0, NULL)` in the entire repository passes `NULL` for `jmp_buf *label`. However, to preserve the external C header signature `Region *make_region(size_t is, jmp_buf *label)` if needed or clean up `longjmp`, we can check if `region.h` is part of the public header surface. Even better: in `region.cpp`, `make_region_node` (lines 72–77) calls `memset(node->block, 0, is);` **before** checking `if (node->block == NULL)` — a classic null-pointer dereference if `malloc(is)` returns `NULL`! Fix `make_region_node` to check `if (node->block == NULL)` before `memset` (or use `calloc(1, is)`).

2. **Complete `std::from_chars` Migration Across `lib/core/src/` & `lib/filesystem/src/`**:
   - **`lib/core/src/getRodsEnv.cpp`** (lines 179, 490): `atoi` -> `std::from_chars`.
   - **`lib/core/src/irods_socket_information.cpp`** (line 46): `atoi` -> `std::from_chars`.
   - **`lib/core/src/msParam.cpp`** (lines 1084, 1465, 1487, 1491, 1495, 1599, 1603, 1762, 1766): `atoi` and `strtoll` -> `std::from_chars` (`parse_int`).
   - **`lib/core/src/packStruct.cpp`** (lines 638, 738, 2368, 2508, 2642): `atoi` and `strtoll` -> `std::from_chars`.
   - **`lib/core/src/parseCommandLine.cpp`** (lines 98, 119, 173, 391, 421, 480): `atoi` and `strtoll` -> `std::from_chars`.
   - **`lib/core/src/rcConnect.cpp`** (line 74): `atoi` -> `std::from_chars`.
   - **`lib/core/src/rcMisc.cpp`** (lines 1931, 2028–2038, 2203, 2231, 2324, 2353, 2387–2397, 2486–2514, 2935, 3904, 3926): `atoi` and `atoll` -> `std::from_chars`.
   - **`lib/filesystem/src/collection_iterator.cpp`** (lines 372, 375) & **`lib/filesystem/src/filesystem.cpp`** (line 386): `std::atoi` and `std::strtoll` -> `std::from_chars`.

3. **Resource & Memory Leak Fixes in `lib/core/src/`**:
   - Audit `sockComm.cpp`, `packStruct.cpp`, `msParam.cpp`, `rcMisc.cpp`, `rcConnect.cpp`, and `clientLogin.cpp` for unguarded file descriptors, sockets, `FILE*` streams, and heap buffers, wrapping them with `irods::at_scope_exit`.

---

## 2. Verification Criteria
1. Zero occurrences of `atoi(`, `atof(`, `atoll(`, `strtol(`, `strtoll(`, `bzero(`, `Jcenv` across `lib/`, `server/`, `clients/`, and `plugins/`.
2. 100% build of all targets in `/home/darkfell/dev/irods/build`.
3. Full pass of `scripts/verify_refactoring.sh` (Build + Catch2 unit tests + Project Insight CPG ingestion + concurrency + typestate verification).
