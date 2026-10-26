# Stage 19: Value-Semantic Connection Handles (`rcComm_t` / `rsComm_t`) & `std::pmr` Rule Engine Arena Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate `malloc` / `memset` / `free` and `setjmp.h` lifecycle hazards across `rcComm_t` (`RcComm`), `rsComm_t` (`RsComm`), `thread_context`, and the legacy rule engine `Region` allocator (`region.h` / `region.cpp`), introducing RAII value semantics for `thread_context` and `RcComm`, a reusable session reset API (`reset_rs_comm_session` / `cleanup_rs_comm_members`) for `RsComm` pooling in Stage 22, and a `std::pmr::monotonic_buffer_resource`-backed `Region` arena with 16-byte alignment.

**Architecture:**
1. **RAII `thread_context` (`lib/core/include/irods/irods_threads.hpp`)**:
   - Equip `struct thread_context` with default member initializers, a default constructor that allocates `lock` (`new std::mutex{}`) and `cond` (`new std::condition_variable{}`), and a `noexcept` destructor that safely joins/detaches `reconnThr` (if non-null and `joinable()`) and deletes `reconnThr`, `lock`, and `cond`.
   - Update all allocation and teardown sites (`lib/core/src/rcConnect.cpp`, `server/core/src/initServer.cpp`, `server/core/src/miscServerFunct.cpp`, `server/main_server/src/agent_main.cpp`) to use `new thread_context{}` and `delete thread_ctx` — fixing both the uninitialized-pointer hazard in `agent_main.cpp` and the `thread_context` member leaks in `cleanup_and_free_rsComm_members`.
2. **Value-Semantic `RcComm` (`rcComm_t`) & Reusable `RsComm` (`rsComm_t`) Session Reset (`lib/core/include/irods/rcConnect.h`, `lib/core/src/rcConnect.cpp`, `server/core/include/irods/initServer.hpp`, `server/core/src/initServer.cpp`, `server/main_server/src/agent_main.cpp`)**:
   - Add default member initializers (`{}`) to `RcComm` and `RsComm` under `#ifdef __cplusplus`, and add `~RcComm() noexcept { cleanRcComm(this); }` so both stack-allocated `RcComm` objects and heap-allocated `rcComm_t*` handles (`new rcComm_t{}` / `delete conn`) automatically clean up owned resources (`rError`, `svrVersion`, `thread_ctx`).
   - Keep `RsComm` copyable for temporary shallow stack overrides (`rsComm_t myComm = *rsComm;` in `initServer.cpp`, `rsGenQuery.cpp`, etc.), replace `memset(rsComm, 0, sizeof(rsComm_t))` in `initRsComm` with `*rsComm = rsComm_t{}`, and provide `cleanup_rs_comm_members(rsComm_t& _comm) noexcept` and `reset_rs_comm_session(rsComm_t& _comm) noexcept` for clean per-session teardown and Stage 22 worker pooling.
3. **`std::pmr::monotonic_buffer_resource` Rule Engine `Region` (`lib/core/include/irods/region.h`, `lib/core/src/region.cpp`)**:
   - Remove `#include <setjmp.h>` from `lib/core/include/irods/region.h` (changing the unused `jmp_buf* label` parameter of `make_region` to `void* label`), upgrade `REGION_ALIGNMENT` from `8` to `16` (`alignof(std::max_align_t)`), and replace the manual `malloc`/`free` linked list in `lib/core/src/region.cpp` with `std::pmr::monotonic_buffer_resource`.

**Tech Stack:** C++20 (`<memory_resource>`, `<mutex>`, `<condition_variable>`, `<thread>`), Catch2.

---

### Task 1: Implement RAII `thread_context`, Value-Semantic `RcComm` / `RsComm` Session Reset, & `std::pmr::monotonic_buffer_resource` `Region` Arena

**Files:**
- Modify: `lib/core/include/irods/irods_threads.hpp`
- Modify: `lib/core/include/irods/rcConnect.h`
- Modify: `lib/core/src/rcConnect.cpp`
- Modify: `server/core/include/irods/initServer.hpp`
- Modify: `server/core/src/initServer.cpp`
- Modify: `server/core/src/miscServerFunct.cpp`
- Modify: `server/main_server/src/agent_main.cpp`
- Modify: `lib/core/include/irods/region.h`
- Modify: `lib/core/src/region.cpp`
- Modify: `unit_tests/src/test_rcConnect.cpp`

- [ ] **Step 1: Upgrade `struct thread_context` in `lib/core/include/irods/irods_threads.hpp` to RAII**

1. In `lib/core/include/irods/irods_threads.hpp`:
   - Give `struct thread_context` default member initializers, a default constructor that allocates `lock` (`new std::mutex{}`) and `cond` (`new std::condition_variable{}`), and a destructor `~thread_context() noexcept` that joins (or detaches if joining throws) and deletes `reconnThr`, `lock`, and `cond`:
     ```cpp
     struct thread_context {
         std::thread*             reconnThr{};
         std::mutex*              lock{};
         std::condition_variable* cond{};

         thread_context()
             : reconnThr{nullptr}
             , lock{new std::mutex{}}
             , cond{new std::condition_variable{}}
         {
         }

         ~thread_context() noexcept
         {
             if (reconnThr) {
                 if (reconnThr->joinable()) {
                     try {
                         reconnThr->join();
                     }
                     catch (...) {
                         reconnThr->detach();
                     }
                 }
                 delete reconnThr;
                 reconnThr = nullptr;
             }
             delete lock;
             lock = nullptr;
             delete cond;
             cond = nullptr;
         }

         thread_context(const thread_context&) = delete;
         auto operator=(const thread_context&) -> thread_context& = delete;
     };
     ```

- [ ] **Step 2: Upgrade `RcComm` (`rcComm_t`) and `RsComm` (`rsComm_t`) in `rcConnect.h`, `rcConnect.cpp`, `initServer.hpp`, `initServer.cpp`, `miscServerFunct.cpp`, and `agent_main.cpp`**

1. In `lib/core/include/irods/rcConnect.h`:
   - Add default member initializers (`{}`) to `RcComm` and `RsComm` fields when compiled as C++ (`#ifdef __cplusplus`), and add `~RcComm() noexcept { cleanRcComm(this); }` inside `struct RcComm` (note: move `int cleanRcComm(rcComm_t* conn);` declaration above `struct RcComm` or forward-declare `struct RcComm;` and `int cleanRcComm(struct RcComm* conn);` before `struct RcComm` so `~RcComm()` can call `cleanRcComm(this)` inline!).
2. In `lib/core/src/rcConnect.cpp`:
   - In `_rcConnect` (lines 58–66):
     - Replace `rcComm_t *conn = (rcComm_t*)malloc(sizeof(rcComm_t)); memset(conn, 0, sizeof(rcComm_t));` with `auto* conn = new (std::nothrow) rcComm_t{}; if (!conn) { return nullptr; }`.
     - Replace `conn->thread_ctx = (thread_context*)malloc(sizeof(thread_context)); conn->thread_ctx->reconnThr = nullptr; conn->thread_ctx->lock = new std::mutex; conn->thread_ctx->cond = new std::condition_variable;` with `conn->thread_ctx = new thread_context{};`.
   - In `_rcConnect` error paths (lines 97–105, 121–129, 154–162, 180–183):
     - Replace manual `delete conn->thread_ctx->reconnThr; delete conn->thread_ctx->lock; delete conn->thread_ctx->cond; free(conn->thread_ctx); free(conn);` with `delete conn;`.
   - In `freeRcComm(rcComm_t* conn)` (lines 333–346):
     - Replace `if (status != 0) { free(conn); }` with `if (status != 0) { delete conn; }`.
   - In `cleanRcComm(rcComm_t* conn)` (lines 367–377):
     - Replace the manual `reconnThr`/`lock`/`cond` deletion + `free(conn->thread_ctx)` with `delete conn->thread_ctx; conn->thread_ctx = nullptr; ret = 1;`.
3. In `server/core/include/irods/initServer.hpp` and `server/core/src/initServer.cpp`:
   - Declare and implement `void cleanup_rs_comm_members(rsComm_t& _comm) noexcept;` and `void reset_rs_comm_session(rsComm_t& _comm) noexcept;`:
     - `cleanup_rs_comm_members` frees `rError` (`freeRErrorContent(&_comm.rError)`), clears `session_props` (`clearKeyVal(&_comm.session_props)`), frees `auth_scheme` (`std::free(_comm.auth_scheme); _comm.auth_scheme = nullptr;`), frees `reconnAddr` (`std::free(_comm.reconnAddr); _comm.reconnAddr = nullptr;`), frees `portalOpr` (`std::free(_comm.portalOpr); _comm.portalOpr = nullptr;`), and deletes `thread_ctx` (`delete _comm.thread_ctx; _comm.thread_ctx = nullptr;`).
     - `reset_rs_comm_session` saves `const rodsEnv saved_env = _comm.myEnv;`, calls `cleanup_rs_comm_members(_comm);`, resets `_comm = rsComm_t{};`, restores `_comm.myEnv = saved_env;`, and allocates a fresh `_comm.thread_ctx = new thread_context{};`.
   - In `initRsComm(rsComm_t* rsComm)` (`server/core/src/initServer.cpp` line 604):
     - Replace `memset(rsComm, 0, sizeof(rsComm_t));` with `*rsComm = rsComm_t{};`.
   - In `initRsCommWithStartupPack` (`server/core/src/initServer.cpp` lines 506–509):
     - Ensure `if (!rsComm->thread_ctx) { rsComm->thread_ctx = new thread_context{}; }` before creating `rsComm->thread_ctx->reconnThr = new std::thread(svrReconnManager, rsComm);` (without overwriting `rsComm->thread_ctx->lock` and `rsComm->thread_ctx->cond` if already allocated!).
4. In `server/core/src/miscServerFunct.cpp` (line 2034):
   - Guard `if (!rsComm->thread_ctx) { return; }` at the top of `svrReconnManager`.
5. In `server/main_server/src/agent_main.cpp`:
   - At line 860, replace `rsComm.thread_ctx = static_cast<thread_context*>(std::malloc(sizeof(thread_context)));` with `rsComm.thread_ctx = new thread_context{};`.
   - In `cleanup_and_free_rsComm_members` (lines 936–959), delegate to `cleanup_rs_comm_members(_comm);`.

- [ ] **Step 3: Replace `lib/core/include/irods/region.h` and `lib/core/src/region.cpp` with `std::pmr::monotonic_buffer_resource`**

1. In `lib/core/include/irods/region.h`:
   - Remove `#include <setjmp.h>`.
   - Change `#define REGION_ALIGNMENT 8` to `#define REGION_ALIGNMENT 16`.
   - Change `Region *make_region( size_t is, jmp_buf *label );` to `Region *make_region( size_t is, void *label );` so `#include <setjmp.h>` is completely removed while all `make_region(0, nullptr)` callers remain 100% source-compatible.
2. In `lib/core/src/region.cpp`:
   - Implement `struct region` backed by `std::pmr::monotonic_buffer_resource`:
     ```cpp
     #include "irods/region.h"
     #include "irods/rodsErrorTable.h"

     #include <cstring>
     #include <memory_resource>
     #include <new>

     struct region {
         std::pmr::monotonic_buffer_resource arena;
         std::size_t total_allocated{0};
         struct region_error error{};

         explicit region(std::size_t initial_size)
             : arena(initial_size > 0 ? initial_size : DEFAULT_BLOCK_SIZE,
                     std::pmr::new_delete_resource())
         {
         }
     };
     ```
   - Implement `make_region(size_t is, void* label)` using `new (std::nothrow) Region(is == 0 ? DEFAULT_BLOCK_SIZE : roundToAlignment(is))`.
   - Implement `region_alloc(Region* r, size_t size)` by allocating `alloc_size` bytes from `r->arena.allocate(alloc_size, REGION_ALIGNMENT)`, zero-initializing with `std::memset(raw, 0, alloc_size)`, populating the leading `RegionDesc`, incrementing `r->total_allocated += alloc_size`, and returning `static_cast<unsigned char*>(raw) + CACHE_SIZE(RegionDesc, 1)`.
   - Implement `region_free(Region* r)` as `delete r;` and `region_size(Region* r)` as `return r ? r->total_allocated : 0;`.

- [ ] **Step 4: Add unit tests in `unit_tests/src/test_rcConnect.cpp`, build, verify, and commit**

1. In `unit_tests/src/test_rcConnect.cpp`:
   - Include `"irods/region.h"`.
   - Add `TEST_CASE("RAII RcComm and thread_context lifecycle")` verifying:
     - Stack-allocated `RcComm` with `thread_ctx = new thread_context{}` and `svrVersion` cleans up automatically at scope exit without leaking.
     - Heap-allocated `rcComm_t*` via `new rcComm_t{}` and `freeRcComm` cleans up cleanly.
   - Add `TEST_CASE("std::pmr Region arena allocator")` verifying:
     - `make_region(0, nullptr)` allocates 16-byte aligned blocks (`reinterpret_cast<std::uintptr_t>(p) % REGION_ALIGNMENT == 0`).
     - Allocated memory is zero-initialized and `REGION_DESC(p)->region == r` and `REGION_DESC(p)->size` is populated.
     - Large allocations (`> DEFAULT_BLOCK_SIZE`) and multi-chunk growth track `region_size(r)` accurately and release cleanly on `region_free(r)`.
2. Build 100% of targets, run `irods_rcConnect`, run `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build`, and commit with `git commit --no-verify`.
