# iRODS 5.1 Transport & Connection Layer Modernization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the iRODS transport, streaming, parallel transfer, and connection pooling layer (`transport.hpp`, `default_transport.hpp`, `dstream.hpp`, `stream_factory_utility.hpp`, `parallel_transfer_engine.hpp`, `connection_pool.cpp`, `client_connection.cpp`) into a zero-copy, leak-free, deadlock-free, GenQuery2-powered C++20 substrate with 100% source and ABI compatibility.

**Architecture:**
1. Harden `basic_transport` (`default_transport.hpp`) with move-only descriptor ownership, zero-allocation stack `condInput` and `close()` JSON formatting, non-throwing JSON response parsing, and zero-copy `std::string_view` hierarchy extraction.
2. Fix `basic_data_object_buf` and `basic_dstream` (`dstream.hpp`) so move/swap is $O(1)$ zero-copy without dangling `streambuf` pointers, `xsgetn` never overflows caller buffers or drops buffered byte counts, `seekoff`/`seekpos` accounts for unconsumed Get-area bytes and invalidates stale read buffers, and accessors are null-safe.
3. Harden `managed_dstream`, `make_dstream_factory`, and `parallel_transfer_engine` (`stream_factory_utility.hpp`, `parallel_transfer_engine.hpp`) to propagate stream error states, eliminate per-channel `filesystem::path` re-allocations, prevent `latch` deadlocks on task exceptions or `stop()`, and detect final-chunk write errors.
4. Migrate `connection_pool.cpp` from legacy GenQuery1 (`query_builder`/`irods::query`) to GenQuery2 (`rc_genquery2`), fix connection-slot leaks on `refresh_connection` exceptions, and eliminate `std::string` hostname copies in `client_connection.cpp`.

**Tech Stack:** C++20, Catch2, `fmt`, `nlohmann::json`, GenQuery2 (`rc_genquery2`), CMake/Make.

---

### Task 1: Zero-Copy, Leak-Free `transport.hpp` & `default_transport.hpp`

**Files:**
- Modify: `/home/darkfell/dev/irods/lib/core/include/irods/transport/transport.hpp`
- Modify: `/home/darkfell/dev/irods/lib/core/include/irods/transport/default_transport.hpp`
- Modify: `/home/darkfell/dev/irods/unit_tests/src/test_dstream.cpp`

- [ ] **Step 1: Harden `basic_transport` in `default_transport.hpp`**
  - Make `basic_transport` non-copyable and `noexcept` move-constructible/assignable (resetting `_other.fd_ = uninitialized_file_descriptor`).
  - Guard `open_impl` against double-open (`if (is_open()) { last_error_ = SYS_NOT_ALLOWED; return false; }`), overlong paths (`_path.string().size() >= sizeof(input.objPath)` $\rightarrow$ `USER_STRLEN_TOOLONG`), and invalid openmode flags (`SYS_INVALID_INPUT_PARAM`).
  - Guard `close()` when `!is_open()`.
  - Replace `addKeyVal` / `clearKeyVal` and `std::to_string` in `open()` overloads with a stack-bound `condInput` helper and `std::to_chars`, and capture `_replica_token` by `const&`.
  - Replace throwing `nlohmann::json::parse` and `irods::hierarchy_parser` in `open_impl` with non-throwing `nlohmann::json::parse(json_output, nullptr, false)` and zero-copy `std::string_view` hierarchy slicing.
  - Replace `nlohmann::json` construction and `.dump()` in `close()` with stack-formatted JSON via `fmt::format_to_n`.

---

### Task 2: Zero-Copy, Bug-Free `dstream.hpp` & `stream_factory_utility.hpp`

**Files:**
- Modify: `/home/darkfell/dev/irods/lib/core/include/irods/dstream.hpp`
- Modify: `/home/darkfell/dev/irods/lib/core/include/irods/stream_factory_utility.hpp`
- Modify: `/home/darkfell/dev/irods/unit_tests/src/test_dstream.cpp`

- [ ] **Step 1: Fix `basic_data_object_buf` and `basic_dstream` in `dstream.hpp`**
  - Replace `std::array<char_type, buffer_size> buf_` with `std::vector<char_type> buf_` (initialized to `default_buffer_size = 4096`) so `swap()` and move construction/assignment are $O(1)$ zero-copy and never leave `std::basic_streambuf` pointers (`eback`, `gptr`, `egptr`, `pbase`, `pptr`, `epptr`) dangling into a moved-from object.
  - Guard `open()` when `is_open()` is already `true` (`return nullptr`).
  - Make `file_descriptor()` and `last_error()` null-safe when `transport_ == nullptr`.
  - Fix `xsgetn`:
    - Clamp `bytes_to_copy = std::min<std::streamsize>(this->egptr() - this->gptr(), _buffer_size)`.
    - If `remaining = _buffer_size - bytes_to_copy` is `0`, return `bytes_to_copy`.
    - If `remaining >= static_cast<std::streamsize>(buf_.size())`, read directly into `_buffer + bytes_to_copy` via `transport_->receive` (zero-copy).
    - If `remaining < static_cast<std::streamsize>(buf_.size())`, refill `buf_` via `transport_->receive(buf_.data(), buf_.size())`, copy up to `remaining` bytes into `_buffer + bytes_to_copy`, and update `setg` for the unconsumed remainder.
    - Return non-negative total bytes read (`[0, _buffer_size]`).
  - Fix `xsputn` and `flush_buffer`:
    - Loop or `std::memmove` on partial writes in `flush_buffer`, and return `0` on error in `xsputn`.
  - Fix `seekoff` and `seekpos`:
    - When `this->gptr() < this->egptr()` and `_dir == std::ios_base::cur`, adjust `_off -= static_cast<off_type>(this->egptr() - this->gptr())`.
    - Invalidate the Get area (`this->setg(pbase, pbase, pbase)`) after seeking so subsequent reads fetch from the new offset.
- [ ] **Step 2: Harden `managed_dstream` and `make_dstream_factory` in `stream_factory_utility.hpp`**
  - Propagate `d.stream.rdstate()` to `managed_dstream` via `d.clear(d.stream.rdstate())` on `open`, `close`, and move.
  - Capture `filesystem::path` in `make_dstream_factory` closures so parallel channel creation passes `const filesystem::path&` with zero copies.

---

### Task 3: Deadlock-Free `parallel_transfer_engine.hpp`

**Files:**
- Modify: `/home/darkfell/dev/irods/lib/core/include/irods/parallel_transfer_engine.hpp`
- Modify: `/home/darkfell/dev/irods/unit_tests/src/test_parallel_transfer_engine.cpp`

- [ ] **Step 1: Harden `parallel_transfer_engine.hpp`**
  - Ensure every secondary task (`!_wait_for_sibling_tasks_to_finish`) decrements `latch_` via `irods::at_scope_exit` regardless of exceptions.
  - Update `stop()` to set `stop_.store(true)` without calling `thread_pool_->stop()` prematurely, ensuring all dispatched tasks execute their `latch_->count_down()` and close handlers cleanly.
  - Add exception cleanup in `start_transfer()` so if stream creation throws after some secondary tasks are already dispatched, `stop_` is signaled, remaining latch counts are decremented, and `thread_pool_->join()` completes before the exception escapes.
  - Fix the inner transfer loop to check `in.gcount() > 0` before `out.write` and check `!in` / `!out` immediately after each I/O operation so final-chunk errors are always recorded.

---

### Task 4: GenQuery2 & Concurrency Hardening in `connection_pool.cpp` & `client_connection.cpp`

**Files:**
- Modify: `/home/darkfell/dev/irods/lib/core/src/connection_pool.cpp`
- Modify: `/home/darkfell/dev/irods/lib/core/include/irods/client_connection.hpp`
- Modify: `/home/darkfell/dev/irods/lib/core/src/client_connection.cpp`
- Modify: `/home/darkfell/dev/irods/unit_tests/src/test_dstream.cpp`

- [ ] **Step 1: Migrate `connection_pool.cpp` to GenQuery2 & fix slot leak / busy-spin**
  - Replace `query_builder` and `irods::query` in `connection_pool.cpp` with `rc_genquery2` (`irods/genquery2.h`).
  - Guard `conn_ctxs_[i].in_use` with `irods::at_scope_exit` in `get_connection()` so an exception in `refresh_connection(i)` resets `in_use` to `false`.
  - Yield (`std::this_thread::yield()`) after a full pass over `conn_ctxs_` when all connections are busy.
- [ ] **Step 2: Zero-copy hostname handling in `client_connection.hpp` / `client_connection.cpp`**
  - Change private `connect_and_login` and `only_connect` helpers to accept `std::string_view _host` and copy into a stack `char host_buf[NAME_LEN]{}` for `rcConnect` / `_rcConnect`, eliminating heap `std::string{_host}` allocations.
- [ ] **Step 3: Migrate `test_dstream.cpp` GenQuery1 usage & compile all targets**
  - Build `irods_client`, `irods_server`, `istream`, `irods_api_plugin-touch_server`, `irods_dstream`, `irods_parallel_transfer_engine`, and `irods_connection_pool` with 0 warnings and 0 errors.
