# iRODS 5.1 Transport & Connection Layer Modernization — Design Specification

**Date**: 2026-10-18  
**Branch**: `feature/filesystem-modernization`  
**Stage**: 2 of 3 (Transport & Connection Layer Modernization)

---

## 1. Objectives & Principles

1. **Zero-Copy & Zero-Allocation Hot Paths**:
   - Eliminate heap allocations (`addKeyVal` / `strdup` / `free`, `std::to_string`, `nlohmann::json::dump`, `irods::hierarchy_parser` `std::vector<std::string>` splitting) from `basic_transport::open` and `basic_transport::close`.
   - Maintain a direct zero-copy read/write fast path in `basic_data_object_buf::xsgetn` and `xsputn` for requests $\ge$ buffer size while buffering small reads (`< buffer_size`) to eliminate RPC amplification.
   - Store `irods::experimental::filesystem::path` in `make_dstream_factory` closures so opening parallel transfer channels does not re-parse or re-allocate path strings.
   - Avoid unnecessary `std::string` allocations when passing `std::string_view` hostnames into `client_connection`.

2. **Memory & Descriptor Safety (Zero Leaks, Zero UB)**:
   - Make `basic_transport` move-constructible and move-assignable (transferring `fd_` and resetting `_other.fd_ = uninitialized_file_descriptor`) and non-copyable so two transport objects can never alias and double-close the same replica descriptor.
   - Prevent server-side replica descriptor leaks in `basic_transport::open_impl` by replacing throwing `nlohmann::json::parse` and `hierarchy_parser` with non-throwing JSON parsing (`nlohmann::json::parse(..., nullptr, false)`) and zero-copy `std::string_view` hierarchy slicing.
   - Fix the dangling pointer UB in `basic_data_object_buf::swap` / move construction where `std::basic_streambuf` pointers (`eback`, `gptr`, `egptr`, `pbase`, `pptr`, `epptr`) remained pointing into the moved-from `std::array<char, 4096>`.
   - Fix the buffer overflow and negative read size bugs in `basic_data_object_buf::xsgetn`.
   - Fix the stale Get-area bug and `SEEK_CUR` read-ahead offset bug in `basic_data_object_buf::seekoff` and `seekpos`.

3. **Concurrency & Deadlock Elimination**:
   - Guarantee `latch_->count_down()` via RAII (`irods::at_scope_exit`) in `parallel_transfer_engine` secondary tasks so an exception never deadlocks the primary channel waiting in `latch_->wait()`.
   - Fix `parallel_transfer_engine::stop()` so signalling stop does not drop unstarted tasks from `thread_pool_` before they count down `latch_`.
   - Clean up any already-scheduled secondary tasks if stream creation throws mid-loop in `parallel_transfer_engine::start_transfer()`.
   - Fix connection-slot leak on `refresh_connection` exception and eliminate the 100% CPU busy-spin loop in `connection_pool::get_connection()`.

4. **GenQuery2 Integration**:
   - Migrate `connection_pool.cpp` from legacy GenQuery1 (`query_builder` and `irods::query`) to GenQuery2 (`rc_genquery2`) for resource modification tracking (`RESC_MODIFY_TIME`, `RESC_MODIFY_TIME_MILLIS`) and connection liveness verification (`ZONE_NAME`).

5. **100% Source & ABI Compatibility**:
   - Preserve all public types, method signatures, and namespaces across `transport.hpp`, `default_transport.hpp`, `dstream.hpp`, `stream_factory_utility.hpp`, `parallel_transfer_engine.hpp`, `connection_pool.hpp`, and `client_connection.hpp`.

---

## 2. Component Architecture & Changes

### 2.1 `transport.hpp` & `default_transport.hpp`
- **`basic_transport` Ownership & Move Semantics**:
  - Delete copy constructor and copy assignment operator.
  - Provide `noexcept` move constructor and move assignment operator that transfer `fd_` and reset `_other.fd_ = uninitialized_file_descriptor`.
- **Precondition Guards**:
  - In `open_impl`, if `is_open()` is already `true`, set `last_error_ = SYS_NOT_ALLOWED` and return `false`.
  - If `_path.string().size() >= sizeof(input.objPath)`, set `last_error_ = USER_STRLEN_TOOLONG` and return `false`.
  - If `make_open_flags(_mode) == translation_error`, set `last_error_ = SYS_INVALID_INPUT_PARAM` and return `false`.
  - In `close()`, if `!is_open()`, set `last_error_ = FILE_INDEX_LOOKUP_ERR` and return `false`.
- **Zero-Copy `condInput` Binding**:
  - Introduce a stack-allocated `static_cond_input<2>` helper inside `basic_transport` that binds `char* key[2]` and `char* value[2]` directly into `dataObjInp_t::condInput` and formats integer replica numbers into a stack `char[16]` buffer via `std::to_chars`—eliminating `addKeyVal` (`strdup`), `clearKeyVal` (`free`), and `std::to_string`.
  - Capture `_replica_token` by `const&` in `open()` lambdas.
- **Non-Throwing JSON & Zero-Copy Hierarchy Extraction**:
  - Parse `json_output` with `nlohmann::json::parse(json_output, nullptr, false)`.
  - Validate `data_object_info`, `resource_hierarchy`, `replica_number`, and `replica_token` without exceptions.
  - Extract `root_resc_name_` and `leaf_resc_name_` from `resource_hierarchy` via `std::string_view` (`find(';')` and `rfind(';')`) with zero intermediate allocations.
- **Zero-Allocation `close()`**:
  - Format the `rx_replica_close` JSON payload into a stack `char json_buf[256]` via `fmt::format_to_n`.

### 2.2 `dstream.hpp` (`basic_data_object_buf` & `basic_dstream`)
- **Zero-Copy Move/Swap Without Dangling Pointers**:
  - Store `std::vector<char_type> buf_` (sized to `default_buffer_size = 4096`, or configurable via constructor) in `basic_data_object_buf`.
  - Moving or swapping `basic_data_object_buf` swaps `buf_` in $O(1)$ without invalidating `eback()`, `gptr()`, `egptr()`, `pbase()`, `pptr()`, or `epptr()`.
- **Correct, Zero-Copy `xsgetn`**:
  - Clamp `bytes_to_copy = std::min<std::streamsize>(this->egptr() - this->gptr(), _buffer_size)`.
  - Copy `bytes_to_copy` from `gptr()` and advance `gbump(static_cast<int>(bytes_to_copy))`.
  - If `remaining = _buffer_size - bytes_to_copy` is `0`, return `bytes_to_copy`.
  - If `remaining >= static_cast<std::streamsize>(buf_.size())`, call `transport_->receive(_buffer + bytes_to_copy, remaining)` directly into the caller's buffer (zero-copy).
  - If `remaining < static_cast<std::streamsize>(buf_.size())`, refill `buf_` via `transport_->receive(buf_.data(), buf_.size())`, copy `std::min(remaining, bytes_read)` to `_buffer + bytes_to_copy`, and set the Get area for the remaining buffered bytes.
  - Always return the non-negative total byte count extracted (`[0, _buffer_size]`).
- **Correct `xsputn` & `flush_buffer`**:
  - Handle partial writes in `flush_buffer` safely.
  - Return `0` (not `-1`) on error in `xsputn`, or the number of bytes written (`[0, _buffer_size]`).
- **Correct `seekoff` & `seekpos`**:
  - When `this->gptr() < this->egptr()` and `_dir == std::ios_base::cur`, adjust `_off -= static_cast<off_type>(this->egptr() - this->gptr())` so `tellg()` and relative `seekg()` account for unconsumed read-ahead bytes.
  - Reset the Get area (`this->setg(buf_.data(), buf_.data(), buf_.data())`) after seeking so subsequent reads fetch from the new offset.
- **Null-Safe Accessors & Double-Open Guard**:
  - If `is_open()` is `true`, `basic_data_object_buf::open(...)` returns `nullptr`.
  - `file_descriptor()` returns `transport_ ? transport_->file_descriptor() : -1`.
  - `last_error()` returns `transport_ ? transport_->last_error() : 0`.

### 2.3 `stream_factory_utility.hpp`
- **Propagate `d.stream` State to `managed_dstream`**:
  - Call `d.clear(d.stream.rdstate())` after `d.stream.open(...)` in `make_dstream_factory` and after `stream.close(...)` in `managed_dstream::close()` so `static_cast<bool>(d)` accurately reflects stream errors.
- **Zero-Copy Path Capture**:
  - Store `filesystem::path p{std::move(_logical_path)}` in the `make_dstream_factory` lambda closure so `d.stream.open(*d.transport, p, ...)` passes `const filesystem::path&` without constructing a temporary `filesystem::path` per channel.

### 2.4 `parallel_transfer_engine.hpp`
- **Deadlock-Free Latch & Shutdown**:
  - Register `irods::at_scope_exit count_down_on_exit{[this] { latch_->count_down(); }};` at the very top of every secondary task (`!_wait_for_sibling_tasks_to_finish`).
  - In `stop()`, set `stop_.store(true)` without calling `thread_pool_->stop()`, allowing all queued tasks to run, observe `stop_`, decrement `latch_`, and close their streams cleanly.
  - In `start_transfer()`, wrap the secondary stream creation loop so that if `create_source_stream` or `create_sink_stream` throws, `stop_.store(true)` is set, unscheduled latch counts are decremented, and `thread_pool_->join()` is called before rethrowing.
- **Immediate I/O Error Detection**:
  - Check `in.gcount() > 0` before writing to `out`, and check `!in` (if short read before chunk completion) and `!out` immediately after `read`/`write` so errors on the final iteration of a chunk are always captured in `errors_`.

### 2.5 `connection_pool.cpp` & `client_connection.cpp`
- **GenQuery2 Migration in `connection_pool.cpp`**:
  - Replace `query_builder` and `irods::query` with `rc_genquery2`:
    - Resource state query: `"select RESC_MODIFY_TIME, RESC_MODIFY_TIME_MILLIS order by RESC_MODIFY_TIME desc, RESC_MODIFY_TIME_MILLIS desc"`
    - Liveness query: `"select ZONE_NAME where ZONE_TYPE = 'local'"`
  - Remove `#include "irods/irods_query.hpp"`, `#include "irods/query_builder.hpp"`, and `#include "irods/rodsGenQuery.h"` from `connection_pool.cpp`.
- **Exception-Safe Slot Acquisition & Contention Backoff in `connection_pool::get_connection()`**:
  - Guard `conn_ctxs_[i].in_use` with `irods::at_scope_exit` until `refresh_connection(i)` succeeds so an exception never permanently leaks a connection slot.
  - Yield (`std::this_thread::yield()`) after a full pass over `conn_ctxs_` when all connections are busy.
- **Zero-Copy Hostname Handling in `client_connection.cpp`**:
  - Pass `std::string_view` directly to `connect_and_login` / `only_connect` and copy into a stack `char host_buf[NAME_LEN]{}` for `rcConnect` / `_rcConnect` instead of heap-allocating `std::string{_host}`.
