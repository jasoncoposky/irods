# iRODS 5.1 Stage 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote the GenQuery2 AST (`genquery2_ast_types.hpp`) and Fluent Builder (`genquery2_builder.hpp`) to `lib/core`, add AST execution & serialization helpers, and refactor `filesystem`, `connection_pool`, `replica`, `metadata`, `administration`, `plugins/api`, `server`, and `icommands` onto the modernized `filesystem`, `transport`, and `gq2::builder` substrates.

---

### Task 1: Promote `genquery2_ast_types.hpp` and `genquery2_builder.hpp` to `lib/core` with AST Serializer & Executor
- [ ] Move AST and builder definitions to `lib/core/include/irods/genquery2_ast_types.hpp` and `lib/core/include/irods/genquery2_builder.hpp`, leaving forwarding `#include` headers in `server/genquery2/include/irods/private/`.
- [ ] Add `to_query_string(const select&)` in `genquery2_builder.hpp` that lowers a `gq2::select` AST to a canonical GenQuery2 DSL string with automatic single-quote escaping (`'` -> `''`) on all literal operands.
- [ ] Add `execute(RxComm& _comm, const select& _sel, std::string_view _zone_hint = {}) -> nlohmann::json` in `genquery2_builder.hpp` (using `rc_genquery2` on `RcComm` and `rs_genquery2` on `RsComm` with RAII buffer cleanup).
- [ ] Add unit tests in `unit_tests/src/test_genquery2_builder.cpp` verifying `to_query_string()` for projections, functions, compound `&&`/`||`/`!` conditions, `group()`, `like`, `in`, `between`, `is_null`, `is_not_null`, `order_by`, `limit`/`offset`, `distinct`, and embedded single quotes.

### Task 2: Refactor `filesystem.cpp` and `connection_pool.cpp` onto `gq2::builder`
- [ ] Replace all 10 raw query string sites in `lib/filesystem/src/filesystem.cpp` with `gq2::builder::select(...)` AST construction and `gq2::to_query_string` / `gq2::execute`.
- [ ] Replace the raw query string in `lib/core/src/connection_pool.cpp` with `gq2::builder::select({"ZONE_NAME"}).where(col("ZONE_TYPE") == "local").build()`.

### Task 3: Refactor `lib/core/include/irods/replica.hpp` onto `gq2::builder` & Zero-Copy Views
- [ ] Migrate `get_data_object_info_impl`, `get_data_object_info(rxComm&, rodsLong_t)`, and both `replica_exists` overloads from legacy `query_builder` (GenQuery1) to `gq2::builder::select` + `gq2::execute`.
- [ ] Collapse the 2-query lookup in `get_data_object_info(_comm, _data_id)` into a single GenQuery2 AST query (`where(col("DATA_ID") == ...)`).
- [ ] Use `_logical_path.parent_path_view()` and `_logical_path.object_name_view()`, `fmt::format_to_n` in `last_write_time_impl`, and `std::from_chars` for integer conversions.

### Task 4: Refactor `lib/metadata` and `lib/administration/{user,resource,zone}` onto `gq2::builder`
- [ ] Refactor `lib/metadata/src/metadata.cpp` to use `constexpr` lookup switches, `parent_path_view()` / `object_name_view()`, and `gq2::builder::select` + `gq2::execute`.
- [ ] Refactor all 11 query sites in `lib/administration/user/src/user_administration.cpp` (including replacing `listGroupsForUser` SpecificQuery + N+1 fallback with a single `gq2::builder::select`), 3 query sites in `lib/administration/resource/src/resource_administration.cpp`, and 2 query sites in `lib/administration/zone/src/zone_administration.cpp` onto `gq2::builder::select` + `gq2::execute`.

### Task 5: Refactor API Plugins, Server Core/APIs, and Client Utilities
- [ ] Refactor `plugins/api/src/touch.cpp`, `plugins/api/src/atomic_apply_metadata_operations.cpp`, `plugins/api/src/atomic_apply_acl_operations.cpp`, and `plugins/experimental/include/irods/private/parallel_filesystem_operation.hpp` onto `status_options::basic` and `gq2::builder::select`.
- [ ] Refactor `server/api/src/rsDataObjCopy.cpp`, `server/api/src/rsDataObjRename.cpp`, `server/core/src/irods_resource_manager.cpp`, `server/core/src/user_validation_utilities.cpp`, `lib/core/src/lsUtil.cpp`, and `clients/icommands/src/itree.cpp`.
- [ ] Build and run all unit tests and targets to verify zero regressions.
