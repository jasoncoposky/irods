# iRODS 5.1 Stage 3 Design: Universal GenQuery2 AST Builder & Codebase-Wide Refactoring

**Date:** 2026-10-19  
**Branch:** `feature/filesystem-modernization` (merged with `feature/genquery2-odbc-icat-modernization-mainline`)  
**Scope:**
- `lib/core/include/irods/genquery2_ast_types.hpp` (promoted from `server/genquery2/include/irods/private/genquery2_ast_types.hpp`)
- `lib/core/include/irods/genquery2_builder.hpp` (promoted from `server/genquery2/include/irods/private/genquery2_builder.hpp`)
- `lib/filesystem/src/filesystem.cpp`
- `lib/core/src/connection_pool.cpp`
- `lib/core/include/irods/replica.hpp`
- `lib/metadata/src/metadata.cpp`
- `lib/administration/user/src/user_administration.cpp`
- `lib/administration/resource/src/resource_administration.cpp`
- `lib/administration/zone/src/zone_administration.cpp`
- `plugins/api/src/touch.cpp`
- `plugins/api/src/atomic_apply_metadata_operations.cpp`
- `plugins/api/src/atomic_apply_acl_operations.cpp`
- `plugins/experimental/include/irods/private/parallel_filesystem_operation.hpp`
- `server/api/src/rsDataObjCopy.cpp`
- `server/api/src/rsDataObjRename.cpp`
- `server/core/src/irods_resource_manager.cpp`
- `server/core/src/user_validation_utilities.cpp`
- `lib/core/src/lsUtil.cpp`
- `clients/icommands/src/itree.cpp`

---

## 1. Architectural Vision

Following the modernization of `irods::experimental::filesystem` (Stage 1), the transport/connection layer (Stage 2), and the database plugin (`feature/genquery2-odbc-icat-modernization-mainline`), Stage 3 unifies the entire codebase around three core invariants:
1. **Universal Intermediary AST & Fluent Query Builder (`irods::experimental::genquery2::builder`)**:
   - Promote `genquery2_ast_types.hpp` and `genquery2_builder.hpp` to `lib/core/include/irods/` (while keeping forwarding headers in `server/genquery2/include/irods/private/` for 100% backward compatibility).
   - Provide `gq2::to_query_string(const select&)` (which serializes the `gq2::select` AST into canonical GenQuery2 DSL with automatic single-quote doubling on all string literals) and `gq2::execute(RxComm& _comm, const select& _ast, std::string_view _zone_hint = {})` (which executes the AST via `rc_genquery2` on the client or `rs_genquery2` on the server and returns the parsed `nlohmann::json` result array).
   - Eliminate raw SQL/GenQuery string assembly (`fmt::format("SELECT ...")`) across `filesystem`, `connection_pool`, `replica`, `metadata`, `administration`, `plugins/api`, and `server`.
2. **Strict Zero-Copy Path & Buffer Discipline**:
   - Replace `fs::path::parent_path()` and `fs::path::object_name()` temporary allocations in catalog query and validation paths with `parent_path_view()` and `object_name_view()`.
   - Replace `std::stoull` / `std::stoi` / `std::stoll` with zero-allocation `std::from_chars`.
   - Replace `std::stringstream` and runtime `std::map` lookup tables with stack buffers (`fmt::format_to_n`) and `constexpr` `switch` tables.
3. **Single-RPC `status_options::basic` Fast Paths**:
   - Replace multi-RPC `exists` + `status` + `is_collection_registered` + `is_data_object_registered` chains in `touch.cpp`, `atomic_apply_acl_operations.cpp`, `parallel_filesystem_operation.hpp`, and `lsUtil.cpp` with single-RPC `status(..., fs::status_options::basic)`.
