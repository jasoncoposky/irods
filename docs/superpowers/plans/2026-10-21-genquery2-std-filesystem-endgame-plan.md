# GenQuery2 & `std::filesystem` Endgame Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the last remaining consumers of `irods::query`, `irods::experimental::query_builder`, `irods::query_processor` (via `irods::query`), and `boost::filesystem` / `libboost_filesystem.so` across the entire iRODS repository so `make all` and `scripts/verify_refactoring.sh` pass with zero errors.

**Architecture:**
1. Replace all remaining `irods::query` and `irods::experimental::query_builder` call sites in `lib/core/`, `server/`, `plugins/`, and `unit_tests/` with the fluent GenQuery2 AST builder (`irods::experimental::genquery2::builder`).
2. Replace all remaining `boost::filesystem` headers and namespace usages in `lib/core/`, `server/`, `plugins/`, and `unit_tests/` with C++17 `std::filesystem`, handling root `parent_path()` termination and `std::filesystem::perm_options` differences.
3. Remove `${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so` from all `CMakeLists.txt` and `unit_tests/cmake/test_config/*.cmake` files.

**Tech Stack:** C++17 (`std::filesystem`), iRODS GenQuery2 AST builder (`irods::experimental::genquery2::builder`), Catch2, CMake / Ninja.

---

### Task 1: Migrate Core, Server, and Microservice Consumers to `gq2::builder`

**Files:**
- Modify: `lib/core/include/irods/query_processor.hpp`
- Modify: `lib/core/include/irods/experimental_plugin_framework.hpp`
- Modify: `server/delay_server/src/irodsDelayServer.cpp`
- Modify: `plugins/microservices/src/test_delay_rule_locking_api.cpp`

- [ ] **Step 1: Migrate `lib/core/include/irods/query_processor.hpp` from `irods::query` to `gq2::builder` / `gq2::execute`**
  - Replace `#include "irods/irods_query.hpp"` with `#include "irods/genquery2_ast_builder.hpp"` and `#include "irods/genquery2_driver.hpp"`.
  - Allow `query_processor` to accept either a GenQuery2 query string or a `gq2::builder::select_builder` AST and execute via `irods::experimental::genquery2::execute(_conn, query_)`.
- [ ] **Step 2: Remove unused `#include "irods/query_builder.hpp"` from `lib/core/include/irods/experimental_plugin_framework.hpp`**
- [ ] **Step 3: Migrate `server/delay_server/src/irodsDelayServer.cpp` to `gq2::builder` and `std::filesystem`**
  - Replace `#include "irods/irods_query.hpp"` and `#include <boost/filesystem.hpp>` with `#include "irods/genquery2_ast_builder.hpp"` and `#include "irods/genquery2_driver.hpp"`.
  - Construct the delay server rule query using `gq2::builder::select({"DELAY_RULE_ID", gq2::builder::order_desc("DELAY_RULE_PRIORITY")})` with `DELAY_RULE_TIME <= now` and empty `DELAY_RULE_LOCK_HOST`, `DELAY_RULE_LOCK_HOST_PID`, and `DELAY_RULE_LOCK_TIME` conditions, executed via `gq2::execute( static_cast<rcComm_t&>(conn), q )`.
- [ ] **Step 4: Migrate `plugins/microservices/src/test_delay_rule_locking_api.cpp` to `gq2::builder`**
  - Replace `#include "irods/irods_query.hpp"` with `#include "irods/genquery2_ast_builder.hpp"` and `#include "irods/genquery2_driver.hpp"`.
  - Convert `RULE_EXEC_*` queries to `DELAY_RULE_*` `gq2::builder::select` ASTs executed via `gq2::execute(*_rei->rsComm, q)`.
- [ ] **Step 5: Build and commit Task 1**
  - Run: `cmake --build /home/darkfell/dev/irods/build --target irodsDelayServer irods_microservice_plugin-test_delay_rule_locking_api -j$(nproc)`

---

### Task 2: Migrate All 13 Unit Test Files to `gq2::builder`

**Files:**
- Modify: `unit_tests/src/test_delay_rule_locking_api.cpp`
- Modify: `unit_tests/src/test_get_delay_rule_info.cpp`
- Modify: `unit_tests/src/test_rc_data_obj.cpp`
- Modify: `unit_tests/src/test_replica_truncate.cpp`
- Modify: `unit_tests/src/test_replica_open_and_close.cpp`
- Modify: `unit_tests/src/test_dstream.cpp`
- Modify: `unit_tests/src/filesystem/test_filesystem.cpp`
- Modify: `unit_tests/src/test_authenticate_client.cpp`
- Modify: `unit_tests/src/test_get_resource_info_for_operation.cpp`
- Modify: `unit_tests/src/test_json_apis_from_client.cpp`
- Modify: `unit_tests/src/test_rc_mod_data_obj_meta.cpp`
- Modify: `unit_tests/src/test_rc_update_replica_access_time.cpp`
- Modify: `unit_tests/src/test_ticket_administration.cpp`
- Modify: `unit_tests/src/test_query_builder.cpp`

- [ ] **Step 1: Migrate `test_rc_data_obj.cpp` and `test_replica_truncate.cpp` (currently failing compilation due to missing `irods::query`) to `gq2::builder`**
- [ ] **Step 2: Migrate `test_delay_rule_locking_api.cpp` and `test_get_delay_rule_info.cpp` to `gq2::builder` (`DELAY_RULE_*` columns)**
- [ ] **Step 3: Migrate `test_replica_open_and_close.cpp`, `test_dstream.cpp`, `filesystem/test_filesystem.cpp`, `test_authenticate_client.cpp`, `test_get_resource_info_for_operation.cpp`, `test_json_apis_from_client.cpp`, `test_rc_mod_data_obj_meta.cpp`, and `test_rc_update_replica_access_time.cpp` to `gq2::builder`**
- [ ] **Step 4: Migrate `test_ticket_administration.cpp` and `test_query_builder.cpp` from `irods::experimental::query_builder` to `gq2::builder`**
- [ ] **Step 5: Build all unit test targets and commit Task 2**

---

### Task 3: Migrate Remaining `boost::filesystem` Usages to `std::filesystem`

**Files:**
- Modify: `lib/core/include/irods/irods_default_paths.hpp.in`
- Modify: `lib/core/src/irods_default_paths.cpp`
- Modify: `lib/core/include/irods/irods_path_recursion.hpp`
- Modify: `lib/core/include/irods/irods_load_plugin.hpp`
- Modify: `lib/core/include/irods/parallel_transfer_engine.hpp`
- Modify: `lib/core/src/client_connection.cpp`
- Modify: `lib/core/src/dns_cache.cpp`
- Modify: `lib/core/src/hostname_cache.cpp`
- Modify: `lib/core/src/irods_client_server_negotiation.cpp`
- Modify: `lib/core/src/irods_configuration_keywords.cpp`
- Modify: `lib/core/src/irods_environment_properties.cpp`
- Modify: `lib/core/src/irods_log.cpp`
- Modify: `lib/core/src/irods_pack_table.cpp`
- Modify: `lib/core/src/irods_plugin_context.cpp`
- Modify: `lib/core/src/irods_plugin_name_generator.cpp`
- Modify: `lib/core/src/irods_Resource.cpp`
- Modify: `lib/core/src/irods_server_properties.cpp`
- Modify: `lib/core/src/irods_Signal.cpp`
- Modify: `lib/core/src/osauth.cpp`
- Modify: `lib/core/src/procApiRequest.cpp`
- Modify: `lib/core/src/rcConnect.cpp`
- Modify: `lib/core/src/rcMisc.cpp`
- Modify: `lib/core/src/rodsLog.cpp`
- Modify: `lib/core/src/sockComm.cpp`
- Modify: `server/main_server/src/rodsServer.cpp`
- Modify: `server/core/include/irods/rodsAgent.hpp`
- Modify: `server/core/src/catalog_utilities.cpp`
- Modify: `server/core/src/client_api_allowlist.cpp`
- Modify: `server/core/src/initServer.cpp`
- Modify: `server/core/src/irods_Dynamic_Cast.cpp`
- Modify: `server/core/src/irods_physical_object.cpp`
- Modify: `server/core/src/irods_report_plugins_in_json.cpp`
- Modify: `server/core/src/irods_resource_backport.cpp`
- Modify: `server/core/src/irods_resource_manager.cpp`
- Modify: `server/core/src/irods_resource_redirect.cpp`
- Modify: `server/core/src/irods_server_control_plane.cpp`
- Modify: `server/core/src/irods_server_state.cpp`
- Modify: `server/core/src/miscServerFunct.cpp`
- Modify: `server/core/src/objDesc.cpp`
- Modify: `server/core/src/physPath.cpp`
- Modify: `server/core/src/procLog.cpp`
- Modify: `server/core/src/rodsAgent.cpp`
- Modify: `server/core/src/rsLog.cpp`
- Modify: `server/core/src/server_utilities.cpp`
- Modify: `server/core/src/specColl.cpp`
- Modify: `server/api/src/rsDataObjClose.cpp`
- Modify: `server/api/src/rsDataObjCreate.cpp`
- Modify: `server/api/src/rsDataObjOpen.cpp`
- Modify: `server/api/src/rsDataObjPut.cpp`
- Modify: `server/api/src/rsDataObjRead.cpp`
- Modify: `server/api/src/rsDataObjRepl.cpp`
- Modify: `server/api/src/rsDataObjUnlink.cpp`
- Modify: `server/api/src/rsDataObjWrite.cpp`
- Modify: `server/api/src/rsFileChksum.cpp`
- Modify: `server/api/src/rsFilePut.cpp`
- Modify: `server/api/src/rsFileRead.cpp`
- Modify: `server/api/src/rsGetGridConfigurationValue.cpp`
- Modify: `server/api/src/rsGetHostForGet.cpp`
- Modify: `server/api/src/rsGetHostForPut.cpp`
- Modify: `server/api/src/rsIESClientHints.cpp`
- Modify: `server/api/src/rsModAVUMetadata.cpp`
- Modify: `server/api/src/rsPhyPathReg.cpp`
- Modify: `server/api/src/rsReplicaClose.cpp`
- Modify: `server/api/src/rsServerReport.cpp`
- Modify: `server/api/src/rsSetGridConfigurationValue.cpp`
- Modify: `server/api/src/rsSubStructFileGet.cpp`
- Modify: `server/api/src/rsSubStructFilePut.cpp`
- Modify: `server/api/src/rsSubStructFileRead.cpp`
- Modify: `server/api/src/rsSyncMountedColl.cpp`
- Modify: `server/re/src/irods_re_plugin.cpp`
- Modify: `server/re/src/irods_re_serialization.cpp`
- Modify: `server/re/src/reHelpers1.cpp`
- Modify: `server/re/src/reIn2p3SysRule.cpp`
- Modify: `server/re/src/reNaraMetaData.cpp`
- Modify: `server/re/src/reServerLib.cpp`
- Modify: `server/re/src/reStruct.cpp`
- Modify: `server/re/src/reSysDataObjOpr.cpp`
- Modify: `server/re/src/rules.cpp`
- Modify: `plugins/api/src/authenticate.cpp`
- Modify: `plugins/api/src/data_object_finalize.cpp`
- Modify: `plugins/api/src/data_object_modify_info.cpp`
- Modify: `plugins/api/src/get_file_descriptor_info.cpp`
- Modify: `plugins/api/src/get_grid_configuration_value.cpp`
- Modify: `plugins/api/src/replica_close.cpp`
- Modify: `plugins/api/src/replica_open.cpp`
- Modify: `plugins/api/src/replica_truncate.cpp`
- Modify: `plugins/api/src/set_grid_configuration_value.cpp`
- Modify: `plugins/api/src/touch.cpp`
- Modify: `plugins/auth/src/pam_interactive.cpp`
- Modify: `plugins/database/src/db_plugin.cpp`
- Modify: `plugins/microservices/src/msiSetQuota.cpp`
- Modify: `plugins/microservices/src/update_unixfilesystem_resource_free_space.cpp`
- Modify: `plugins/resources/passthru/src/passthru.cpp`
- Modify: `plugins/resources/replication/src/irods_create_write_replicator.cpp`
- Modify: `plugins/resources/replication/src/irods_object_oper.cpp`
- Modify: `plugins/resources/replication/src/irods_open_replicator.cpp`
- Modify: `plugins/resources/replication/src/irods_repl_rebalance.cpp`
- Modify: `plugins/resources/replication/src/irods_repl_retry.cpp`
- Modify: `plugins/resources/replication/src/irods_replicator.cpp`
- Modify: `plugins/resources/replication/src/replication.cpp`
- Modify: `plugins/resources/structfile/src/structfile.cpp`
- Modify: `plugins/resources/univmss/src/univmss.cpp`
- Modify: `plugins/resources/unixfilesystem/src/unixfilesystem.cpp`
- Modify: `plugins/rule_engines/irods_rule_engine_plugin-cpp_default_policy/libirods_rule_engine_plugin-cpp_default_policy.cpp`
- Modify: `plugins/rule_engines/irods_rule_engine_plugin-passthrough/libirods_rule_engine_plugin-passthrough.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/include/irods/private/re/datetime.hpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/configuration.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/filesystem.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/functions.cpp`
- Modify: `unit_tests/src/unit_test_utils.hpp`
- Modify: `unit_tests/src/filesystem/test_filesystem.cpp`
- Modify: `unit_tests/src/test_data_object_finalize.cpp`
- Modify: `unit_tests/src/test_data_object_modify_info.cpp`
- Modify: `unit_tests/src/test_data_object_proxy.cpp`
- Modify: `unit_tests/src/test_dstream.cpp`
- Modify: `unit_tests/src/test_irods_default_paths.cpp`
- Modify: `unit_tests/src/test_irods_key_value_proxy.cpp`
- Modify: `unit_tests/src/test_logical_locking.cpp`
- Modify: `unit_tests/src/test_pack_entry_table.cpp`
- Modify: `unit_tests/src/test_parallel_transfer_engine.cpp`
- Modify: `unit_tests/src/test_physical_object_proxy.cpp`
- Modify: `unit_tests/src/test_plugin_property_ proxy.cpp`
- Modify: `unit_tests/src/test_query_processor.cpp`
- Modify: `unit_tests/src/test_rc_data_obj.cpp`
- Modify: `unit_tests/src/test_replica_open_and_close.cpp`
- Modify: `unit_tests/src/test_replica_proxy.cpp`
- Modify: `unit_tests/src/test_replica_state_table.cpp`
- Modify: `unit_tests/src/test_replication_resource_plugin.cpp`
- Modify: `unit_tests/src/test_resource_administration.cpp`
- Modify: `unit_tests/src/test_scoped_client_identity.cpp`
- Modify: `unit_tests/src/test_scoped_permission.cpp`
- Modify: `unit_tests/src/test_shared_memory_object.cpp`
- Modify: `unit_tests/src/test_struct_file_ext_and_reg.cpp`
- Modify: `unit_tests/src/test_switch_user.cpp`
- Modify: `unit_tests/src/test_user_administration.cpp`
- Modify: `unit_tests/src/test_zone_report.cpp`

- [ ] **Step 1: Migrate `lib/core/` headers and sources from `boost::filesystem` to `std::filesystem`**
- [ ] **Step 2: Migrate `server/` headers and sources from `boost::filesystem` to `std::filesystem`**
- [ ] **Step 3: Migrate `plugins/` headers and sources from `boost::filesystem` to `std::filesystem` (including `parent_path()` root termination guards and `stat()` in `filesystem.cpp`)**
- [ ] **Step 4: Migrate `unit_tests/` headers and sources from `boost::filesystem` to `std::filesystem` (including `fs::perm_options::add`)**
- [ ] **Step 5: Verify `git grep "boost::filesystem\|boost/filesystem" lib/ server/ plugins/ unit_tests/ clients/` returns 0 matches and commit Task 3**

---

### Task 4: Remove `libboost_filesystem.so` from CMake and Run Full Verification

**Files:**
- Modify: `lib/core/CMakeLists.txt`
- Modify: `server/CMakeLists.txt`
- Modify: `server/main_server/CMakeLists.txt`
- Modify: `server/delay_server/CMakeLists.txt`
- Modify: `plugins/api/CMakeLists.txt`
- Modify: `plugins/auth/CMakeLists.txt`
- Modify: `plugins/database/CMakeLists.txt`
- Modify: `plugins/microservices/CMakeLists.txt`
- Modify: `plugins/network/CMakeLists.txt`
- Modify: `plugins/resources/CMakeLists.txt`
- Modify: `plugins/rule_engines/irods_rule_engine_plugin-cpp_default_policy/CMakeLists.txt`
- Modify: `plugins/rule_engines/irods_rule_engine_plugin-passthrough/CMakeLists.txt`
- Modify: `plugins/rule_engines/irods_rule_language/CMakeLists.txt`
- Modify: `test/c_api_test/CMakeLists.txt`
- Modify: `unit_tests/cmake/test_config/*.cmake` (all 37 files referencing `libboost_filesystem.so`)

- [ ] **Step 1: Remove `${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so` from all `CMakeLists.txt` and `unit_tests/cmake/test_config/*.cmake` files**
- [ ] **Step 2: Run full repository build (`cmake --build /home/darkfell/dev/irods/build -j$(nproc)`) and confirm zero errors**
- [ ] **Step 3: Run `scripts/verify_refactoring.sh` and standalone unit tests (`irods_default_paths`, `irods_genquery2_ast_builder`, `irods_collections_architecture`, `irods_query_builder`)**
- [ ] **Step 4: Commit CMake changes and Stage 9 docs (`docs/superpowers/`)**
