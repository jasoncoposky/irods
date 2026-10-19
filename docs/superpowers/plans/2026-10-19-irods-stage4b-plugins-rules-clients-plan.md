# iRODS 5.1 Stage 4B: Plugins, Rule Engines, and Client Utilities GenQuery2 AST Modernization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Migrate remaining legacy `rsGenQuery`, `rcGenQuery`, `irods::query`, and `query_builder` call sites across `server/api`, `server/re`, `plugins/`, `lib/core`, and `clients/icommands` onto `gq2::builder` ASTs, zero-copy `fs::path` views, and `std::from_chars` in atomic, rebase-ready commits.

---

### Task 1: GenQuery2 SQL Lowering Extensions (`SL_*`, `SLD_*`, `QUOTA_*`, `TOKEN_*`, `RULE_EXEC_*`)

**Files:**
- Modify: `server/genquery2/include/irods/private/genquery2_table_column_mappings.hpp`
- Modify: `server/genquery2/src/genquery2_sql.cpp`
- Modify: `unit_tests/src/test_genquery2_sql_dml.cpp`

### Task 2: Server API, Rule Engine, & Header Hygiene

**Files:**
- Modify: `server/api/src/rsDataObjPhymv.cpp`
- Modify: `server/re/src/reIn2p3SysRule.cpp`
- Modify: `server/re/src/reSysDataObjOpr.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/configuration.cpp`
- Modify: `server/api/src/rsRuleExecDel.cpp`
- Modify: `server/core/src/irods_resource_manager.cpp`
- Modify: `server/core/src/miscServerFunct.cpp`
- Modify: `lib/core/include/irods/replica.hpp`
- Modify: `lib/administration/zone/include/irods/zone_administration.hpp`

### Task 3: API, Rule Engine, & Resource Plugins (`replica_close`, `set_delay_server_migration_info`, `cpp_default_policy`, `load_balanced`, `irods_repl_rebalance`, `librepl`)

**Files:**
- Modify: `plugins/api/src/replica_close.cpp`
- Modify: `plugins/api/src/set_delay_server_migration_info.cpp`
- Modify: `plugins/rule_engines/src/cpp_default_policy.cpp`
- Modify: `plugins/resources/src/load_balanced.cpp`
- Modify: `plugins/resources/replication/src/irods_repl_rebalance.cpp`
- Modify: `plugins/resources/replication/src/librepl.cpp`

### Task 4: Client Core Utilities & iCommands (`rsyncUtil.cpp`, `lsUtil.cpp`, `scanUtil.cpp`, `iuserinfo.cpp`, `iadmin.cpp`)

**Files:**
- Modify: `lib/core/src/rsyncUtil.cpp`
- Modify: `lib/core/src/lsUtil.cpp`
- Modify: `lib/core/src/scanUtil.cpp`
- Modify: `clients/icommands/src/iuserinfo.cpp`
- Modify: `clients/icommands/src/iadmin.cpp`
