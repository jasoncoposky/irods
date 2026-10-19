# iRODS 5.1 Stage 4B: Plugins, Rule Engines, and Client Utilities GenQuery2 AST Modernization — Design Specification

**Date**: 2026-10-19  
**Target Branch**: `feature/filesystem-modernization` (fork: `git@github.com:jasoncoposky/irods.git`)  
**Scope**: `server/genquery2`, `server/api`, `server/re`, `plugins/api`, `plugins/resources`, `plugins/rule_engines`, `lib/core`, and `clients/icommands`

---

## 1. Executive Summary

Following Stage 4A (`server/core` and core `server/api` catalog query modernization), Stage 4B completes the migration of the remaining high-value catalog queries across:
1. **GenQuery2 SQL Lowering (`server/genquery2`)**:
   - Extend `column_name_mappings` and the join graph in `genquery2_sql.cpp` with:
     - `TOKEN_CREATE_TIME` and `TOKEN_MODIFY_TIME` on `R_TOKN_MAIN`.
     - `R_SERVER_LOAD` (`SL_*`) and `R_SERVER_LOAD_DIGEST` (`SLD_*`).
     - `R_QUOTA_MAIN` (`QUOTA_*`) and `R_QUOTA_USAGE` (`QUOTA_USAGE_*`) along with inner joins to `R_USER_MAIN` and `R_RESC_MAIN`.
     - `RULE_EXEC_*` aliases for `R_RULE_EXEC` (`DELAY_RULE_*`).
   - Generalize `generate_inner_joins` in `genquery2_sql.cpp` so star-topology joins across three or more tables (such as `R_USER_MAIN` <- `R_QUOTA_MAIN` -> `R_RESC_MAIN`) resolve deterministically regardless of column order.
2. **Server API & Rule Engine (`server/api`, `server/re`, `plugins/rule_engines`)**:
   - Migrate `rsDataObjPhymv.cpp`, `reIn2p3SysRule.cpp` (`msiCheckHostAccessControl`), `reSysDataObjOpr.cpp` (`msiSetDataTypeFromExt`), `configuration.cpp` (`readICatUserInfo`), and `cpp_default_policy.cpp` (`rule_is_not_already_enqueued`) onto `gq2::builder` ASTs and direct server execution (`gq2::try_execute` / `gq2::execute`).
   - Fix latent memory leaks (`regParam` in `msiSetDataTypeFromExt`) and buffer-overflow hazards (`strcat` in `msiCheckHostAccessControl`).
3. **API & Resource Plugins (`plugins/api`, `plugins/resources`)**:
   - Migrate `set_delay_server_migration_info.cpp` (`is_hostname_valid`), `load_balanced.cpp` (`get_load_lists`), and `irods_repl_rebalance.cpp` (`get_source_data_object_attributes`, `get_out_of_date_replicas_batch`) onto `gq2::builder` ASTs, eliminating `GenQueryInpWrapper` boilerplate and `boost::lexical_cast`.
   - Fix a switch fallthrough bug in `load_balanced.cpp` (`case 0:` falling through into `case 1:` and clobbering `_resc_loads[j]` with `atoi(resc_name)`).
4. **Client Core Utilities & iCommands (`lib/core`, `clients/icommands`)**:
   - Migrate `rsyncUtil.cpp`, `lsUtil.cpp` (`lsSubfilesInBundle`), `scanUtil.cpp` (`chkObjExist`, `checkIsMount`), `iuserinfo.cpp`, and `iadmin.cpp` (`show_token`, `show_group`, `show_resource`, `show_zone`, `show_user`, `show_user_auth`, `show_user_auth_name`, `show_global_quotas`, `show_resource_quotas`) onto `gq2::builder` ASTs and `gq2::execute` / `gq2::try_execute`.
