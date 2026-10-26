#include "irods/agent_session_sanitizer.hpp"

#include "irods/fileOpr.hpp"
#include "irods/getRescQuota.h"
#include "irods/icatHighLevelRoutines.hpp"
#include "irods/initServer.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_logger.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/miscServerFunct.hpp"
#include "irods/objDesc.hpp"
#include "irods/plugin_lifetime_manager.hpp"
#include "irods/replica_access_table.hpp"
#include "irods/replica_state_table.hpp"
#include "irods/rodsKeyWdDef.h"
#include "irods/rsGlobalExtern.hpp"
#include "irods/rsIcatOpr.hpp"

#include <unistd.h>

namespace irods::server
{
    namespace
    {
        using log_agent = irods::experimental::log::agent;
    } // anonymous namespace

    auto sanitize_agent_session(rsComm_t& _comm, bool _keep_db_open) noexcept -> int
    {
        int rollback_ec = 0;

        try {
            // 1. Close any leaked or unclosed L1 descriptors.
            if (INITIAL_DONE == InitialState) {
                close_all_l1_descriptors(_comm);
            }
            for (int fd = 3; fd < NUM_L1_DESC; ++fd) {
                if (L1desc[fd].inuseFlag != FD_FREE) {
                    freeL1desc(fd);
                }
            }

            // 2. Close any open collection handles.
            for (std::size_t idx = 0; idx < CollHandle.size(); ++idx) {
                if (CollHandle[idx].inuseFlag != FD_FREE) {
                    freeCollHandle(static_cast<int>(idx));
                }
            }
            CollHandle.clear();

            // 3. Free any open special collection descriptors.
            for (int idx = 1; idx < NUM_SPEC_COLL_DESC; ++idx) {
                if (SpecCollDesc[idx].inuseFlag != FD_FREE) {
                    freeSpecCollDesc(idx);
                }
            }

            // 4. Clean up replica access/state tables and server-to-server redirects.
            if (INITIAL_DONE == InitialState) {
                try {
                    irods::experimental::replica_access_table::erase_pid(getpid());
                }
                catch (...) {
                }
                irods::replica_state_table::deinit();
                irods::replica_state_table::init();
                disconnectAllSvrToSvrConn();
            }

            // 5. Roll back any uncommitted database transaction while keeping the catalog connection warm.
            std::string svc_role;
            if (get_catalog_service_role(svc_role).ok() && irods::KW_CFG_SERVICE_ROLE_PROVIDER == svc_role) {
                if (INITIAL_DONE == IcatConnState) {
                    rollback_ec = chlRollback(&_comm);
                    if (rollback_ec < 0) {
                        log_agent::warn("{}: chlRollback returned [{}]", __func__, rollback_ec);
                    }
                    if (!_keep_db_open) {
                        disconnectRcat();
                    }
                }
            }

            // 6. Reset global quota state for the next user session.
            GlobalQuotaLimit = RESC_QUOTA_UNINIT;
            GlobalQuotaOverrun = 0;
            RescQuotaPolicy = RESC_QUOTA_UNINIT;

            // 7. Clear session-scoped server_properties (server-to-server negotiation & rule engine user globals).
            for (const auto& prop : {irods::AGENT_CONN_KW,
                                     irods::AGENT_KEY_KW,
                                     irods::CLIENT_USER_NAME_KW,
                                     irods::CLIENT_USER_ZONE_KW,
                                     irods::CLIENT_USER_PRIV_KW,
                                     irods::PROXY_USER_NAME_KW,
                                     irods::PROXY_USER_ZONE_KW,
                                     irods::PROXY_USER_PRIV_KW})
            {
                try {
                    irods::delete_server_property(prop);
                }
                catch (...) {
                }
            }
        }
        catch (const std::exception& e) {
            log_agent::error("{}: Exception during session sanitization: {}", __func__, e.what());
        }
        catch (...) {
            log_agent::error("{}: Unknown exception during session sanitization.", __func__);
        }

        // 8. Scrub rsComm_t session identity, error stack, auth_scheme, session_props, and encryption secrets.
        if (ThisComm == &_comm) {
            ThisComm = nullptr;
        }
        reset_rs_comm_session(_comm);

        return rollback_ec;
    }

} // namespace irods::server
