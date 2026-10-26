#ifndef IRODS_AGENT_SESSION_SANITIZER_HPP
#define IRODS_AGENT_SESSION_SANITIZER_HPP

/// \file

#include "irods/rodsConnect.h"

namespace irods::server
{
    /// \brief Sanitizes a warm `irodsAgent` worker's state after a client disconnects so it can safely
    ///        serve another sequential client session without leaking descriptors, locks, or credentials.
    ///
    /// Specifically:
    /// 1. Closes any open or leaked `L1desc` entries (`close_all_l1_descriptors` + `freeL1desc`).
    /// 2. Closes any open collection slots in `CollHandle` (`freeCollHandle`).
    /// 3. Frees any open special collection descriptors in `SpecCollDesc` (`freeSpecCollDesc`).
    /// 4. Erases the agent PID from `replica_access_table` and reinitializes `replica_state_table`.
    /// 5. Rolls back any uncommitted catalog database transaction (`chlRollback`) while keeping the
    ///    underlying `database_session` and prepared statement cache warm when `_keep_db_open` is true.
    /// 6. Scrubs `rsComm_t` per-session fields (`clientUser`, `proxyUser`, `rError`, `auth_scheme`,
    ///    `session_props`, `negotiation_results`, and encryption keys) via `reset_rs_comm_session`.
    ///
    /// \param[in,out] _comm         The server communication handle to sanitize.
    /// \param[in]     _keep_db_open Whether to keep the catalog connection open (true for warm pool workers).
    /// \return 0 on success, or a negative iRODS error code if transaction rollback failed.
    auto sanitize_agent_session(rsComm_t& _comm, bool _keep_db_open = true) noexcept -> int;

} // namespace irods::server

#endif // IRODS_AGENT_SESSION_SANITIZER_HPP
