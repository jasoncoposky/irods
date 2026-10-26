#include <catch2/catch_all.hpp>

#include "irods/agent_session_sanitizer.hpp"
#include "irods/agent_worker_pool.hpp"
#include "irods/getRescQuota.h"
#include "irods/initServer.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/objDesc.hpp"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/rsApiHandler.hpp"
#include "irods/rsGlobalExtern.hpp"
#include "irods/sockComm.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>
#include <string_view>

TEST_CASE("SCM_RIGHTS session file descriptor passing over socketpair")
{
    int unix_sv[2]{-1, -1};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, unix_sv) == 0);
    const auto close_unix_sv = irods::at_scope_exit{[&unix_sv] {
        if (unix_sv[0] >= 0) {
            ::close(unix_sv[0]);
        }
        if (unix_sv[1] >= 0) {
            ::close(unix_sv[1]);
        }
    }};

    int client_sv[2]{-1, -1};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv) == 0);
    const auto close_client_sv = irods::at_scope_exit{[&client_sv] {
        if (client_sv[0] >= 0) {
            ::close(client_sv[0]);
        }
        if (client_sv[1] >= 0) {
            ::close(client_sv[1]);
        }
    }};

    const std::time_t now = 1761400000;
    REQUIRE(irods::server::send_session_fd(unix_sv[0], client_sv[1], now) == 0);

    int received_fd = -1;
    std::time_t received_ts = 0;
    REQUIRE(irods::server::recv_session_fd(unix_sv[1], received_fd, received_ts) == 0);
    const auto close_received_fd = irods::at_scope_exit{[&received_fd] {
        if (received_fd >= 0) {
            ::close(received_fd);
        }
    }};

    REQUIRE(received_fd >= 0);
    CHECK(received_ts == now);

    // Verify bidirectional I/O between client_sv[0] and received_fd
    constexpr std::string_view msg = "hello_irods6_pool";
    REQUIRE(::write(client_sv[0], msg.data(), msg.size()) == static_cast<ssize_t>(msg.size()));

    char buf[64]{};
    const ssize_t nread = ::read(received_fd, buf, sizeof(buf));
    REQUIRE(nread == static_cast<ssize_t>(msg.size()));
    CHECK(std::string_view{buf, static_cast<std::size_t>(nread)} == msg);

    // Shutdown sentinel (client_fd = -1) returns DISCONN_STATUS
    REQUIRE(irods::server::send_session_fd(unix_sv[0], -1, 0) == 0);
    int sentinel_fd = 99;
    std::time_t sentinel_ts = 99;
    CHECK(irods::server::recv_session_fd(unix_sv[1], sentinel_fd, sentinel_ts) == DISCONN_STATUS);
    CHECK(sentinel_fd == -1);
}

TEST_CASE("sanitize_agent_session scrubs L1desc, CollHandle, SpecCollDesc, and rsComm_t across sessions")
{
    initL1desc();
    initSpecCollDesc();
    CollHandle.clear();

    rsComm_t comm{};
    irods::server_comm_guard guard{comm};

    rstrcpy(comm.myEnv.rodsZone, "tempZone", NAME_LEN);
    rstrcpy(comm.clientUser.userName, "alice", NAME_LEN);
    rstrcpy(comm.clientUser.rodsZone, "tempZone", NAME_LEN);
    comm.clientUser.authInfo.authFlag = LOCAL_PRIV_USER_AUTH;
    rstrcpy(comm.proxyUser.userName, "alice", NAME_LEN);
    rstrcpy(comm.proxyUser.rodsZone, "tempZone", NAME_LEN);
    comm.auth_scheme = strdup("pam_password");
    addKeyVal(&comm.session_props, "agent_session_key", "secret_val");
    addRErrorMsg(&comm.rError, SYS_INTERNAL_ERR, "stale error from session 1");

    // Simulate leaked L1 descriptor, collection handle, and specColl descriptor
    const int l1_idx = allocL1desc();
    REQUIRE(l1_idx >= 3);
    CHECK(L1desc[l1_idx].inuseFlag == FD_INUSE);

    const int coll_idx = allocCollHandle();
    REQUIRE(coll_idx >= 0);
    CHECK(CollHandle[static_cast<std::size_t>(coll_idx)].inuseFlag == FD_INUSE);

    const int sc_idx = allocSpecCollDesc();
    REQUIRE(sc_idx >= 1);
    CHECK(SpecCollDesc[sc_idx].inuseFlag == FD_INUSE);

    GlobalQuotaLimit = 1024;
    GlobalQuotaOverrun = 512;

    // Sanitize session for next client
    CHECK(irods::server::sanitize_agent_session(comm, /*_keep_db_open=*/false) == 0);

    // Verify all leaked descriptors were reclaimed
    CHECK(L1desc[l1_idx].inuseFlag == FD_FREE);
    CHECK(CollHandle.empty());
    CHECK(SpecCollDesc[sc_idx].inuseFlag == FD_FREE);
    CHECK(GlobalQuotaLimit == RESC_QUOTA_UNINIT);
    CHECK(GlobalQuotaOverrun == 0);

    // Verify rsComm_t session identity & secrets were scrubbed while preserving myEnv
    CHECK(std::string_view{comm.myEnv.rodsZone} == "tempZone");
    CHECK(comm.clientUser.userName[0] == '\0');
    CHECK(comm.clientUser.authInfo.authFlag == 0);
    CHECK(comm.proxyUser.userName[0] == '\0');
    CHECK(comm.auth_scheme == nullptr);
    CHECK(comm.session_props.len == 0);
    CHECK(comm.rError.len == 0);
    CHECK(comm.thread_ctx != nullptr);
}

TEST_CASE("agent_worker_pool multi-session reuse and automatic worker recycling")
{
    irods::server::agent_worker_pool::config cfg{};
    cfg.pool_size = 1;
    cfg.max_sessions_per_agent = 2;

    int session_seq = 0;
    irods::server::agent_worker_pool pool{
        cfg,
        [&session_seq](int _client_fd, std::time_t /*_created_at*/, bool _is_last_session) -> int {
            ++session_seq;
            const std::string reply =
                std::to_string(getpid()) + ":" + std::to_string(session_seq) + ":" + (_is_last_session ? "last" : "reuse");
            (void) ::write(_client_fd, reply.data(), reply.size());
            ::close(_client_fd);
            return 0;
        }};

    REQUIRE(pool.start() == 0);
    CHECK(pool.active_worker_count() == 1);

    auto run_client_exchange = [&](std::time_t _ts) -> std::string {
        int sv[2]{-1, -1};
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
        REQUIRE(pool.dispatch_client(sv[1], _ts) == 0);
        ::close(sv[1]);

        char buf[128]{};
        const ssize_t n = ::read(sv[0], buf, sizeof(buf));
        ::close(sv[0]);
        REQUIRE(n > 0);
        return std::string{buf, static_cast<std::size_t>(n)};
    };

    // Session 1 and Session 2 should be served by the exact same pre-forked worker PID
    const std::string resp1 = run_client_exchange(1001);
    const std::string resp2 = run_client_exchange(1002);

    const auto colon1 = resp1.find(':');
    const auto colon2 = resp2.find(':');
    REQUIRE(colon1 != std::string::npos);
    REQUIRE(colon2 != std::string::npos);

    const std::string pid1 = resp1.substr(0, colon1);
    const std::string pid2 = resp2.substr(0, colon2);
    CHECK(pid1 == pid2);
    CHECK(resp1.ends_with(":1:reuse"));
    CHECK(resp2.ends_with(":2:last"));

    // Session 3 exceeds max_sessions_per_agent (2), so the pool recycles the worker and spawns a fresh PID
    const std::string resp3 = run_client_exchange(1003);
    const auto colon3 = resp3.find(':');
    REQUIRE(colon3 != std::string::npos);
    const std::string pid3 = resp3.substr(0, colon3);
    CHECK(pid3 != pid1);
    CHECK(resp3.ends_with(":1:reuse"));

    pool.stop();
    CHECK(pool.active_worker_count() == 0);
}
