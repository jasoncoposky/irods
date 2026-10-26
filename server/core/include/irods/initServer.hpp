#ifndef IRODS_INIT_SERVER_HPP
#define IRODS_INIT_SERVER_HPP

#include "irods/rodsConnect.h"

#include <vector>
#include <string>

int initServerInfo(int processType, rsComm_t* rsComm);
int initLocalServerHost();
int initRcatServerHostByFile();
int initZone(rsComm_t* rsComm);
int initAgent(int processType, rsComm_t* rsComm);

int initHostConfigByFile();
int initRsComm(rsComm_t* rsComm);
int initRsCommWithStartupPack(rsComm_t& rsComm, const startupPack_t& startupPack, bool& require_cs_neg);
int chkAllowedUser(const char* userName, const char* rodsZone);
void close_all_l1_descriptors(RsComm& _comm);
void cleanup_rs_comm_members(rsComm_t& _comm) noexcept;
void cleanupRsComm(rsComm_t& _comm) noexcept;
void reset_rs_comm_session(rsComm_t& _comm) noexcept;

namespace irods {

class server_comm_guard {
public:
    explicit server_comm_guard(rsComm_t& _comm) noexcept
        : comm_{&_comm}
    {
    }

    ~server_comm_guard() noexcept
    {
        if (comm_) {
            cleanup_rs_comm_members(*comm_);
        }
    }

    server_comm_guard(const server_comm_guard&) = delete;
    auto operator=(const server_comm_guard&) -> server_comm_guard& = delete;

    void release() noexcept
    {
        comm_ = nullptr;
    }

private:
    rsComm_t* comm_;
};

} // namespace irods

#endif // IRODS_INIT_SERVER_HPP
