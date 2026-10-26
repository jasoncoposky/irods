#include "irods/rcMisc.h"
#include "irods/rsGlobalExtern.hpp"
#include "irods/objInfo.h"
#include "irods/protocol/lite3_structs.hpp"
#include "irods/rsDataObjOpen.hpp"

int rsDataObjCreate(rsComm_t *rsComm, dataObjInp_t *dataObjInp) {
    if (auto req_inp = irods::protocol::active_api_buffer_scope::try_get_active_request<irods::protocol::DataObjInp>(dataObjInp)) {
        req_inp->open_flags(req_inp->open_flags() | (O_CREAT | O_RDWR | O_TRUNC));
    }
    dataObjInp->openFlags |= (O_CREAT | O_RDWR | O_TRUNC);
    return rsDataObjOpen(rsComm, dataObjInp);
} // rsDataObjCreate

