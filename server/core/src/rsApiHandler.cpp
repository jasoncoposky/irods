#include "irods/rsApiHandler.hpp"

#include "irods/agent_globals.hpp"
#include "irods/modDataObjMeta.h"
#include "irods/rcMisc.h"
#include "irods/miscServerFunct.hpp"
#include "irods/regReplica.h"
#include "irods/rodsErrorTable.h"
#include "irods/unregDataObj.h"
#include "irods/modAVUMetadata.h"
#include "irods/sockComm.h"
#include "irods/irods_re_structs.hpp"
#include "irods/sslSockComm.h"
#include "irods/irods_client_server_negotiation.hpp"
#include "irods/apiNumber.h"
#include "irods/plugins/api/api_plugin_number.h"
#include "irods/client_api_allowlist.hpp"
#include "irods/key_value_proxy.hpp"
#include "irods/protocol/lite3_structs.hpp"

#include <mutex>

// =-=-=-=-=-=-=-
// irods includes
#include "irods/irods_at_scope_exit.hpp"
#include "irods/irods_network_factory.hpp"
#include "irods/irods_server_api_table.hpp"
#include "irods/irods_threads.hpp"
#include "irods/sockCommNetworkInterface.hpp"
#include "irods/irods_hierarchy_parser.hpp"
#include "irods/irods_api_number_validator.hpp"
#include "irods/irods_logger.hpp"

#define MAKE_IRODS_ERROR_MAP
#include "irods/rodsErrorTable.h"
#undef MAKE_IRODS_ERROR_MAP

#include <cstring>
#include <iterator>
#include <algorithm>

namespace ix = irods::experimental;

// clang-format off
using log_agent  = irods::experimental::log::agent;
using log_server = irods::experimental::log::server;
// clang-format on

namespace
{
    void attach_api_request_info_to_logger(rsComm_t* _comm, int _api_number)
    {
        namespace log = irods::experimental::log;

        log::set_request_client_version(&_comm->cliVersion);
        // NOLINTBEGIN(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        log::set_request_client_hostname(_comm->clientAddr);
        log::set_request_client_username(_comm->clientUser.userName);
        log::set_request_proxy_username(_comm->proxyUser.userName);
        // NOLINTEND(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        log::set_request_api_number(_api_number);
    }
} // anonymous namespace

int rsApiHandler(rsComm_t*   rsComm,
                 int         apiNumber,
                 bytesBuf_t* inputStructBBuf,
                 bytesBuf_t* bsBBuf)
{
    attach_api_request_info_to_logger(rsComm, apiNumber);

    log_agent::trace("Verifying if API number is supported ...");

    if (const auto [supported, ec] = irods::is_api_number_supported(apiNumber); !supported) {
        log_server::error("Unsupported api number [{}]", ec);
        return ec;
    }

    memset(&rsComm->rError, 0, sizeof(rError_t));

    const int apiInx = apiTableLookup( apiNumber );

    // =-=-=-=-=-=-=-
    // create a network object
    irods::network_object_ptr net_obj;
    irods::error ret = irods::network_factory( rsComm, net_obj );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        return apiInx;
    }

    if (apiInx < 0) {
        log_agent::error("rsApiHandler: apiTableLookup of apiNumber {} failed", apiNumber);
        // cannot use sendApiReply because it does not know apiInx
        sendRodsMsg( net_obj, RODS_API_REPLY_T, NULL, NULL, NULL,
                     apiInx, rsComm->irodsProt );
        return apiInx;
    }

    rsComm->apiInx = apiInx;

    // Clear the session properties stored in the connection object.
    // This is required to avoid incorrect behavior when multiple API calls are
    // invoked via the same connection object.
    ix::key_value_proxy{rsComm->session_props}.clear();

    void *myOutStruct = NULL;
    bytesBuf_t myOutBsBBuf;
    memset( &myOutBsBBuf, 0, sizeof( bytesBuf_t ) );

    int status = chkApiVersion( apiInx );
    if ( status < 0 ) {
        sendApiReply( rsComm, apiInx, status, myOutStruct, &myOutBsBBuf );
        return status;
    }

    log_agent::trace("Checking API permissions ...");

    status = chkApiPermission( rsComm, apiInx );
    if ( status < 0 ) {
        log_agent::info("rsApiHandler: User has no permission for apiNumber {}", apiNumber);
        sendApiReply( rsComm, apiInx, status, myOutStruct, &myOutBsBBuf );
        return status;
    }

    irods::api_entry_table& RsApiTable = irods::get_server_api_table();

    /* some sanity check */
    if ( inputStructBBuf->len > 0 && RsApiTable[apiInx]->inPackInstruct == NULL ) {
        log_agent::info("rsApiHandler: input struct error 1 for apiNumber {}", apiNumber);
        sendApiReply( rsComm, apiInx, SYS_API_INPUT_ERR, myOutStruct,
                      &myOutBsBBuf );
        return SYS_API_INPUT_ERR;
    }

    if ( inputStructBBuf->len <= 0 && RsApiTable[apiInx]->inPackInstruct != NULL ) {
        log_agent::info("rsApiHandler: input struct error 2 for apiNumber {}", apiNumber);
        sendApiReply( rsComm, apiInx, SYS_API_INPUT_ERR, myOutStruct,
                      &myOutBsBBuf );
        return SYS_API_INPUT_ERR;
    }

    if ( bsBBuf->len > 0 && RsApiTable[apiInx]->inBsFlag <= 0 ) {
        log_agent::info("rsApiHandler: input byte stream error for apiNumber {}", apiNumber);
        sendApiReply( rsComm, apiInx, SYS_API_INPUT_ERR, myOutStruct,
                      &myOutBsBBuf );
        return SYS_API_INPUT_ERR;
    }

    char *myInStruct = nullptr;
    const auto clear_structs = irods::at_scope_exit{[&RsApiTable, apiInx, &myInStruct, &myOutStruct] {
        if (myInStruct) {
            if (RsApiTable[apiInx] && RsApiTable[apiInx]->clearInStruct) {
                RsApiTable[apiInx]->clearInStruct(myInStruct);
            }

            std::free(myInStruct);
            myInStruct = nullptr;
        }

        if (myOutStruct) {
            if (RsApiTable[apiInx] && RsApiTable[apiInx]->clearOutStruct) {
                RsApiTable[apiInx]->clearOutStruct(myOutStruct);
            }

            std::free(myOutStruct);
            myOutStruct = nullptr;
        }
    }};

    irods::protocol::active_api_buffer_scope active_lite3_scope{inputStructBBuf, rsComm->irodsProt};

    if ( inputStructBBuf->len > 0 ) {
        log_agent::debug("Unpacking byte buffer based on packing instruction [{}]", RsApiTable[apiInx]->inPackInstruct);
        status = unpack_struct( inputStructBBuf->buf, ( void ** )( static_cast< void * >( &myInStruct ) ),
                               ( char* )RsApiTable[apiInx]->inPackInstruct, RodsPackTable, rsComm->irodsProt,
                               rsComm->cliVersion.relVersion);
        if ( status < 0 ) {
            log_agent::info("rsApiHandler: unpackStruct error for apiNumber {}, status = {}",
                              apiNumber, status);
            sendApiReply( rsComm, apiInx, status, myOutStruct, &myOutBsBBuf );
            return status;
        }
    }

    /* ready to call the handler functions */

    irods::api_entry_ptr api_entry = RsApiTable[apiInx];
    if ( !api_entry.get() ) {
        log_agent::error("Null handler encountered for api number {} in rsApiHandler.", apiNumber);
        return SYS_API_INPUT_ERR;
    }

    void *myArgv[4];
    int numArg = 0;

    if ( RsApiTable[apiInx]->inPackInstruct != NULL ) {
        myArgv[numArg] = myInStruct;
        numArg++;
    };

    if ( RsApiTable[apiInx]->inBsFlag != 0 ) {
        myArgv[numArg] = bsBBuf;
        numArg++;
    };

    if ( RsApiTable[apiInx]->outPackInstruct != NULL ) {
        myArgv[numArg] = ( void * ) &myOutStruct;
        numArg++;
    };

    if ( RsApiTable[apiInx]->outBsFlag != 0 ) {
        myArgv[numArg] = ( void * ) &myOutBsBBuf;
        numArg++;
    };

    int retVal = 0;
    if ( numArg == 0 ) {
        retVal = api_entry->call_wrapper(
                     api_entry.get(),
                     rsComm );
    }
    else if ( numArg == 1 ) {
        retVal = api_entry->call_wrapper(
                     api_entry.get(),
                     rsComm,
                     myArgv[0] );
    }
    else if ( numArg == 2 ) {
        retVal = api_entry->call_wrapper(
                     api_entry.get(),
                     rsComm,
                     myArgv[0],
                     myArgv[1] );
    }
    else if ( numArg == 3 ) {
        retVal = api_entry->call_wrapper(
                     api_entry.get(),
                     rsComm,
                     myArgv[0],
                     myArgv[1],
                     myArgv[2] );
    }
    else if ( numArg == 4 ) {
        retVal = api_entry->call_wrapper(
                     api_entry.get(),
                     rsComm,
                     myArgv[0],
                     myArgv[1],
                     myArgv[2],
                     myArgv[3]);
    }

    if (retVal != SYS_NO_HANDLER_REPLY_MSG) {
        status = sendAndProcApiReply(rsComm, apiInx, retVal, myOutStruct, &myOutBsBBuf);
    }

    if (retVal >= 0 && status < 0) {
        return status;
    }

    return retVal;
}

int sendAndProcApiReply(rsComm_t* rsComm, int apiInx, int status, void*& myOutStruct, bytesBuf_t* myOutBsBBuf)
{
    const int retval = sendApiReply(rsComm, apiInx, status, myOutStruct, myOutBsBBuf);

    clearBBuf(myOutBsBBuf);
    freeRErrorContent(&rsComm->rError);

    // Check for portal operation.
    if (rsComm->portalOpr) {
        handlePortalOpr(rsComm);
        clearKeyVal(&rsComm->portalOpr->dataOprInp.condInput);
        std::free(rsComm->portalOpr);
        rsComm->portalOpr = nullptr;
    }

    return retval;
}

int sendApiReply(rsComm_t* rsComm, int apiInx, int retVal, void*& myOutStruct, bytesBuf_t* myOutBsBBuf)
{
    int status = 0;
    bytesBuf_t* outStructBBuf = nullptr;
    bytesBuf_t* myOutStructBBuf;
    bytesBuf_t* rErrorBBuf = nullptr;
    bytesBuf_t* myRErrorBBuf;

    svrChkReconnAtSendStart( rsComm );

    if ( retVal == SYS_HANDLER_DONE_NO_ERROR ) {
        /* not actually an error */
        retVal = 0;
    }

    // Create a network object.
    irods::network_object_ptr net_obj;
    irods::error ret = irods::network_factory( rsComm, net_obj );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        return ret.code();
    }

    irods::api_entry_table& RsApiTable = irods::get_server_api_table();

    if (RsApiTable[apiInx]->outPackInstruct) {
        if (rsComm->irodsProt == LITE3_PROT) {
            if (auto reply_buf = irods::protocol::active_api_buffer_scope::take_active_reply_buffer()) {
                auto encoded = irods::wire::encode_from_lite3(*reply_buf,
                                                              RsApiTable[apiInx]->outPackInstruct,
                                                              rsComm->irodsProt,
                                                              rsComm->cliVersion.relVersion,
                                                              RodsPackTable);
                if (!encoded) {
                    status = encoded.error();
                    log_agent::info("{}: encode_from_lite3 error, status = [{}]", __func__, status);
                    sendRodsMsg(net_obj, RODS_API_REPLY_T, nullptr, nullptr, nullptr, status, rsComm->irodsProt);
                    svrChkReconnAtSendEnd(rsComm);
                    return status;
                }
                outStructBBuf = *encoded;
            }
        }

        if (!outStructBBuf && myOutStruct) {
            status = pack_struct(static_cast<char*>(myOutStruct),
                                 &outStructBBuf,
                                 RsApiTable[apiInx]->outPackInstruct,
                                 RodsPackTable,
                                 0,
                                 rsComm->irodsProt,
                                 rsComm->cliVersion.relVersion);

            if ( status < 0 ) {
                log_agent::info("{}: packStruct error, status = [{}]", __func__, status);
                sendRodsMsg(net_obj, RODS_API_REPLY_T, nullptr, nullptr, nullptr, status, rsComm->irodsProt);
                svrChkReconnAtSendEnd(rsComm);
                return status;
            }
        }

        myOutStructBBuf = outStructBBuf;
    }
    else {
        myOutStructBBuf = nullptr;
    }

    if ( RsApiTable[apiInx]->outBsFlag == 0 ) {
        myOutBsBBuf = nullptr;
    }

    if ( rsComm->rError.len > 0 ) {
        status = pack_struct(&rsComm->rError,
                             &rErrorBBuf,
                             "RError_PI",
                             RodsPackTable,
                             0,
                             rsComm->irodsProt,
                             rsComm->cliVersion.relVersion);

        if ( status < 0 ) {
            log_agent::info("sendApiReply: packStruct error, status=[{}]", status);
            sendRodsMsg(net_obj, RODS_API_REPLY_T, nullptr, nullptr, nullptr, status, rsComm->irodsProt);
            svrChkReconnAtSendEnd( rsComm );
            freeBBuf( outStructBBuf );
            freeBBuf( rErrorBBuf );
            return status;
        }

        myRErrorBBuf = rErrorBBuf;
    }
    else {
        myRErrorBBuf = nullptr;
    }

    ret = sendRodsMsg(net_obj, RODS_API_REPLY_T, myOutStructBBuf, myOutBsBBuf, myRErrorBBuf, retVal, rsComm->irodsProt);

    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );

        if ( rsComm->reconnSock > 0 ) {
            int savedStatus = ret.code();
            std::unique_lock< std::mutex > boost_lock( *rsComm->thread_ctx->lock );
            log_agent::debug("sendApiReply: svrSwitchConnect. client state=[{}], agent state=[{}]",
                             rsComm->clientState,
                             rsComm->agentState);
            const auto ec = svrSwitchConnect(rsComm);
            boost_lock.unlock();
            if (ec > 0) {
                // Should not be here!
                log_agent::info("sendApiReply: Switch connection and retry sendRodsMsg");
                ret = sendRodsMsg(
                    net_obj, RODS_API_REPLY_T, myOutStructBBuf, myOutBsBBuf, myRErrorBBuf, retVal, rsComm->irodsProt);

                if ( ret.code() >= 0 ) {
                    log_agent::info("sendApiReply: retry sendRodsMsg succeeded");
                }
                else {
                    status = savedStatus;
                }
            }
        }
    }

    svrChkReconnAtSendEnd( rsComm );

    freeBBuf( outStructBBuf );
    freeBBuf( rErrorBBuf );

    return status;
}

int
chkApiVersion( int apiInx ) {
    char *cliApiVersion;

    irods::api_entry_table& RsApiTable = irods::get_server_api_table();
    if ( ( cliApiVersion = getenv( SP_API_VERSION ) ) != NULL ) {
        if ( strcmp( cliApiVersion, RsApiTable[apiInx]->apiVersion ) != 0 ) {
            log_agent::error(
                "chkApiVersion:Client's API Version {} does not match Server's {}",
                cliApiVersion, RsApiTable[apiInx]->apiVersion);
            return USER_API_VERSION_MISMATCH;
        }
    }
    return 0;
}

int chkApiPermission(rsComm_t* rsComm, int apiInx)
{
    auto& api_table = irods::get_server_api_table();
    auto api_entry = api_table[apiInx];

    const int clientUserAuth = api_entry->clientUserAuth & 0xfff; // Remove XMSG_SVR_* flags
    if (clientUserAuth > rsComm->clientUser.authInfo.authFlag) {
        return SYS_NO_API_PRIV;
    }

    const int proxyUserAuth = api_entry->proxyUserAuth & 0xfff;
    if (proxyUserAuth > rsComm->proxyUser.authInfo.authFlag) {
        return SYS_NO_API_PRIV;
    }

    namespace allowlist = irods::client_api_allowlist;

    if (allowlist::enforce(*rsComm) && !allowlist::contains(api_entry->apiNumber)) {
        return SYS_NO_API_PRIV;
    }

    return 0;
}

static
int
apply_acPostProcForParallelTransferReceived(rsComm_t *rsComm) {
    if (rsComm == NULL) {
        log_agent::error("apply_acPostProcForParallelTransferReceived: NULL rsComm");
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }
    if (rsComm->portalOpr == NULL) {
        log_agent::error("apply_acPostProcForParallelTransferReceived: NULL rsComm->portalOpr");
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    const int l3_index = rsComm->portalOpr->dataOprInp.destL3descInx;
    if (l3_index < 3 || l3_index >= NUM_FILE_DESC) {
        log_agent::error("apply_acPostProcForParallelTransferReceived: bad l3 descriptor index {}", l3_index);
        return SYS_FILE_DESC_OUT_OF_RANGE;
    }

    const char* resource_hierarchy = FileDesc[l3_index].rescHier;
    if (resource_hierarchy == NULL) {
        log_agent::error("apply_acPostProcForParallelTransferReceived: NULL resource_hierarchy");
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    irods::hierarchy_parser hierarchy_parser;
    irods::error err = hierarchy_parser.set_string(resource_hierarchy);
    if (!err.ok()) {
        log_agent::error("apply_acPostProcForParallelTransferReceived: set_string error [{}]", err.result());
        return err.status();
    }

    std::string leaf_resource;
    err = hierarchy_parser.last_resc(leaf_resource);
    if (!err.ok()) {
        log_agent::error("apply_acPostProcForParallelTransferReceived: last_resc error [{}]", err.result());
        return err.status();
    }

    const char *args[] = {leaf_resource.c_str()};
    ruleExecInfo_t rei;
    memset(&rei, 0, sizeof(rei));
    rei.rsComm = rsComm;
    int ret = applyRuleArg("acPostProcForParallelTransferReceived", args, sizeof(args)/sizeof(args[0]), &rei, NO_SAVE_REI);
    return ret;
}

int
handlePortalOpr( rsComm_t * rsComm ) {
    int oprType;
    int status;

    if ( rsComm == NULL || rsComm->portalOpr == NULL ) {
        return 0;
    }

    oprType = rsComm->portalOpr->oprType;

    switch ( oprType ) {
    case PUT_OPR:
    case GET_OPR:
        status = svrPortalPutGet( rsComm );
        if (status >=0 && oprType == PUT_OPR) {
            apply_acPostProcForParallelTransferReceived(rsComm);
        }
        break;
    default:
        log_agent::info("handlePortalOpr: Invalid portal oprType: {}", oprType);
        status = SYS_INVALID_PORTAL_OPR;
        break;
    }
    return status;
}

int
readAndProcClientMsg( rsComm_t * rsComm, int flags ) {
    int status = 0;
    msgHeader_t myHeader;
    bytesBuf_t inputStructBBuf{}, bsBBuf{}, errorBBuf{};
    const auto clear_bufs = irods::at_scope_exit{[&inputStructBBuf, &bsBBuf, &errorBBuf] {
        clearBBuf(&inputStructBBuf);
        clearBBuf(&bsBBuf);
        clearBBuf(&errorBBuf);
    }};

    svrChkReconnAtReadStart( rsComm );
    /* everything else are set in readMsgBody */

    /* read the header */

    // =-=-=-=-=-=-=-
    // create a network object
    irods::network_object_ptr net_obj;
    irods::error ret = irods::network_factory( rsComm, net_obj );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        return ret.code();
    }

    if ( ( flags & READ_HEADER_TIMEOUT ) != 0 ) {
        int retryCnt = 0;
        struct timeval tv;
        tv.tv_sec = READ_HEADER_TIMEOUT_IN_SEC;
        tv.tv_usec = 0;

        while ( 1 ) {
            ret = readMsgHeader( net_obj, &myHeader, &tv );
            if ( !ret.ok() ) {
                log_agent::debug("{}: readMsgHeader() returned [{}]", __func__, ret.code());

                if (ret.code() == INTERRUPT_DETECTED) {
                    // Check if the agent factory requested for the agent to stop.
                    if (g_terminate) {
                        log_agent::info(
                            "{}: Received instruction to shutdown. Agent is shutting down.", __func__, ret.code());
                        return SHUTDOWN_SEQUENCE_INITIATED;
                    }

                    // If a read() from the socket was interrupted in iRODS 4, the read() operation would
                    // ignore it and restart the operation by continuing the loop. This "continue" instruction
                    // maintains that behavior in iRODS 5. The only difference between the implementations is
                    // that tcp_socket_read() returns when an interrupt occurs, allowing this code block to
                    // react.
                    //
                    // Compare the implementation of tcp_socket_read() in tcp.cpp from iRODS 5 to iRODS 4.
                    continue;
                }

                if ( isL1descInuse() && retryCnt < MAX_READ_HEADER_RETRY ) {
                    log_agent::error(
                        "readAndProcClientMsg:readMsgHeader error. status = {}", ret.code());
                    retryCnt++;
                    continue;
                }

                if ( ret.code() == USER_SOCK_CONNECT_TIMEDOUT ) {
                    log_agent::error(
                        "readAndProcClientMsg: readMsgHeader by pid {} timedout",
                        getpid());
                    return  ret.code();
                }
            }
            break;
        } // while 1
    }
    else {
        ret = readMsgHeader( net_obj, &myHeader, NULL );
        if (!ret.ok() && ret.code() == INTERRUPT_DETECTED) {
            // Check if the agent factory requested for the agent to stop.
            if (g_terminate) {
                log_agent::info("{}: Received instruction to shutdown. Agent is shutting down.", __func__, ret.code());
                return SHUTDOWN_SEQUENCE_INITIATED;
            }
        }
    }

    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        /* attempt to accept reconnect. ENOENT result  from * user cntl-C */
        if ( rsComm->reconnSock > 0 ) {
            int savedStatus = ret.code();
            /* try again. the socket might have changed */
            std::unique_lock< std::mutex > boost_lock( *rsComm->thread_ctx->lock );
            log_agent::debug(
                "readAndProcClientMsg: svrSwitchConnect. cliState = {},agState={}",
                rsComm->clientState, rsComm->agentState);
            svrSwitchConnect( rsComm );
            boost_lock.unlock();
            ret = readMsgHeader( net_obj, &myHeader, NULL );
            if ( !ret.ok() ) {
                svrChkReconnAtReadEnd( rsComm );
                return savedStatus;
            }
        }
        else {
            svrChkReconnAtReadEnd( rsComm );
            return ret.code();
        }
    } // if !ret.ok()

    ret = readMsgBody( net_obj, &myHeader, &inputStructBBuf,
                       &bsBBuf, &errorBBuf, rsComm->irodsProt, NULL );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        svrChkReconnAtReadEnd( rsComm );
        return ret.code();
    }

    svrChkReconnAtReadEnd( rsComm );

    /* handler switch by msg type */

    if ( strcmp( myHeader.type, RODS_API_REQ_T ) == 0 ) {
        status = rsApiHandler(rsComm, myHeader.intInfo, &inputStructBBuf, &bsBBuf);

        if ( ( flags & RET_API_STATUS ) != 0 ) {
            return status;
        }
        else {
            return 0;
        }
    }
    else if ( strcmp( myHeader.type, RODS_DISCONNECT_T ) == 0 ) {
        log_agent::debug("readAndProcClientMsg: received disconnect msg from client");
        return DISCONN_STATUS;
    }
    else if ( strcmp( myHeader.type, RODS_RECONNECT_T ) == 0 ) {
        log_agent::info("readAndProcClientMsg: received reconnect msg from client");
        /* call itself again. be careful */
        status = readAndProcClientMsg( rsComm, flags );
        return status;
    }
    else {
        log_agent::info(
            "agentMain: msg type {} not support by server",
            myHeader.type);
        return USER_MSG_TYPE_NO_SUPPORT;
    }
}

/* sendAndRecvBranchMsg - Break out the normal mode of
 * clientReuest/serverReply protocol for handling API. Instead of returning
 * to rsApiHandler() and send a API reply, it sends a reply directly to
 * the client through sendAndProcApiReply.
 * Then it loops though readAndProcClientMsg() to process additional
 * clients requests until the status is SYS_HANDLER_DONE_NO_ERROR
 * which is generated by a rcOprComplete() call by the client. The client
 * must remember to send a rcOprComplete() call or the server will hang
 * in this loop.
 * The caller of this routine should return a SYS_NO_HANDLER_REPLY_MSG
 * status to rsApiHandler() if the client is not expecting any more
 * reply msg.
 */

int
sendAndRecvBranchMsg( rsComm_t * rsComm, int apiInx, int status,
                      void * myOutStruct, bytesBuf_t * myOutBsBBuf ) {
    int retval;
    int savedApiInx;

    savedApiInx = rsComm->apiInx;
    retval = sendAndProcApiReply( rsComm, apiInx, status,
                                  myOutStruct, myOutBsBBuf );
    if ( retval < 0 ) {
        log_agent::error(
            "sendAndRecvBranchMsg: sendAndProcApiReply error. status = {}", retval);
        rsComm->apiInx = savedApiInx;
        return retval;
    }

    while ( 1 )  {
        retval = readAndProcClientMsg( rsComm, RET_API_STATUS );
        if ( retval >= 0 || retval == SYS_NO_HANDLER_REPLY_MSG ) {
            /* more to come */
            continue;
        }
        else {
            rsComm->apiInx = savedApiInx;
            if ( retval == SYS_HANDLER_DONE_NO_ERROR ) {
                return 0;
            }
            else {
                return retval;
            }
        }
    }
}

int
svrSendCollOprStat( rsComm_t * rsComm, collOprStat_t * collOprStat ) {
    int status;

    status = _svrSendCollOprStat( rsComm, collOprStat );

    if ( status != SYS_CLI_TO_SVR_COLL_STAT_REPLY ) {
        log_agent::error(
            "svrSendCollOprStat: client reply {} != {}.",
            status, SYS_CLI_TO_SVR_COLL_STAT_REPLY);
        return UNMATCHED_KEY_OR_INDEX;
    }
    else {
        return 0;
    }
}

int
_svrSendCollOprStat( rsComm_t * rsComm, collOprStat_t * collOprStat ) {
    int myBuf;
    int status;

    auto* p = static_cast<void*>(collOprStat);
    status = sendAndProcApiReply(rsComm, rsComm->apiInx, SYS_SVR_TO_CLI_COLL_STAT, p, nullptr);
    if ( status < 0 ) {
        log_agent::error(
            "svrSendCollOprStat: sendAndProcApiReply failed. status = {}",
            status);
        return status;
    }

    /* read 4 bytes */
    if (irods::CS_NEG_USE_SSL == rsComm->negotiation_results) {
        status = sslRead(rsComm->sock, static_cast<void*>(&myBuf), 4, NULL, NULL, rsComm->ssl);
    } else {
        status = myRead(rsComm->sock, static_cast<void*>(&myBuf), 4, NULL, NULL );
    }

    if ( status < 0 ) {
        log_agent::error(
            "_svrSendCollOprStat: read handshake failed. [{}] status = {}", rsComm->negotiation_results, status);
    }
    return ntohl( myBuf );
}

int
svrSendZoneCollOprStat( rsComm_t * rsComm, rcComm_t * conn,
                        collOprStat_t * collOprStat, int retval ) {
    int status = retval;

    while ( status == SYS_SVR_TO_CLI_COLL_STAT ) {
        status = _svrSendCollOprStat( rsComm, collOprStat );
        if ( status == SYS_CLI_TO_SVR_COLL_STAT_REPLY ) {
            status = _cliGetCollOprStat( conn, &collOprStat );
        }
        else {
            int myBuf = htonl( status );
            if (irods::CS_NEG_USE_SSL == conn->negotiation_results) {
                sslWrite(static_cast<void*>(&myBuf), 4, NULL, conn->ssl);
            } else {
                myWrite(conn->sock, static_cast<void*>(&myBuf), 4, NULL );
            }
            break;
        }
    }
    return status;
}
