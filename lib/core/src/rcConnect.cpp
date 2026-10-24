#include "irods/rcConnect.h"
#include "irods/rcGlobalExtern.h"
#include "irods/rcMisc.h"

#include <boost/asio.hpp>

#include "irods/irods_network_factory.hpp"

#include "irods/sockComm.h"
#include "irods/irods_threads.hpp"
#include "irods/sockCommNetworkInterface.hpp"
#include "irods/rodsError.h"

#include <charconv>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <cstring>
#include <mutex>
#include <system_error>
#include <thread>

rcComm_t *
rcConnect( const char *rodsHost, int rodsPort, const char *userName, const char *rodsZone,
           int reconnFlag, rErrMsg_t *errMsg ) {
    rcComm_t *conn;

#ifdef windows_platform
    if ( 0 != startWinsock() ) {
        conn = NULL;
        /*error -34*/
        return conn;
    }
#endif

#ifndef windows_platform
    if ( reconnFlag != RECONN_TIMEOUT && getenv( RECONNECT_ENV ) != NULL ) {
        reconnFlag = RECONN_TIMEOUT;
    }
#endif

    conn = _rcConnect( rodsHost, rodsPort, userName, rodsZone, NULL, NULL,
                       errMsg, 0, reconnFlag );

    return conn;
}

rcComm_t* _rcConnect(
    const char *rodsHost,
    int         rodsPort,
    const char *proxyUserName,
    const char *proxyRodsZone,
    const char *clientUserName,
    const char *clientRodsZone,
    rErrMsg_t *errMsg,
    int connectCnt,
    int reconnFlag ) {
    rcComm_t *conn;
    int status;
    char *tmpStr;

    conn = ( rcComm_t* )malloc( sizeof( rcComm_t ) );
    memset( conn, 0, sizeof( rcComm_t ) );

    conn->thread_ctx = ( thread_context* ) malloc( sizeof( thread_context ) );
    memset( conn->thread_ctx, 0, sizeof( thread_context ) );

    allocate_error_stack_if_necessary(&conn->rError);

    if ( errMsg != NULL ) {
        memset( errMsg, 0, sizeof( rErrMsg_t ) );
    }

    if ( ( tmpStr = getenv( IRODS_PROT ) ) != NULL ) {
        int prot{};
        std::from_chars(tmpStr, tmpStr + std::strlen(tmpStr), prot);
        conn->irodsProt = static_cast<irodsProt_t>(prot);

        if (conn->irodsProt != NATIVE_PROT && conn->irodsProt != XML_PROT) {
            rodsLog(LOG_ERROR, "Invalid protocol value.");
            return nullptr;
        }
    }
    else {
        conn->irodsProt = NATIVE_PROT;
    }

    status = setUserInfo( proxyUserName, proxyRodsZone,
                          clientUserName, clientRodsZone, &conn->clientUser, &conn->proxyUser );

    if ( status < 0 ) {
        if ( errMsg != NULL ) {
            errMsg->status = status;
            snprintf( errMsg->msg, ERR_MSG_LEN - 1,
                      "_rcConnect: setUserInfo failed\n" );
        }
        if ( conn->thread_ctx != NULL ) {
            if ( conn->thread_ctx->reconnThr && conn->thread_ctx->reconnThr->joinable() ) {
                conn->thread_ctx->reconnThr->detach();
            }
            delete  conn->thread_ctx->reconnThr;
            delete  conn->thread_ctx->lock;
            delete  conn->thread_ctx->cond;
            free( conn->thread_ctx );
        }
        freeRError(conn->rError);
        free( conn );
        return NULL;
    }

    status = setRhostInfo( conn, rodsHost, rodsPort );

    if ( status < 0 ) {
        if ( errMsg != NULL ) {
            rodsLogError( LOG_ERROR, status,
                          "_rcConnect: setRhostInfo error, IRODS_HOST is probably not set correctly" );
            errMsg->status = status;
            snprintf( errMsg->msg, ERR_MSG_LEN - 1,
                      "_rcConnect: setRhostInfo failed\n" );
        }
        if ( conn->thread_ctx != NULL ) {
            if ( conn->thread_ctx->reconnThr && conn->thread_ctx->reconnThr->joinable() ) {
                conn->thread_ctx->reconnThr->detach();
            }
            delete  conn->thread_ctx->reconnThr;
            delete  conn->thread_ctx->lock;
            delete  conn->thread_ctx->cond;
            free( conn->thread_ctx );
        }
        freeRError(conn->rError);
        free( conn );
        return NULL;
    }
    status = connectToRhost( conn, connectCnt, reconnFlag );

    if ( status < 0 ) {
        if ( getIrodsErrno( status ) == SYS_SOCK_READ_TIMEDOUT ) {
            /* timed out. try again */
            rodsLog( LOG_ERROR,
                     "_rcConnect: connectToRhost timed out, retrying" );
            status = connectToRhost( conn, connectCnt, reconnFlag );
        }
    }

    if ( status < 0 ) {
        rodsLogError( LOG_ERROR, status,
                      "_rcConnect: connectToRhost error, server on %s:%d is probably down",
                      conn->host, conn->portNum );
        if ( errMsg != NULL ) {
            errMsg->status = status;
            snprintf( errMsg->msg, ERR_MSG_LEN - 1,
                      "_rcConnect: connectToRhost failed\n" );
        }
        if ( conn->thread_ctx != NULL ) {
            if ( conn->thread_ctx->reconnThr && conn->thread_ctx->reconnThr->joinable() ) {
                conn->thread_ctx->reconnThr->detach();
            }
            delete  conn->thread_ctx->reconnThr;
            delete  conn->thread_ctx->lock;
            delete  conn->thread_ctx->cond;
            free( conn->thread_ctx );
        }
        freeRError(conn->rError);
        free( conn );
        return NULL;
    }

#ifndef windows_platform
    if ( reconnFlag == RECONN_TIMEOUT &&
            conn->svrVersion != NULL &&
            conn->svrVersion->reconnPort > 0 ) {
        if ( isLoopbackAddress( conn->svrVersion->reconnAddr ) ||
                strcmp( conn->svrVersion->reconnAddr , "0.0.0.0" ) == 0 ||
                strcmp( conn->svrVersion->reconnAddr , "localhost" ) ) {
            /* localhost. just use conn->host */
            rstrcpy( conn->svrVersion->reconnAddr, conn->host, NAME_LEN );
        }
        conn->exit_flg = false;
        try {
            conn->thread_ctx->lock      = new std::mutex;
            conn->thread_ctx->cond      = new std::condition_variable;
            conn->thread_ctx->reconnThr = new std::thread( cliReconnManager, conn );
        }
        catch ( const std::system_error& ) {
            rodsLog( LOG_ERROR, "failure initializing the thread context in _rcConnect" );
            return NULL;
        }
    }
#endif

    return conn;
}


int
setUserInfo(
    const char *proxyUserName, const char *proxyRodsZone,
    const char *clientUserName, const char *clientRodsZone,
    userInfo_t *clientUser, userInfo_t *proxyUser ) {

    rstrcpy( proxyUser->userName, proxyUserName, NAME_LEN );
    if ( clientUserName != NULL ) {
        rstrcpy( clientUser->userName, clientUserName, NAME_LEN );
    }
    else if ( char * myUserName = getenv( CLIENT_USER_NAME_KEYWD ) ) {
        rstrcpy( clientUser->userName, myUserName, NAME_LEN );
    }
    else {
        rstrcpy( clientUser->userName, proxyUserName, NAME_LEN );
    }

    rstrcpy( proxyUser->rodsZone, proxyRodsZone, NAME_LEN );
    if ( clientRodsZone != NULL ) {
        rstrcpy( clientUser->rodsZone, clientRodsZone, NAME_LEN );
    }
    else if ( char * myRodsZone = getenv( CLIENT_RODS_ZONE_KEYWD ) ) {
        rstrcpy( clientUser->rodsZone, myRodsZone, NAME_LEN );
    }
    else {
        rstrcpy( clientUser->rodsZone, proxyRodsZone, NAME_LEN );
    }

    return 0;
}

int
setRhostInfo( rcComm_t *conn, const char *rodsHost, int rodsPort ) {
    int status;

    if ( rodsHost == NULL || strlen( rodsHost ) == 0 ) {
        return USER_RODS_HOST_EMPTY;
    }

    rstrcpy( conn->host, rodsHost, NAME_LEN );
    conn->portNum = rodsPort;

    status = setSockAddr( &conn->remoteAddr, rodsHost, rodsPort );

    return status;
}

int
setSockAddr( struct sockaddr_in *remoteAddr, const char *rodsHost, int rodsPort ) {
    const int status = load_in_addr_from_hostname(rodsHost, &remoteAddr->sin_addr);
    if ( status != 0 ) {
        return status;
    }

    remoteAddr->sin_family = AF_INET;
    remoteAddr->sin_port = htons( ( unsigned short )( rodsPort ) );
    return 0;
}

// =-=-=-=-=-=-=-
// request shut down client-server connection
int rcDisconnect(
    rcComm_t* _conn ) {
    // =-=-=-=-=-=-=-
    // check for invalid param
    if ( _conn == NULL ) {
        return 0;
    }

    // =-=-=-=-=-=-=-
    // create network object to pass to plugin interface
    irods::network_object_ptr net_obj;
    irods::error ret = irods::network_factory( _conn, net_obj );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        return ret.code();
    }

    // =-=-=-=-=-=-=-
    // send disconnect msg to agent
    ret = sendRodsMsg(
              net_obj,
              RODS_DISCONNECT_T,
              NULL, NULL, NULL, 0,
              _conn->irodsProt );
#ifdef RODS_CLERVER
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
    }
#endif

    // =-=-=-=-=-=-=-
    // get rods env to pass to client start for policy decisions
    rodsEnv rods_env;
    int status = getRodsEnv( &rods_env );

    // =-=-=-=-=-=-=-
    // shut down any network plugin activity
    ret = sockClientStop( net_obj, &rods_env );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
    }

    net_obj->to_client( _conn );

    close( _conn->sock );


    if (_conn->thread_ctx) {
        if (_conn->thread_ctx->lock && _conn->thread_ctx->cond) {
            std::lock_guard<std::mutex> lock{*_conn->thread_ctx->lock};
            _conn->exit_flg = true;
            _conn->thread_ctx->cond->notify_all();
        }
        else {
            _conn->exit_flg = true;
        }
        if (_conn->thread_ctx->reconnThr && _conn->thread_ctx->reconnThr->joinable()) {
            try {
                _conn->thread_ctx->reconnThr->join();
            }
            catch (const std::system_error&) {
                rodsLog(LOG_ERROR, "System error joining reconnect thread.");
                status = SYS_INTERNAL_ERR;
            }
        }
    }
    else {
        _conn->exit_flg = true;
    }

    status = freeRcComm( _conn );

    return status;

} // rcDisconnect

int
freeRcComm( rcComm_t *conn ) {
    int status;

    if ( conn == NULL ) {
        return 0;
    }

    status = cleanRcComm( conn );
    if (status != 0) {
        free(conn);
    }

    return 0;
}

int
cleanRcComm( rcComm_t *conn ) {
    int ret = 0;

    if ( conn == NULL ) {
        return ret;
    }

    if (conn->rError != nullptr) {
        freeRError(conn->rError);
        conn->rError = nullptr;
        ret = 1;
    }

    if ( conn->svrVersion != NULL ) {
        free( conn->svrVersion );
        conn->svrVersion = NULL;
        ret = 1;
    }
    if ( conn->thread_ctx != NULL ) {
        if ( conn->thread_ctx->reconnThr && conn->thread_ctx->reconnThr->joinable() ) {
            conn->thread_ctx->reconnThr->detach();
        }
        delete  conn->thread_ctx->reconnThr;
        delete  conn->thread_ctx->lock;
        delete  conn->thread_ctx->cond;
        free(conn->thread_ctx);
        conn->thread_ctx = nullptr;
        ret = 1;
    }

    return ret;
}

void
cliReconnManager( rcComm_t *conn ) {
    struct sockaddr_in remoteAddr;
    reconnMsg_t reconnMsg;
    reconnMsg_t *reconnMsgOut = NULL;
    if ( conn == NULL || conn->svrVersion == NULL ||
            conn->svrVersion->reconnPort <= 0 ) {
        return;
    }

    conn->reconnTime = time( 0 ) + RECONN_TIMEOUT_TIME;

    while ( !conn->exit_flg ) { /* JMC */
        std::unique_lock<std::mutex> boost_lock;
        try {
            boost_lock = std::unique_lock<std::mutex>( *conn->thread_ctx->lock );
        }
        catch ( const std::system_error& ) {
            rodsLog( LOG_ERROR, "lock_error on unique_lock" );
            return;
        }

        time_t curTime = time( 0 );

        if ( curTime < conn->reconnTime ) {
            conn->thread_ctx->cond->wait_for(
                boost_lock,
                std::chrono::seconds( conn->reconnTime - curTime ),
                [conn] { return conn->exit_flg != 0; } );
        }
        if ( conn->exit_flg ) {
            break;
        }
        /* need to check clientState */
        while ( !conn->exit_flg && conn->clientState != PROCESSING_STATE ) {
            /* have to wait until the client stop sending */
            conn->reconnThrState = CONN_WAIT_STATE;
            rodsLog( LOG_DEBUG,
                     "cliReconnManager: clientState = %d", conn->clientState );
            printf( "cliReconnManager: clientState = %d\n", conn->clientState );
            fflush( stdout );
            conn->thread_ctx->cond->wait( boost_lock );

        }
        if ( conn->exit_flg ) {
            break;
        }
        rodsLog( LOG_DEBUG,
                 "cliReconnManager: Reconnecting clientState = %d",
                 conn->clientState );
        printf( "cliReconnManager: Reconnecting clientState = %d\n",
                conn->clientState );
        fflush( stdout );

        conn->reconnThrState = PROCESSING_STATE;
        /* connect to server's reconn thread */

        const int status = load_in_addr_from_hostname(conn->svrVersion->reconnAddr, &remoteAddr.sin_addr);
        if (status != 0) {
            rodsLog(LOG_ERROR, "cliReconnManager: unknown hostname: %s",
                     conn->svrVersion->reconnAddr );
            return;
        }

        remoteAddr.sin_family = AF_INET;
        remoteAddr.sin_port = htons((unsigned short)conn->svrVersion->reconnPort);

        conn->reconnectedSock =
            connectToRhostWithRaddr( &remoteAddr, conn->windowSize, 0 );

        if ( conn->reconnectedSock < 0 ) {
            conn->thread_ctx->cond->notify_all();
            boost_lock.unlock();
            rodsLog( LOG_ERROR,
                     "cliReconnManager: connect to host %s failed, status = %d",
                     conn->svrVersion->reconnAddr, conn->reconnectedSock );
            printf( "cliReconnManager: connect to host %s failed, status = %d\n",
                    conn->svrVersion->reconnAddr, conn->reconnectedSock );
            fflush( stdout );
            rodsSleep( RECONNECT_SLEEP_TIME, 0 );
            continue;
        }

        std::memset(&reconnMsg, 0, sizeof(reconnMsg_t));
        reconnMsg.procState = conn->clientState;
        reconnMsg.cookie    = conn->svrVersion->cookie;

        // =-=-=-=-=-=-=-
        // create network object, need to override the socket
        // with the reconn socket.  no way to infer this in the
        // factory for the client comm
        irods::network_object_ptr net_obj;
        irods::error ret = irods::network_factory( conn, net_obj );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
        }

        net_obj->socket_handle( conn->reconnectedSock ); // repave w/ recon socket
        ret = sendReconnMsg( net_obj, &reconnMsg );
        if ( !ret.ok() ) {
            close( conn->reconnectedSock );
            conn->reconnectedSock = 0;
            conn->thread_ctx->cond->notify_all();
            boost_lock.unlock();
            rodsLog( LOG_ERROR,
                     "cliReconnManager: sendReconnMsg to host %s failed, status = %d",
                     conn->svrVersion->reconnAddr, ret.code() );
            rodsSleep( RECONNECT_SLEEP_TIME, 0 );
            continue;
        }

        ret = readReconMsg( net_obj, &reconnMsgOut );
        if ( !ret.ok() ) {
            close( conn->reconnectedSock );
            conn->reconnectedSock = 0;
            conn->thread_ctx->cond->notify_all();
            boost_lock.unlock();
            rodsLog( LOG_ERROR,
                     "cliReconnManager: readReconMsg to host %s failed, status = %d",
                     conn->svrVersion->reconnAddr, ret.code() );
            rodsSleep( RECONNECT_SLEEP_TIME, 0 );
            continue;
        }

        conn->agentState = reconnMsgOut->procState;
        free( reconnMsgOut );
        reconnMsgOut = NULL;
        conn->reconnTime = time( 0 ) + RECONN_TIMEOUT_TIME;
        if ( conn->clientState == PROCESSING_STATE ) {
            rodsLog( LOG_DEBUG,
                     "cliReconnManager: svrSwitchConnect. cliState = %d,agState=%d",
                     conn->clientState, conn->agentState );
            cliSwitchConnect( conn );
        }
        else {
            rodsLog( LOG_DEBUG,
                     "cliReconnManager: Not calling svrSwitchConnect,  clientState = %d",
                     conn->clientState );
        }
        conn->thread_ctx->cond->notify_all();
        boost_lock.unlock();
    }
}

int
cliChkReconnAtSendStart( rcComm_t *conn ) {
    if( !conn ) {
        printf( "cliChkReconnAtSendStart - null conn\n" );
        return SYS_INVALID_INPUT_PARAM;
    }
    if ( conn->svrVersion != NULL && conn->svrVersion->reconnPort > 0 ) {
        /* handle reconn */
        std::unique_lock<std::mutex> boost_lock;
        try {
            boost_lock = std::unique_lock<std::mutex>( *conn->thread_ctx->lock );
        }
        catch ( const std::system_error& ) {
            rodsLog( LOG_ERROR, "lock_error on unique_lock" );
            return SYS_INTERNAL_ERR;
        }
        if ( conn->reconnThrState == CONN_WAIT_STATE ) {
            rodsLog( LOG_DEBUG,
                     "cliChkReconnAtSendStart:ThrState=CONN_WAIT_STATE,clientState=%d",
                     conn->clientState );
            conn->clientState = PROCESSING_STATE;

            conn->thread_ctx->cond->notify_all();
            /* wait for reconnManager to get done */
            conn->thread_ctx->cond->wait( boost_lock );

        }
        conn->clientState = SENDING_STATE;
        boost_lock.unlock();
    }
    return 0;
}

int
cliChkReconnAtSendEnd( rcComm_t *conn ) {
    if ( conn->svrVersion != NULL && conn->svrVersion->reconnPort > 0 ) {
        /* handle reconn */
        std::unique_lock<std::mutex> boost_lock;
        try {
            boost_lock = std::unique_lock<std::mutex>( *conn->thread_ctx->lock );
        }
        catch ( const std::system_error& ) {
            rodsLog( LOG_ERROR, "lock_error on unique_lock" );
            return SYS_INTERNAL_ERR;
        }
        conn->clientState = PROCESSING_STATE;
        if ( conn->reconnThrState == CONN_WAIT_STATE ) {
            conn->thread_ctx->cond->notify_all();
        }

        boost_lock.unlock();
    }
    return 0;
}

int
cliChkReconnAtReadStart( rcComm_t *conn ) {
    if ( conn->svrVersion != NULL && conn->svrVersion->reconnPort > 0 ) {
        /* handle reconn */
        std::unique_lock<std::mutex> boost_lock;
        try {
            boost_lock = std::unique_lock<std::mutex>( *conn->thread_ctx->lock );
        }
        catch ( const std::system_error& ) {
            rodsLog( LOG_ERROR, "lock_error on unique_lock" );
            return SYS_INTERNAL_ERR;
        }
        conn->clientState = RECEIVING_STATE;
        boost_lock.unlock();
    }
    return 0;
}

int
cliChkReconnAtReadEnd( rcComm_t *conn ) {
    if ( conn->svrVersion != NULL && conn->svrVersion->reconnPort > 0 ) {
        /* handle reconn */
        std::unique_lock<std::mutex> boost_lock;
        try {
            boost_lock = std::unique_lock<std::mutex>( *conn->thread_ctx->lock );
        }
        catch ( const std::system_error& ) {
            rodsLog( LOG_ERROR, "lock_error on unique_lock" );
            return SYS_INTERNAL_ERR;
        }
        conn->clientState = PROCESSING_STATE;
        if ( conn->reconnThrState == CONN_WAIT_STATE ) {
            rodsLog( LOG_DEBUG,
                     "cliChkReconnAtReadEnd:ThrState=CONN_WAIT_STATE, clientState=%d",
                     conn->clientState );

            conn->thread_ctx->cond->notify_all();
            /* wait for reconnManager to get done */
            conn->thread_ctx->cond->wait( boost_lock );
        }
        boost_lock.unlock();
    }
    return 0;
}

int
isLoopbackAddress(const char* ip_address) {
    try {
        return boost::asio::ip::make_address_v4(ip_address).is_loopback();
    }
    catch (...) {
        return false;
    }
}
