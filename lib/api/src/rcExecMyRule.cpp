#include "irods/execMyRule.h"
#include "irods/oprComplete.h"
#include "irods/dataObjPut.h"
#include "irods/dataObjGet.h"
#include "irods/rcMisc.h"
#include "irods/apiNumber.h"
#include "irods/filesystem/path.hpp"

#include <cstring>

/**
 * \fn rcExecMyRule( rcComm_t *conn, execMyRuleInp_t *execMyRuleInp, msParamArray_t **outParamArray )
 *
 * \brief Execute my rule.
 *
 * \user client
 *
 * \ingroup rules
 *
 * \since 1.0
 *
 *
 * \remark none
 *
 * \note none
*
 * \param[in] conn - A rcComm_t connection handle to the server.
 * \param[in] execMyRuleInp
 * \param[out] outParamArray
 *
 * \return integer
 * \retval 0 on success.
 * \sideeffect none
 * \pre none
 * \post none
 * \sa none
**/
int
rcExecMyRule( rcComm_t *conn, execMyRuleInp_t *execMyRuleInp,
              msParamArray_t **outParamArray ) {

    int status = procApiRequest( conn, EXEC_MY_RULE_AN, execMyRuleInp, NULL,
                                 ( void ** )outParamArray, NULL );

    while ( status == SYS_SVR_TO_CLI_MSI_REQUEST ) {
        /* it is a server request */
        msParam_t *myParam = NULL, *putParam = NULL;

        if ( ( myParam = putParam = getMsParamByLabel( *outParamArray, CL_PUT_ACTION ) ) ||
                ( myParam = getMsParamByLabel( *outParamArray, CL_GET_ACTION ) ) ) {
            //putParam is non-null if it's a put, null if it's a get
            dataObjInp_t * dataObjInp = ( dataObjInp_t * ) myParam->inOutStruct;
            char * locFilePath = getValByKey( &dataObjInp->condInput, LOCAL_PATH_KW );
            char myFile[MAX_NAME_LEN]{};
            if ( !locFilePath ) {
                const irods::experimental::filesystem::path p{dataObjInp->objPath};
                const auto file_sv = p.object_name_view();
                if ( std::strchr( dataObjInp->objPath, '/' ) == NULL ) {
                    status = SYS_INVALID_FILE_PATH;
                }
                else if ( p.parent_path_view().size() >= MAX_NAME_LEN || file_sv.size() >= MAX_NAME_LEN ) {
                    status = USER_STRLEN_TOOLONG;
                }
                else {
                    std::memcpy( myFile, file_sv.data(), file_sv.size() );
                    locFilePath = myFile;
                    status = 0;
                }
            }
            if ( locFilePath ) {
                status = putParam ?
                         rcDataObjPut( conn, dataObjInp, locFilePath ) :
                         rcDataObjGet( conn, dataObjInp, locFilePath );
                rcOprComplete( conn, status );
            }
            else {
                rodsLogError( LOG_ERROR, status,
                              "rcExecMyRule: splitPathByKey for %s error",
                              dataObjInp->objPath );
                rcOprComplete( conn, USER_FILE_DOES_NOT_EXIST );
            }
            clearKeyVal( &dataObjInp->condInput );
        }
        else {
            rcOprComplete( conn, SYS_SVR_TO_CLI_MSI_NO_EXIST );
        }
        /* free outParamArray */
        clearMsParamArray( *outParamArray, 1 );
        free( *outParamArray );
        *outParamArray = NULL;

        /* read the reply from the earlier call */

        status = branchReadAndProcApiReply( conn, EXEC_MY_RULE_AN,
                                            ( void ** )outParamArray, NULL );
        if ( status < 0 ) {
            rodsLogError( LOG_DEBUG, status,
                          "rcExecMyRule: readAndProcApiReply failed. status = %d",
                          status );
        }
    }

    return status;
}
