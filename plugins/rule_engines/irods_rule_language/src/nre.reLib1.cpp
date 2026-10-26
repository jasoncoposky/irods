#include "irods/rodsKeyWdDef.h"
#define HAS_MICROSDEF_T
#include "irods/rodsConnect.h"
#include "irods/private/re/reGlobals.hpp"
#include "irods/private/re/reFuncDefs.hpp"
#include "irods/private/re/reHelpers1.hpp"
#include "irods/apiHeaderAll.h"
#include "irods/private/re/parser.hpp"
#include "irods/private/re/index.hpp"
#include "irods/private/re/rules.hpp"
#include "irods/private/re/cache.hpp"
#include "irods/locks.hpp"
#include "irods/private/re/functions.hpp"
#include "irods/private/re/configuration.hpp"
#include "irods/irods_log.hpp"
#include "irods/irods_get_full_path_for_config_file.hpp"
#include "irods/irods_re_plugin.hpp"
#include "irods/rsEndTransaction.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/rs_genquery2.hpp"

#include <fmt/format.h>
#include <charconv>
#include <cstring>
#include <string>
#include <string_view>

#ifdef MYMALLOC
# Within reLib1.c here, change back the redefines of malloc back to normal
#define malloc(x) malloc(x)
#define free(x) free(x)
#endif

extern int GlobalAllRuleExecFlag;

int processReturnRes( Res *res );



int
applyRule331( char *inAction, msParamArray_t *inMsParamArray, ruleExecInfo_t *rei, int reiSaveFlag );

int
applyRuleArg331( const char *action, const char *args[MAX_NUM_OF_ARGS_IN_ACTION], int argc,
              ruleExecInfo_t *rei, int reiSaveFlag ) {
#ifdef DEBUG
    writeToTmp( "entry.log", "applyRuleArg: " );
    writeToTmp( "entry.log", action );
    writeToTmp( "entry.log", "(pass on to applyRulePA)\n" );
#endif
    msParamArray_t *inMsParamArray = NULL;
    int i;

    i = applyRuleArgPA( action , args,  argc, inMsParamArray, rei, reiSaveFlag );
    return i;
}

/* utility function */
int processReturnRes( Res *res ) {
    int ret;
    if ( res->nodeType == N_ERROR ) {
        ret = RES_ERR_CODE( res );
    }
    else {
        switch ( TYPE( res ) ) {
        case T_INT:
            ret = RES_INT_VAL( res );
            break;
        default:
            ret = 0; /* other types */
            break;
        }
    }

    return ret;
}
int computeExpression( char *inAction, msParamArray_t *inMsParamArray, ruleExecInfo_t *rei, int reiSaveFlag, char *res ) {
#ifdef DEBUG
    writeToTmp( "entry.log", "computeExpression: " );
    writeToTmp( "entry.log", inAction );
    writeToTmp( "entry.log", "\n" );
#endif

    Region *r = make_region( 0, NULL );

    Res *res0 = parseAndComputeExpressionAdapter( inAction, inMsParamArray, 0, rei, reiSaveFlag, r );
    int ret;
    char *res1 = convertResToString( res0 );
    snprintf( res, MAX_COND_LEN, "%s", res1 );
    free( res1 );

    if ( getNodeType( res0 ) == N_ERROR ) {
        ret = RES_ERR_CODE( res0 );
    }
    else {
        switch ( TYPE( res0 ) ) {
        case T_INT:
            ret = RES_INT_VAL( res0 );
            break;
        case T_BOOL:
            ret = !RES_BOOL_VAL( res0 );
            break;
        default:
            ret = 0; /* other types */
            break;
        }
    }
    region_free( r );

    return ret;
}


/* This function computes the expression in inAction using inMsParamArray as the env.
 * It doesn't update inMsParamArray with the new env. */
int
applyRule331( char *inAction, msParamArray_t *inMsParamArray, ruleExecInfo_t *rei, int reiSaveFlag ) {
    return applyRuleBase( inAction, inMsParamArray, 0, rei, reiSaveFlag );
}

/* This function computes the expression in inAction using inMsParamArray as the env and updates inMsParamArray with the new env. */
int
applyRuleUpdateParams( char *inAction, msParamArray_t *inMsParamArray, ruleExecInfo_t *rei, int reiSaveFlag ) {
    return applyRuleBase( inAction, inMsParamArray, 1, rei, reiSaveFlag );

}

int
applyRuleBase( char *inAction, msParamArray_t *inMsParamArray, int updateInMsParam, ruleExecInfo_t *rei, int reiSaveFlag ) {
#if defined(DEBUG) || defined(RE_LOG_RULES_TMP)
    writeToTmp( "entry.log", "applyRule: " );
    writeToTmp( "entry.log", inAction );
    writeToTmp( "entry.log", "\n" );
#endif
    if ( GlobalREAuditFlag > 0 ) {
        RuleEngineEventParam param;
        param.actionName = inAction;
        param.ruleIndex = -1;
        reDebug( APPLY_RULE_BEGIN, -1, &param, NULL, NULL, rei );

    }

    Region *r = make_region( 0, NULL );
    int ret;
    Res *res;
    if ( inAction[strlen( inAction ) - 1] == '|' ) {
        char *inActionCopy = strdup( inAction );
        inActionCopy[strlen( inAction ) - 1] = '\0';
        char *action = ( char * ) malloc( sizeof( char ) * strlen( inAction ) + 3 );
        sprintf( action, "{%s}", inActionCopy );
        res = parseAndComputeExpressionAdapter( action, inMsParamArray, updateInMsParam, rei, reiSaveFlag, r );
        free( action );
        free( inActionCopy );
    }
    else {
        res = parseAndComputeExpressionAdapter( inAction, inMsParamArray, updateInMsParam, rei, reiSaveFlag, r );
    }
    ret = processReturnRes( res );
    region_free( r );
    if ( GlobalREAuditFlag > 0 ) {
        RuleEngineEventParam param;
        param.actionName = inAction;
        param.ruleIndex = -1;
        reDebug( APPLY_RULE_END, -1, &param, NULL, NULL, rei );
    }

    return ret;

}


// devnote: this method differs from applyRuleArg in that it allows passing in a preallocated msParamArray_t
int applyRuleArgPA( const char *action, const char *args[MAX_NUM_OF_ARGS_IN_ACTION], int argc,
                msParamArray_t *inMsParamArray, ruleExecInfo_t *rei, int reiSaveFlag ) {
    int i;

    Region *r = make_region( 0, NULL );
    rError_t errmsgBuf;
    errmsgBuf.errMsg = NULL;
    errmsgBuf.len = 0;
    Res *res = computeExpressionWithParams( action, args, argc, rei, reiSaveFlag, inMsParamArray, &errmsgBuf, r );
    i = processReturnRes( res );
    region_free( r );
    /* applyRule(tmpStr, inMsParamArray, rei, reiSaveFlag); */
    if ( i < 0 ) {
        logErrMsg( &errmsgBuf, &rei->rsComm->rError );
    }
    freeRErrorContent( &errmsgBuf );
    return i;

}





/**
 * \fn applyAllRules( char *inAction, msParamArray_t *inMsParamArray, ruleExecInfo_t *rei, int reiSaveFlag, int allRuleExecFlag )
 *
 * \brief  DEPRECATED - applyAllRules is too ad-hoc and non-deterministic.
 *      If you need to apply multiple rules and catch the cases where one may fail,
 *      the best practice is to use the errorcode() and test for the result.
 *
 * \deprecated Since 4.1.0.  Will be removed in 4.2.0
 *
 **/
int
applyAllRules( char *inAction, msParamArray_t *inMsParamArray,
               ruleExecInfo_t *rei, int reiSaveFlag, int allRuleExecFlag ) {
#ifdef DEBUG
    writeToTmp( "entry.log", "applyAllRules: " );
    writeToTmp( "entry.log", inAction );
    writeToTmp( "entry.log", "(set GlobalAllRuleExecFlag and forward to applyRule)\n" );
#endif

    /* store global flag in a temp variables */
    int tempFlag = GlobalAllRuleExecFlag;
    /* set global flag */
    GlobalAllRuleExecFlag = allRuleExecFlag == 1 ? 2 : 1;

    if ( GlobalREAuditFlag > 0 ) {
        RuleEngineEventParam param;
        param.actionName = inAction;
        param.ruleIndex = -1;
        reDebug( APPLY_ALL_RULES_BEGIN, -1, &param, NULL, NULL, rei );
    }

    int ret = applyRule( inAction, inMsParamArray, rei, reiSaveFlag );

    if ( GlobalREAuditFlag > 0 ) {
        RuleEngineEventParam param;
        param.actionName = inAction;
        param.ruleIndex = -1;
        reDebug( APPLY_ALL_RULES_END, -1, &param, NULL, NULL, rei );
    }

    /* restore global flag */
    GlobalAllRuleExecFlag = tempFlag;
    return ret;
}

int
execMyRule( char * ruleDef, msParamArray_t *inMsParamArray, const char *outParamsDesc,
            ruleExecInfo_t *rei ) {

    return execMyRuleWithSaveFlag( ruleDef, inMsParamArray, outParamsDesc, rei, 0 );
}

// Adds all missing outParamNames, except ALL_MS_PARAM_KW, to the MsParamArray.
void appendOutputToInput(MsParamArray* inpParamArray, char** outParamNames, int outParamN)
{
    for (int i = 0; i < outParamN; ++i) {
        const std::string_view param_name = outParamNames[i];

        // Ignore ALL_MS_PARAM_KW keyword.
        if (param_name == ALL_MS_PARAM_KW) {
            continue;
        }

        bool is_in_msParamArray = false;

        for (int k = 0; k < inpParamArray->len; ++k) {
            if (inpParamArray->msParam[k]->label && param_name == inpParamArray->msParam[k]->label) {
                is_in_msParamArray = true;
                break;
            }
        }

        if (!is_in_msParamArray) {
            addMsParam(inpParamArray, outParamNames[i], nullptr, nullptr, nullptr);
        }
    }
}

int extractVarNames( char **varNames, const char *outBuf ) {
    int n = 0;
    const char *p = outBuf;
    const char *psrc = p;

    while (n < MAX_PARAMS_LEN) {
        if ( *psrc == '%' ) {
            varNames[n++] = strndup( p, psrc - p );
            p = psrc + 1;
        }
        else if ( *psrc == '\0' ) {
            if ( strlen( p ) != 0 ) {
                varNames[n++] = strdup( p );
            }
            break;
        }
        psrc++;
    }
    return n;
}

int
execMyRuleWithSaveFlag( char * ruleDef, msParamArray_t *inMsParamArray, const char *outParamsDesc,
                        ruleExecInfo_t *rei, int reiSaveFlag )
{
    int status;
    if ( GlobalREAuditFlag ) {
        RuleEngineEventParam param;
        param.actionName = ruleDef;
        param.ruleIndex = -1;
        reDebug( EXEC_MY_RULE_BEGIN, -1, &param, NULL, NULL, rei );
    }

    char *outParamNames[MAX_PARAMS_LEN];
    int n = extractVarNames( outParamNames, outParamsDesc );
    appendOutputToInput( inMsParamArray, outParamNames, n );

    int i;
    for ( i = 0; i < n; i++ ) {
        free( outParamNames[i] );
    }

    Region *r = make_region( 0, NULL );
    status =
        parseAndComputeRuleAdapter( ruleDef, inMsParamArray, rei, reiSaveFlag, r );
    region_free( r );
    if ( status < 0 ) {
        rodsLog( LOG_NOTICE, "execMyRule %s Failed with status %i", ruleDef, status );
    }

    if ( GlobalREAuditFlag ) {
        RuleEngineEventParam param;
        param.actionName = ruleDef;
        param.ruleIndex = -1;
        reDebug( EXEC_MY_RULE_END, -1, &param, NULL, NULL, rei );
    }
    return status;
}

int
initRuleStruct( const char* inst_name, rsComm_t *svrComm, const char *irbSet, const char *dvmSet, const char *fnmSet ) {
    int i;
    char r1[NAME_LEN], r2[RULE_SET_DEF_LENGTH], r3[RULE_SET_DEF_LENGTH];

    /*strcpy(r2,irbSet);*/
    coreRuleStrct.MaxNumOfRules = 0;
    appRuleStrct.MaxNumOfRules = 0;
    GlobalAllRuleExecFlag = 0;

    i = readRuleStructFromFile( inst_name, irbSet );
    if ( i < 0 ) {
        return i;
    }

    snprintf( r2, sizeof( r2 ), "%s", dvmSet );
    coreRuleVarDef.MaxNumOfDVars = 0;
    appRuleVarDef.MaxNumOfDVars = 0;

    while ( strlen( r2 ) > 0 ) {
        i = rSplitStr( r2, r1, NAME_LEN, r3, RULE_SET_DEF_LENGTH, ',' );
        if ( i == 0 ) {
            i = readDVarStructFromFile( r1, &coreRuleVarDef );
        }
        if ( i < 0 ) {
            return i;
        }
        snprintf( r2, sizeof( r2 ), "%s", r3 );
    }
    snprintf( r2, sizeof( r2 ), "%s", fnmSet );
    coreRuleFuncMapDef.MaxNumOfFMaps = 0;
    appRuleFuncMapDef.MaxNumOfFMaps = 0;

    while ( strlen( r2 ) > 0 ) {
        i = rSplitStr( r2, r1, NAME_LEN, r3, RULE_SET_DEF_LENGTH, ',' );
        if ( i == 0 ) {
            i = readFuncMapStructFromFile( r1, &coreRuleFuncMapDef );
        }
        if ( i < 0 ) {
            return i;
        }
        snprintf( r2, sizeof( r2 ), "%s", r3 );
    }

    /* read logging settings */
    if ( svrComm != NULL ) { /* if this is not a process started by a client, then we used the default logging setting */
        readICatUserLogging( svrComm->clientUser.userName, &ruleEngineConfig.logging, svrComm );
    }

    if ( const char* env_val = getenv( RETESTFLAG ); env_val != NULL ) {
        reTestFlag = 0;
        std::from_chars(env_val, env_val + std::strlen(env_val), reTestFlag);
        if ( const char* lb_val = getenv( RELOOPBACKFLAG ); lb_val != NULL ) {
            reLoopBackFlag = 0;
            std::from_chars(lb_val, lb_val + std::strlen(lb_val), reLoopBackFlag);
        }
        else {
            reLoopBackFlag = 0;
        }
    }
    else {
        reTestFlag = 0;
        reLoopBackFlag = 0;
    }
    if ( getenv( "GLOBALALLRULEEXECFLAG" ) != NULL ) {
        GlobalAllRuleExecFlag = 9;
    }

    if ( const char* dbg_val = getenv( GLOBALREDEBUGFLAG ); dbg_val != NULL ) {
        GlobalREDebugFlag = 0;
        std::from_chars(dbg_val, dbg_val + std::strlen(dbg_val), GlobalREDebugFlag);
    }
    if ( const char* aud_val = getenv( GLOBALREAUDITFLAG ); aud_val != NULL ) {
        GlobalREAuditFlag = 0;
        std::from_chars(aud_val, aud_val + std::strlen(aud_val), GlobalREAuditFlag);
    }
    if ( GlobalREAuditFlag == 0 ) {
        GlobalREAuditFlag = GlobalREDebugFlag;
    }

    initializeReDebug( svrComm );

    return 0;
}


int
readRuleSetFromDB( char *ruleBaseName, char *versionStr, RuleSet *ruleSet, ruleExecInfo_t *rei, rError_t *errmsg, Region *region ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    Env *env = newEnv( newHashTable2( 100, region ), NULL, NULL, region );
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"RULE_BASE_MAP_BASE_NAME",
                                             "RULE_NAME",
                                             "RULE_EVENT",
                                             "RULE_CONDITION",
                                             "RULE_BODY",
                                             "RULE_RECOVERY",
                                             "RULE_ID"})
                           .where(col("RULE_BASE_MAP_BASE_NAME") == ruleBaseName &&
                                  col("RULE_BASE_MAP_VERSION") == versionStr)
                           .order_by("RULE_BASE_MAP_PRIORITY", true)
                           .offset(offset)
                           .limit(MAX_SQL_ROWS)
                           .build();

        lite3cpp::Buffer rows_buf;
        if ( gq2::try_execute_buffer( *rei->rsComm, q, rows_buf ) < 0 ) {
            break;
        }
        const gq2::rows_buffer_view rows{rows_buf};
        if ( rows.empty() ) {
            break;
        }

        for ( std::size_t r = 0; r < rows.size(); ++r ) {
            char ruleStr[MAX_RULE_LEN * 4];
            const auto ruleHead = rows.cell_str(r, 2);
            const auto ruleCondition = rows.cell_str(r, 3);
            const auto ruleAction = rows.cell_str(r, 4);
            const auto ruleRecovery = rows.cell_str(r, 5);
            long ruleId = 0;
            rows.cell_int(r, 6, ruleId);

            if ( !ruleRecovery.empty() && ruleRecovery[0] == '@' ) {
                const auto tag = std::string_view{ruleRecovery}.substr(1);
                if ( tag == "DATA" ) {
                    snprintf( ruleStr, MAX_RULE_LEN * 4, "data %s", ruleHead.c_str() );
                }
                else if ( tag == "CONSTR" ) {
                    snprintf( ruleStr, MAX_RULE_LEN * 4, "constructor %s : %s", ruleHead.c_str(), ruleAction.c_str() );
                }
                else if ( tag == "EXTERN" ) {
                    snprintf( ruleStr, MAX_RULE_LEN * 4, "%s : %s", ruleHead.c_str(), ruleAction.c_str() );
                }
                else if ( tag == "FUNC" ) {
                    snprintf( ruleStr, MAX_RULE_LEN * 4, "%s = %s\n @(\"id\", \"%ld\")", ruleHead.c_str(), ruleAction.c_str(), ruleId );
                }
                else {
                    snprintf( ruleStr, MAX_RULE_LEN * 4, "%s { on %s %s @(\"id\", \"%ld\") }\n", ruleHead.c_str(), ruleCondition.c_str(), ruleAction.c_str(), ruleId );
                }
            }
            else {
                snprintf( ruleStr, MAX_RULE_LEN * 4, "%s|%s|%s|%s", ruleHead.c_str(), ruleCondition.c_str(), ruleAction.c_str(), ruleRecovery.c_str() );
            }
            Pointer *p = newPointer2( ruleStr );
            int errloc;
            int errcode = parseRuleSet( p, ruleSet, env, &errloc, errmsg, region );
            deletePointer( p );
            if ( errcode < 0 ) {
                return errcode;
            }
        }

        if ( rows.size() < static_cast<std::size_t>(MAX_SQL_ROWS) ) {
            break;
        }
        offset += rows.size();
    }

    return 0;
}

int
readRuleStructFromDB( char *ruleBaseName, char *versionStr, ruleStruct_t *inRuleStrct, ruleExecInfo_t *rei ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    std::size_t offset = 0;
    while ( true ) {
        const auto q = gq2::builder::select({"RULE_BASE_MAP_BASE_NAME",
                                             "RULE_NAME",
                                             "RULE_EVENT",
                                             "RULE_CONDITION",
                                             "RULE_BODY",
                                             "RULE_RECOVERY",
                                             "RULE_ID"})
                           .where(col("RULE_BASE_MAP_BASE_NAME") == ruleBaseName &&
                                  col("RULE_BASE_MAP_VERSION") == versionStr)
                           .order_by("RULE_BASE_MAP_PRIORITY", true)
                           .offset(offset)
                           .limit(MAX_SQL_ROWS)
                           .build();

        lite3cpp::Buffer rows_buf;
        if ( gq2::try_execute_buffer( *rei->rsComm, q, rows_buf ) < 0 ) {
            break;
        }
        const gq2::rows_buffer_view rows{rows_buf};
        if ( rows.empty() ) {
            break;
        }

        for ( std::size_t r = 0; r < rows.size(); ++r ) {
            const int idx = inRuleStrct->MaxNumOfRules;
            inRuleStrct->ruleBase[idx] = rows.cell_strdup(r, 0);
            inRuleStrct->action[idx] = rows.cell_strdup(r, 1);
            inRuleStrct->ruleHead[idx] = rows.cell_strdup(r, 2);
            inRuleStrct->ruleCondition[idx] = rows.cell_strdup(r, 3);
            inRuleStrct->ruleAction[idx] = rows.cell_strdup(r, 4);
            inRuleStrct->ruleRecovery[idx] = rows.cell_strdup(r, 5);
            long rule_id = 0;
            rows.cell_int(r, 6, rule_id);
            inRuleStrct->ruleId[idx] = rule_id;
            inRuleStrct->MaxNumOfRules++;
        }

        if ( rows.size() < static_cast<std::size_t>(MAX_SQL_ROWS) ) {
            break;
        }
        offset += rows.size();
    }

    return 0;
}


int
readDVMapStructFromDB( char *dvmBaseName, char *versionStr, rulevardef_t *inDvmStrct, ruleExecInfo_t *rei ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    int l = inDvmStrct->MaxNumOfDVars;
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"DVM_EXT_VAR_NAME", "DVM_CONDITION", "DVM_INT_MAP_PATH", "DVM_ID"})
                           .where(col("DVM_BASE_MAP_BASE_NAME") == dvmBaseName &&
                                  col("DVM_BASE_MAP_VERSION") == versionStr)
                           .order_by("DVM_ID", true)
                           .offset(offset)
                           .limit(MAX_SQL_ROWS)
                           .build();

        lite3cpp::Buffer rows_buf;
        if ( gq2::try_execute_buffer( *rei->rsComm, q, rows_buf ) < 0 ) {
            break;
        }
        const gq2::rows_buffer_view rows{rows_buf};
        if ( rows.empty() ) {
            break;
        }

        for ( std::size_t r = 0; r < rows.size(); ++r ) {
            inDvmStrct->varName[l] = rows.cell_strdup(r, 0);
            inDvmStrct->action[l] = rows.cell_strdup(r, 1);
            inDvmStrct->var2CMap[l] = rows.cell_strdup(r, 2);
            long var_id = 0;
            rows.cell_int(r, 3, var_id);
            inDvmStrct->varId[l] = var_id;
            l++;
        }

        if ( rows.size() < static_cast<std::size_t>(MAX_SQL_ROWS) ) {
            break;
        }
        offset += rows.size();
    }

    inDvmStrct->MaxNumOfDVars = l;
    return 0;
}



int
readFNMapStructFromDB( char *fnmBaseName, char *versionStr, fnmapStruct_t *inFnmStrct, ruleExecInfo_t *rei ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    int l = inFnmStrct->MaxNumOfFMaps;
    std::size_t offset = 0;

    while ( true ) {
        const auto q = gq2::builder::select({"FNM_EXT_FUNC_NAME", "FNM_INT_FUNC_NAME", "FNM_ID"})
                           .where(col("FNM_BASE_MAP_BASE_NAME") == fnmBaseName &&
                                  col("FNM_BASE_MAP_VERSION") == versionStr)
                           .order_by("FNM_ID", true)
                           .offset(offset)
                           .limit(MAX_SQL_ROWS)
                           .build();

        lite3cpp::Buffer rows_buf;
        if ( gq2::try_execute_buffer( *rei->rsComm, q, rows_buf ) < 0 ) {
            break;
        }
        const gq2::rows_buffer_view rows{rows_buf};
        if ( rows.empty() ) {
            break;
        }

        for ( std::size_t r = 0; r < rows.size(); ++r ) {
            inFnmStrct->funcName[l] = rows.cell_strdup(r, 0);
            inFnmStrct->func2CMap[l] = rows.cell_strdup(r, 1);
            long fmap_id = 0;
            rows.cell_int(r, 2, fmap_id);
            inFnmStrct->fmapId[l] = fmap_id;
            l++;
        }

        if ( rows.size() < static_cast<std::size_t>(MAX_SQL_ROWS) ) {
            break;
        }
        offset += rows.size();
    }

    inFnmStrct->MaxNumOfFMaps = l;
    return 0;
}



int
readMsrvcStructFromDB( int inStatus, msrvcStruct_t *inMsrvcStrct, ruleExecInfo_t *rei ) {
    namespace gq2 = irods::experimental::genquery2;
    using gq2::builder::col;

    int l = inMsrvcStrct->MaxNumOfMsrvcs;
    std::size_t offset = 0;
    const auto status_str = fmt::to_string(inStatus);

    while ( true ) {
        const auto q = gq2::builder::select({"MSRVC_MODULE_NAME",
                                             "MSRVC_NAME",
                                             "MSRVC_SIGNATURE",
                                             "MSRVC_VERSION",
                                             "MSRVC_HOST",
                                             "MSRVC_LOCATION",
                                             "MSRVC_LANGUAGE",
                                             "MSRVC_TYPE_NAME",
                                             "MSRVC_STATUS",
                                             "MSRVC_ID"})
                           .where(col("MSRVC_STATUS") == status_str)
                           .order_by("MSRVC_ID", true)
                           .offset(offset)
                           .limit(MAX_SQL_ROWS)
                           .build();

        lite3cpp::Buffer rows_buf;
        if ( gq2::try_execute_buffer( *rei->rsComm, q, rows_buf ) < 0 ) {
            break;
        }
        const gq2::rows_buffer_view rows{rows_buf};
        if ( rows.empty() ) {
            break;
        }

        for ( std::size_t r = 0; r < rows.size(); ++r ) {
            inMsrvcStrct->moduleName[l] = rows.cell_strdup(r, 0);
            inMsrvcStrct->msrvcName[l] = rows.cell_strdup(r, 1);
            inMsrvcStrct->msrvcSignature[l] = rows.cell_strdup(r, 2);
            inMsrvcStrct->msrvcVersion[l] = rows.cell_strdup(r, 3);
            inMsrvcStrct->msrvcHost[l] = rows.cell_strdup(r, 4);
            inMsrvcStrct->msrvcLocation[l] = rows.cell_strdup(r, 5);
            inMsrvcStrct->msrvcLanguage[l] = rows.cell_strdup(r, 6);
            inMsrvcStrct->msrvcTypeName[l] = rows.cell_strdup(r, 7);
            long msrvc_status = 0;
            rows.cell_int(r, 8, msrvc_status);
            inMsrvcStrct->msrvcStatus[l] = msrvc_status;
            long msrvc_id = 0;
            rows.cell_int(r, 9, msrvc_id);
            inMsrvcStrct->msrvcId[l] = msrvc_id;
            l++;
        }

        if ( rows.size() < static_cast<std::size_t>(MAX_SQL_ROWS) ) {
            break;
        }
        offset += rows.size();
    }

    inMsrvcStrct->MaxNumOfMsrvcs = l;
    return 0;
}

int
readRuleStructFromFile( const char* inst_name, const char *ruleBaseName ) {
    return loadRuleFromCacheOrFile( inst_name, ruleBaseName );
}

int
clearRuleStruct( ruleStruct_t *inRuleStrct ) {
    int i;
    for ( i = 0 ; i < inRuleStrct->MaxNumOfRules ; i++ ) {
        if ( inRuleStrct->ruleBase[i]  != NULL ) {
            free( inRuleStrct->ruleBase[i] );
        }
        if ( inRuleStrct->ruleHead[i]  != NULL ) {
            free( inRuleStrct->ruleHead[i] );
        }
        if ( inRuleStrct->ruleCondition[i]  != NULL ) {
            free( inRuleStrct->ruleCondition[i] );
        }
        if ( inRuleStrct->ruleAction[i]  != NULL ) {
            free( inRuleStrct->ruleAction[i] );
        }
        if ( inRuleStrct->ruleRecovery[i]  != NULL ) {
            free( inRuleStrct->ruleRecovery[i] );
        }

    }
    inRuleStrct->MaxNumOfRules  = 0;
    if ( inRuleStrct == &coreRuleStrct ) {
        clearResources( RESC_CORE_RULE_SET | RESC_CORE_FUNC_DESC_INDEX );
    }
    else if ( inRuleStrct == &appRuleStrct ) {
        clearResources( RESC_APP_RULE_SET | RESC_APP_FUNC_DESC_INDEX );
    }

    return 0;
}

int clearDVarStruct( rulevardef_t *inRuleVarDef ) {
    int i;
    for ( i = 0 ; i < inRuleVarDef->MaxNumOfDVars; i++ ) {
        if ( inRuleVarDef->varName[i] != NULL ) {
            free( inRuleVarDef->varName[i] );
        }
        if ( inRuleVarDef->action[i] != NULL ) {
            free( inRuleVarDef->action[i] );
        }
        if ( inRuleVarDef->var2CMap[i] != NULL ) {
            free( inRuleVarDef->var2CMap[i] );
        }
    }
    inRuleVarDef->MaxNumOfDVars =  0;
    return 0;
}

int clearFuncMapStruct( rulefmapdef_t* inRuleFuncMapDef ) {
    int i;
    for ( i = 0 ; i < inRuleFuncMapDef->MaxNumOfFMaps; i++ ) {
        if ( inRuleFuncMapDef->funcName[i] != NULL ) {
            free( inRuleFuncMapDef->funcName[i] );
        }
        if ( inRuleFuncMapDef->func2CMap[i] != NULL ) {
            free( inRuleFuncMapDef->func2CMap[i] );
        }
    }
    inRuleFuncMapDef->MaxNumOfFMaps = 0;
    if ( inRuleFuncMapDef == &coreRuleFuncMapDef ) {
        clearIndex( &coreRuleFuncMapDefIndex );
    }
    else if ( inRuleFuncMapDef == &appRuleFuncMapDef ) {
        clearIndex( &appRuleFuncMapDefIndex );
    }

    return 0;
}


int
readDVarStructFromFile( char *dvarBaseName, rulevardef_t *inRuleVarDef ) {
    int i = 0;
    char l0[MAX_DVAR_LENGTH];
    char l1[MAX_DVAR_LENGTH];
    char l2[MAX_DVAR_LENGTH];
    char l3[MAX_DVAR_LENGTH];
    char dvarsFileName[MAX_NAME_LEN];
    FILE *file;
    char buf[MAX_DVAR_LENGTH];
    //char *configDir;

    i = inRuleVarDef->MaxNumOfDVars;

    if ( dvarBaseName[0] == '/' || dvarBaseName[0] == '\\' ||
            dvarBaseName[1] == ':' ) {
        snprintf( dvarsFileName, MAX_NAME_LEN, "%s", dvarBaseName );
    }
    else {
        std::string cfg_file, fn( dvarBaseName );
        fn += ".dvm";
        irods::error ret = irods::get_full_path_for_config_file( fn, cfg_file );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }
        snprintf( dvarsFileName, sizeof( dvarsFileName ), "%s", cfg_file.c_str() );

    }
    file = fopen( dvarsFileName, "r" );
    if ( file == NULL ) {
        rodsLog( LOG_NOTICE,
                 "readDvarStructFromFile() could not open dvm file %s\n",
                 dvarsFileName );
        return DVARMAP_FILE_READ_ERROR;
    }
    const auto close_file = irods::at_scope_exit{[file] { std::fclose(file); }};
    buf[MAX_DVAR_LENGTH - 1] = '\0';
    while ( fgets( buf, MAX_DVAR_LENGTH - 1, file ) != NULL ) {
        if ( buf[strlen( buf ) - 1] == '\n' ) {
            buf[strlen( buf ) - 1] = '\0';
        }
        if ( buf[0] == '#' || strlen( buf ) < 4 ) {
            continue;
        }
        rSplitStr( buf, l1, MAX_DVAR_LENGTH, l0, MAX_DVAR_LENGTH, '|' );
        inRuleVarDef->varName[i] = strdup( l1 ); /** varName **/
        rSplitStr( l0, l1, MAX_DVAR_LENGTH, l3, MAX_DVAR_LENGTH, '|' );
        inRuleVarDef->action[i] = strdup( l1 ); /** action **/
        rSplitStr( l3, l1, MAX_DVAR_LENGTH, l2, MAX_DVAR_LENGTH, '|' );
        inRuleVarDef->var2CMap[i] = strdup( l1 ); /** var2CMap **/
        if ( strlen( l2 ) > 0 ) {
            inRuleVarDef->varId[i] = 0;
            std::from_chars( l2, l2 + std::strlen( l2 ), inRuleVarDef->varId[i] );    /** varId **/
        }
        else {
            inRuleVarDef->varId[i] = i;    /** ruleId **/
        }
        i++;
    }
    inRuleVarDef->MaxNumOfDVars = ( long int )  i;
    return 0;
}

int
readFuncMapStructFromFile( char *fmapBaseName, rulefmapdef_t* inRuleFuncMapDef ) {
    int i = 0;
    char l0[MAX_FMAP_LENGTH];
    char l1[MAX_FMAP_LENGTH];
    char l2[MAX_FMAP_LENGTH];
    /*  char l3[MAX_FMAP_LENGTH];*/
    char fmapsFileName[MAX_NAME_LEN];
    FILE *file;
    char buf[MAX_FMAP_LENGTH];
    //char *configDir;

    i = inRuleFuncMapDef->MaxNumOfFMaps;

    if ( fmapBaseName[0] == '/' || fmapBaseName[0] == '\\' ||
            fmapBaseName[1] == ':' ) {
        snprintf( fmapsFileName, MAX_NAME_LEN, "%s", fmapBaseName );
    }
    else {
        std::string cfg_file, fn( fmapBaseName );
        fn += ".fnm";
        irods::error ret = irods::get_full_path_for_config_file( fn, cfg_file );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }
        snprintf( fmapsFileName, sizeof( fmapsFileName ), "%s", cfg_file.c_str() );
    }
    file = fopen( fmapsFileName, "r" );
    if ( file == NULL ) {
        rodsLog( LOG_NOTICE,
                 "readFmapStructFromFile() could not open fnm file %s\n",
                 fmapsFileName );
        return FMAP_FILE_READ_ERROR;
    }
    const auto close_file = irods::at_scope_exit{[file] { std::fclose(file); }};
    buf[MAX_FMAP_LENGTH - 1] = '\0';
    while ( fgets( buf, MAX_FMAP_LENGTH - 1, file ) != NULL ) {
        if ( buf[strlen( buf ) - 1] == '\n' ) {
            buf[strlen( buf ) - 1] = '\0';
        }
        if ( buf[0] == '#' || strlen( buf ) < 4 ) {
            continue;
        }
        rSplitStr( buf, l1, MAX_FMAP_LENGTH, l0, MAX_FMAP_LENGTH, '|' );
        inRuleFuncMapDef->funcName[i] = strdup( l1 ); /** funcName **/
        rSplitStr( l0, l1, MAX_FMAP_LENGTH, l2, MAX_FMAP_LENGTH, '|' );
        inRuleFuncMapDef->func2CMap[i] = strdup( l1 ); /** func2CMap **/
        if ( strlen( l2 ) > 0 ) {
            inRuleFuncMapDef->fmapId[i] = 0;
            std::from_chars( l2, l2 + std::strlen( l2 ), inRuleFuncMapDef->fmapId[i] );    /** fmapId **/
        }
        else {
            inRuleFuncMapDef->fmapId[i] = i;    /** fmapId **/
        }
        i++;
    }
    inRuleFuncMapDef->MaxNumOfFMaps = ( long int ) i;
    if ( inRuleFuncMapDef == &coreRuleFuncMapDef ) {
        createFuncMapDefIndex( &coreRuleFuncMapDef, &coreRuleFuncMapDefIndex );
    }
    else if ( inRuleFuncMapDef == &appRuleFuncMapDef ) {
        createFuncMapDefIndex( &appRuleFuncMapDef, &appRuleFuncMapDefIndex );
    }
    return 0;
}

int
readMsrvcStructFromFile( char *msrvcFileName, msrvcStruct_t* inMsrvcStruct ) {
    int i = 0;
    char l0[MAX_RULE_LENGTH];
    char l1[MAX_RULE_LENGTH];
    char l2[MAX_RULE_LENGTH];
    char mymsrvcFileName[MAX_NAME_LEN];
    FILE *file;
    char buf[MAX_RULE_LENGTH];


    i = inMsrvcStruct->MaxNumOfMsrvcs;

    if ( msrvcFileName[0] == '/' || msrvcFileName[0] == '\\' ||
            msrvcFileName[1] == ':' ) {
        snprintf( mymsrvcFileName, MAX_NAME_LEN, "%s", msrvcFileName );
    }
    else {
        std::string re_dir;
        irods::error ret = irods::get_full_path_for_unmoved_configs(
                               "reConfigs",
                               re_dir );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }

        snprintf( mymsrvcFileName, MAX_NAME_LEN, "%s/%s.msi", re_dir.c_str(), msrvcFileName );
    }
    file = fopen( mymsrvcFileName, "r" );
    if ( file == NULL ) {
        rodsLog( LOG_NOTICE,
                 "readMservcStructFromFile() could not open msrvc file %s\n",
                 mymsrvcFileName );
        return MSRVC_FILE_READ_ERROR;
    }
    buf[MAX_RULE_LENGTH - 1] = '\0';
    while ( fgets( buf, MAX_RULE_LENGTH - 1, file ) != NULL ) {
        if ( buf[strlen( buf ) - 1] == '\n' ) {
            buf[strlen( buf ) - 1] = '\0';
        }
        if ( buf[0] == '#' || strlen( buf ) < 4 ) {
            continue;
        }
        rSplitStr( buf, l1, MAX_RULE_LENGTH, l0, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->moduleName[i] = strdup( l1 );

        rSplitStr( l0, l1, MAX_RULE_LENGTH, l2, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcName[i] = strdup( l1 );
        rSplitStr( l2, l1, MAX_RULE_LENGTH, l0, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcSignature[i] = strdup( l1 );

        rSplitStr( l0, l1, MAX_RULE_LENGTH, l2, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcVersion[i] = strdup( l1 );
        rSplitStr( l2, l1, MAX_RULE_LENGTH, l0, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcHost[i] = strdup( l1 );

        rSplitStr( l0, l1, MAX_RULE_LENGTH, l2, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcLocation[i] = strdup( l1 );
        rSplitStr( l2, l1, MAX_RULE_LENGTH, l0, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcLanguage[i] = strdup( l1 );

        rSplitStr( l0, l1, MAX_RULE_LENGTH, l2, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcTypeName[i] = strdup( l1 );
        rSplitStr( l2, l1, MAX_RULE_LENGTH, l0, MAX_RULE_LENGTH, '|' );
        inMsrvcStruct->msrvcStatus[i] = atol( l1 );
        if ( strlen( l0 ) > 0 ) {
            inMsrvcStruct->msrvcId[i] = atol( l1 );
        }
        else {
            inMsrvcStruct->msrvcId[i] = ( long int ) i;
        }
        i++;
    }
    fclose( file );
    inMsrvcStruct->MaxNumOfMsrvcs = i;
    return 0;
}

int
getRule( int ri, char *ruleBase, char *ruleHead, char *ruleCondition,
         char *ruleAction, char *ruleRecovery, int rSize ) {

    ruleStruct_t& ruleStruct = ri < CORE_RULE_INDEX_OFF ? appRuleStrct : coreRuleStrct;
    int rule_index = ri < CORE_RULE_INDEX_OFF ? ri : ri - CORE_RULE_INDEX_OFF;
    if ( rule_index >= MAX_NUM_OF_RULES ) {
        rodsLog( LOG_ERROR, "Failed to get rule at %d", ri );
        return SYS_RULE_NOT_FOUND;
    }
    rstrcpy( ruleBase , ruleStruct.ruleBase[rule_index], rSize );
    rstrcpy( ruleHead , ruleStruct.ruleHead[rule_index], rSize );
    rstrcpy( ruleCondition , ruleStruct.ruleCondition[rule_index], rSize );
    rstrcpy( ruleAction , ruleStruct.ruleAction[rule_index], rSize );
    rstrcpy( ruleRecovery , ruleStruct.ruleRecovery[rule_index], rSize );
    return 0;
}

int
writeRulesIntoFile( char * inFileName, ruleStruct_t *myRuleStruct,
                    ruleExecInfo_t* ) {

    int i;
    FILE *file;
    char fileName[MAX_NAME_LEN];
    //char *configDir;

    if ( inFileName[0] == '/' || inFileName[0] == '\\' ||
            inFileName[1] == ':' ) {
        snprintf( fileName, MAX_NAME_LEN, "%s", inFileName );
    }
    else {
        std::string cfg_file, fn( inFileName );
        fn += ".irb";
        irods::error ret = irods::get_full_path_for_config_file( fn, cfg_file );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }
        snprintf( fileName, sizeof( fileName ), "%s", cfg_file.c_str() );

    }


    file = fopen( fileName, "w" );
    if ( file == NULL ) {
        rodsLog( LOG_NOTICE,
                 "writeRulesIntoFile() could not open rules file %s for writing\n",
                 fileName );
        return FILE_OPEN_ERR;
    }
    for ( i = 0; i < myRuleStruct->MaxNumOfRules; i++ ) {
        fprintf( file, "%s|%s|%s|%s|%ld\n", myRuleStruct->ruleHead[i],
                 myRuleStruct->ruleCondition[i],
                 myRuleStruct->ruleAction[i],
                 myRuleStruct->ruleRecovery[i],
                 myRuleStruct->ruleId[i] );

    }
    fclose( file );
    return 0;
}

int
writeDVMapsIntoFile( char * inFileName, dvmStruct_t *myDVMapStruct,
                     ruleExecInfo_t* ) {
    int i;
    FILE *file;
    char fileName[MAX_NAME_LEN];

    if ( inFileName[0] == '/' || inFileName[0] == '\\' ||
            inFileName[1] == ':' ) {
        snprintf( fileName, MAX_NAME_LEN, "%s", inFileName );
    }
    else {
        std::string cfg_file, fn( inFileName );
        fn += ".dvm";
        irods::error ret = irods::get_full_path_for_config_file( fn, cfg_file );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }
        snprintf( fileName, sizeof( fileName ), "%s", cfg_file.c_str() );
    }

    file = fopen( fileName, "w" );
    if ( file == NULL ) {
        rodsLog( LOG_NOTICE,
                 "writeDVMapsIntoFile() could not open rules file %s for writing\n",
                 fileName );
        return FILE_OPEN_ERR;
    }
    for ( i = 0; i < myDVMapStruct->MaxNumOfDVars; i++ ) {
        fprintf( file, "%s|%s|%s|%ld\n", myDVMapStruct->varName[i],
                 myDVMapStruct->action[i],
                 myDVMapStruct->var2CMap[i],
                 myDVMapStruct->varId[i] );
    }
    fclose( file );
    return 0;
}


int
writeFNMapsIntoFile( char * inFileName, fnmapStruct_t *myFNMapStruct,
                     ruleExecInfo_t* ) {
    int i;
    FILE *file;
    char fileName[MAX_NAME_LEN];

    if ( inFileName[0] == '/' || inFileName[0] == '\\' ||
            inFileName[1] == ':' ) {
        snprintf( fileName, MAX_NAME_LEN, "%s", inFileName );
    }
    else {
        std::string cfg_file, fn( inFileName );
        fn += ".fnm";
        irods::error ret = irods::get_full_path_for_config_file( fn, cfg_file );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }
        snprintf( fileName, sizeof( fileName ), "%s", cfg_file.c_str() );
    }

    file = fopen( fileName, "w" );
    if ( file == NULL ) {
        rodsLog( LOG_NOTICE,
                 "writeFNMapsIntoFile() could not open rules file %s for writing\n",
                 fileName );
        return FILE_OPEN_ERR;
    }
    for ( i = 0; i < myFNMapStruct->MaxNumOfFMaps; i++ ) {
        fprintf( file, "%s|%s|%ld\n", myFNMapStruct->funcName[i],
                 myFNMapStruct->func2CMap[i],
                 myFNMapStruct->fmapId[i] );
    }
    fclose( file );
    return 0;
}



int
writeMSrvcsIntoFile( char * inFileName, msrvcStruct_t *myMsrvcStruct,
                     ruleExecInfo_t* ) {
    int i;
    FILE *file;
    char fileName[MAX_NAME_LEN];

    if ( inFileName[0] == '/' || inFileName[0] == '\\' ||
            inFileName[1] == ':' ) {
        snprintf( fileName, MAX_NAME_LEN, "%s", inFileName );
    }
    else {
        std::string re_dir;
        irods::error ret = irods::get_full_path_for_unmoved_configs(
                               "reConfigs",
                               re_dir );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return ret.code();
        }

        snprintf( fileName, MAX_NAME_LEN, "%s/%s.msi", re_dir.c_str(), inFileName );
    }

    file = fopen( fileName, "w" );
    if ( file == NULL ) {
        rodsLog( LOG_NOTICE,
                 "writeMsrvcsIntoFile() could not open microservics file %s for writing\n",
                 fileName );
        return FILE_OPEN_ERR;
    }
    for ( i = 0; i < myMsrvcStruct->MaxNumOfMsrvcs; i++ ) {
        fprintf( file, "%s|%s|%s|%s|%s|%s|%s|%s|%ld|%ld\n",
                 myMsrvcStruct->moduleName[i],
                 myMsrvcStruct->msrvcName[i],
                 myMsrvcStruct->msrvcSignature[i],
                 myMsrvcStruct->msrvcVersion[i],
                 myMsrvcStruct->msrvcHost[i],
                 myMsrvcStruct->msrvcLocation[i],
                 myMsrvcStruct->msrvcLanguage[i],
                 myMsrvcStruct->msrvcTypeName[i],
                 myMsrvcStruct->msrvcStatus[i],
                 myMsrvcStruct->msrvcId[i] );
    }
    fclose( file );
    return 0;
}

