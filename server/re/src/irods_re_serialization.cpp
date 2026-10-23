#include "irods/irods_re_serialization.hpp"

#include "irods/irods_logger.hpp"
#include "irods/irods_plugin_context.hpp"
#include "irods/logical_quota_utilities.hpp"
#include "irods/msParam.h"
#include "irods/rodsErrorTable.h"
#include "irods/ticketAdmin.h"

#include <boost/range/adaptors.hpp>

#include <fmt/format.h>

#include <cxxabi.h>

namespace
{
    using log_re = irods::experimental::log::rule_engine;
} // anonymous namespace

namespace irods::re_serialization
{
    static void serialize_keyValPair(
        const keyValPair_t&     _kvp,
        serialized_parameter_t& _out) {
        if(_kvp.len > 0) {
            for(int i = 0; i < _kvp.len; ++i) {
               if(_kvp.keyWord && _kvp.keyWord[i]) {
                    if(_kvp.value && _kvp.value[i]) {
                        _out[_kvp.keyWord[i]] = _kvp.value[i];
                    }
                    else {
                        _out[_kvp.keyWord[i]] = "empty_value";
                    }
                }
            }
        } else {
            _out["keyValPair_t"] = "nullptr";
        }
    }

    static error serialize_float_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
             float* f = std::any_cast<float*>(_p);
             if (f) {
                 _out["float_pointer"] = fmt::to_string(*f);
             }
             else {
                 _out["float_pointer"] = "nullptr";
             }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast float*" );
        }

        return SUCCESS();
    } // serialize_float_ptr

    static error serialize_const_std_string_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            const std::string* s = std::any_cast<const std::string*>(_p);
            if (s) {
                _out["const_std_string_ptr"] = *s;
            }
            else {
                _out["const_std_string_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast const_std_string*" );
        }

        return SUCCESS();
    } // serialize_const_std_string_ptr

    static error serialize_std_string_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
             std::string* s = std::any_cast<std::string*>(_p);
             if (s) {
                 _out["std_string_ptr"] = *s;
             }
             else {
                 _out["std_string_ptr"] = "nullptr";
             }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast std_string_ptr" );
        }

        return SUCCESS();
    } // serialize_std_string_ptr

    static error serialize_std_string(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
             _out["std_string"] = std::any_cast<std::string>(_p);
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast std_string*" );
        }

        return SUCCESS();
    } // serialize_std_string

    static error serialize_hierarchy_parser_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            hierarchy_parser* p = std::any_cast<hierarchy_parser*>(_p);
            if (p) {
                std::string hier;
                p->str(hier);
                _out["hierarchy_parser_ptr"] = hier;
            }
            else {
                _out["hierarchy_parser_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast hierarchy_parser_ptr*" );
        }

        return SUCCESS();
    } // serialize_hierarchy_parser_ptr

    static error serialize_rodslong(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            rodsLong_t l = std::any_cast<rodsLong_t>(_p);
            _out["rodslong"] = std::to_string(l);
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast rodslong" );
        }

        return SUCCESS();
    } // serialize_rodslong

    static error serialize_rodslong_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            rodsLong_t* l = std::any_cast<rodsLong_t*>(_p);
            if (l) {
                _out["rodslong_ptr"] = std::to_string(*l);
            }
            else {
                _out["rodslong_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast rodslong" );
        }

        return SUCCESS();
    } // serialize_rodslong_ptr

    static error serialize_sizet(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            size_t l = std::any_cast<size_t>(_p);
            _out["sizet"] = std::to_string(l);
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast sizet" );
        }

        return SUCCESS();
    } // serialize_sizet

    static error serialize_int(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            int l = std::any_cast<int>(_p);
            _out["int"] = std::to_string(l);
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast int" );
        }

        return SUCCESS();
    } // serialize_int

    static error serialize_int_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            int* l = std::any_cast<int*>(_p);
            if (l) {
                _out["int_ptr"] = std::to_string(*l);
            }
            else {
                _out["int_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast int ptr" );
        }

        return SUCCESS();
    } // serialize_rodslong_ptr

    static error serialize_char_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            char* l = std::any_cast<char*>(_p);
            if(l) {
                _out["char_ptr"] = l;
            }
            else {
                _out["char_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast char ptr" );
        }

        return SUCCESS();
    } // serialize_char_ptr

    static error serialize_const_char_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            const char* l = std::any_cast<const char*>(_p);
            if(l) {
                _out["const_char_ptr"] = l;
            }
            else {
                _out["char_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast const char ptr" );
        }

        return SUCCESS();
    } // serialize_const_char_ptr

    static error serialize_rsComm_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            rsComm_t* l = std::any_cast<rsComm_t*>(_p);
            if (l) {
                _out["client_addr"] = l->clientAddr;

                if(l->auth_scheme) {_out["auth_scheme"] = l->auth_scheme;}

                _out["proxy_user_name"] = l->proxyUser.userName;
                _out["proxy_rods_zone"] = l->proxyUser.rodsZone;
                _out["proxy_user_type"] = l->proxyUser.userType;
                _out["proxy_sys_uid"] = std::to_string(l->proxyUser.sysUid);
                _out["proxy_auth_info_auth_scheme"] = l->proxyUser.authInfo.authScheme;
                _out["proxy_auth_info_auth_flag"] = std::to_string(l->proxyUser.authInfo.authFlag);
                _out["proxy_auth_info_flag"] = std::to_string(l->proxyUser.authInfo.flag);
                _out["proxy_auth_info_ppid"] = std::to_string(l->proxyUser.authInfo.ppid);
                _out["proxy_auth_info_host"] = l->proxyUser.authInfo.host;
                _out["proxy_auth_info_auth_str"] = l->proxyUser.authInfo.authStr;
                _out["proxy_user_other_info_user_info"] = l->proxyUser.userOtherInfo.userInfo;
                _out["proxy_user_other_info_user_comments"] = l->proxyUser.userOtherInfo.userComments;
                _out["proxy_user_other_info_user_create"] = l->proxyUser.userOtherInfo.userCreate;
                _out["proxy_user_other_info_user_modify"] = l->proxyUser.userOtherInfo.userModify;

                _out["user_user_name"] = l->clientUser.userName;
                _out["user_rods_zone"] = l->clientUser.rodsZone;
                _out["user_user_type"] = l->clientUser.userType;
                _out["user_sys_uid"] = std::to_string(l->clientUser.sysUid);
                _out["user_auth_info_auth_scheme"] = l->clientUser.authInfo.authScheme;
                _out["user_auth_info_auth_flag"] = std::to_string(l->clientUser.authInfo.authFlag);
                _out["user_auth_info_flag"] = std::to_string(l->clientUser.authInfo.flag);
                _out["user_auth_info_ppid"] = std::to_string(l->clientUser.authInfo.ppid);
                _out["user_auth_info_host"] = l->clientUser.authInfo.host;
                _out["user_auth_info_auth_str"] = l->clientUser.authInfo.authStr;
                _out["user_user_other_info_user_info"] = l->clientUser.userOtherInfo.userInfo;
                _out["user_user_other_info_user_comments"] = l->clientUser.userOtherInfo.userComments;
                _out["user_user_other_info_user_create"] = l->clientUser.userOtherInfo.userCreate;
                _out["user_user_other_info_user_modify"] = l->clientUser.userOtherInfo.userModify;

                _out["socket"] = std::to_string(l->sock);
                _out["connect_count"] = std::to_string(l->connectCnt);
                _out["status"] = std::to_string(l->status);
                _out["api_index"] = std::to_string(l->apiInx);
                _out["option"] = l->option;
            } else {
                _out["rsComm_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast rsComm ptr" );
        }

        return SUCCESS();
    } // serialize_rsComm_ptr

    static error serialize_plugin_context(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            plugin_context l = std::any_cast<plugin_context>(_p);
            if( l.fco().get() ) {
                l.fco()->get_re_vars( _out );
            }

            if( l.comm() ) {
                error ret = serialize_rsComm_ptr( l.comm(), _out ); 
                if(!ret.ok()) {
                    return PASS(ret);
                }
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast plugin_context" );
        }

        return SUCCESS();
    } // serialize_plugin_context

    static void serialize_spec_coll_info_ptr(
            specColl_t* _sc,
            serialized_parameter_t& _out) {
        if( _sc ) {
            _out["coll_class"] = std::to_string(static_cast<int>(_sc->collClass));
            _out["type"] = std::to_string(static_cast<int>(_sc->type));
            _out["collection"] = _sc->collection;
            _out["obj_path"] = _sc->objPath;
            _out["resource"] = _sc->resource;
            _out["resc_hier"] = _sc->rescHier;
            _out["phy_path"] = _sc->phyPath;
            _out["cache_dir"] = _sc->cacheDir;
            _out["cache_dirty"] = std::to_string(_sc->cacheDirty);
            _out["repl_num"] = std::to_string(_sc->replNum);
        }
        else {
            _out["specColl_ptr"] = "nullptr";
        }
    } // serialize_spec_coll_info_ptr

    static error serialize_dataObjInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            dataObjInp_t* l = std::any_cast<dataObjInp_t*>(_p);

            if (l) {
                _out["obj_path"]    = l->objPath;
                _out["create_mode"] = std::to_string(l->createMode);
                _out["open_flags"]  = std::to_string(l->openFlags);
                _out["offset"]      = std::to_string(l->offset);
                _out["data_size"]   = std::to_string(l->dataSize);
                _out["num_threads"] = std::to_string(l->numThreads);
                _out["opr_type"]    = std::to_string(l->oprType);
                if(l->specColl) {
                    serialize_spec_coll_info_ptr(
                            l->specColl,
                            _out );
                }

                serialize_keyValPair(l->condInput, _out);

            } else {
                _out["dataObjInp_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast dataObjInp ptr" );
        }

        return SUCCESS();
    } // serialize_dataObjInp_ptr

    static error serialize_bulkOprInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) {

        try {
            auto* blk = std::any_cast<bulkOprInp_t*>(_p);

            if(!blk) {
                _out["bulkOprInp_ptr"] = "nullptr";
                return SUCCESS();
            }

            serialize_keyValPair(blk->condInput, _out);

            auto obj_path = getSqlResultByInx(&blk->attriArray, COL_DATA_NAME);
            if(!obj_path) {
                THROW(UNMATCHED_KEY_OR_INDEX, "missing object path");
            }

            auto offset = getSqlResultByInx(&blk->attriArray, OFFSET_INX);
            if(!offset) {
                THROW(UNMATCHED_KEY_OR_INDEX, "missing offset");
            }

            std::vector<int> offset_int{};
            for (int i = 0; i < blk->attriArray.rowCnt; ++i) {
                offset_int.push_back(atoi(&offset->value[offset->len * i]));
            }

            for(auto i = 0; i < blk->attriArray.rowCnt; ++i) {
                auto lp = std::string{"logical_path_"}+std::to_string(i);
                auto ds = std::string{"data_size_"}+std::to_string(i);

                _out[lp] = &obj_path->value[obj_path->len * i];
                _out[ds] = std::to_string(i==0 ? offset_int[0] : offset_int[i]-offset_int[i-1]);
            } // for i
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast dataObjInp ptr" );
        }

        return SUCCESS();

    } // serialize_bulkOprInp_ptr

    static error serialize_authResponseInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            authResponseInp_t* l = std::any_cast<authResponseInp_t*>(_p);
            if (l) {
                if(l->response) {
                    _out["response"] = l->response;
                }
                if(l->username) {
                    _out["username"]  = l->username;
                }
            } else {
                _out["authResponseInp_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast authResponseInp*" );
        }

        return SUCCESS();
    } // serialize_authResponseInp_ptr

    static error serialize_dataObjInfo_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            dataObjInfo_t* l = std::any_cast<dataObjInfo_t*>(_p);

            if (l) {
                _out["logical_path"] = l->objPath;
                _out["resc_name"] = l->rescName;
                _out["resc_hier"] = l->rescHier;
                _out["data_type"] = l->dataType;
                _out["data_size"] = std::to_string(l->dataSize);
                _out["checksum"] = l->chksum;
                _out["version"] = l->version;
                _out["physical_path"] = l->filePath;
                _out["data_owner_name"] = l->dataOwnerName;
                _out["data_owner_zone"] = l->dataOwnerZone;
                _out["replica_number"] = std::to_string(l->replNum);
                _out["replica_status"] = std::to_string(l->replStatus);
                _out["status_string"] = l->statusString;
                _out["data_id"] = std::to_string(l->dataId);
                _out["coll_id"] = std::to_string(l->collId);
                _out["data_map_id"] = std::to_string(l->dataMapId);
                _out["flags"] = std::to_string(l->flags);
                _out["data_comments"] = l->dataComments;
                _out["data_mode"] = l->dataMode;
                _out["data_expiry"] = l->dataExpiry;
                _out["data_create"] = l->dataCreate;
                _out["data_modify"] = l->dataModify;
                _out["data_access"] = l->dataAccess;
                _out["data_access_index"] = std::to_string(l->dataAccessInx);
                _out["write_flag"] = std::to_string(l->writeFlag);
                _out["dest_resc_name"] = l->destRescName;
                _out["sub_path"] = l->subPath;
                _out["reg_uid"] = std::to_string(l->regUid);
                _out["other_flags"] = std::to_string(l->otherFlags);
                _out["in_pdmo"] = l->in_pdmo;
                _out["resc_id"] = std::to_string(l->rescId);
                _out["data_access_time"] = l->dataAccessTime;

                // TODO Serialize DataObjInfo objects referenced by the "DataObjInfo::next".
                // To do that requires namespacing due to the flat structure of the serialized
                // data format.

                if (l->specColl) {
                    serialize_spec_coll_info_ptr(l->specColl, _out);
                }

                serialize_keyValPair(l->condInput, _out);
            }
            else {
                _out["dataObjInfo_ptr"] = "nullptr";
            }
        }
        catch (const std::exception&) {
            return ERROR(INVALID_ANY_CAST, "failed to cast dataObjInfo ptr");
        }

        return SUCCESS();
    } // serialize_dataObjInfo_ptr

    static error serialize_keyValPair_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            keyValPair_t* l = std::any_cast<keyValPair_t*>(_p);

            if (l) {
                for(int i = 0; i < l->len; ++i) {
                    _out[l->keyWord[i]] = l->value[i];
                }
            } else {
                _out["keyValPair_ptr"] = "nullptr";
            }

        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast keyValPair ptr" );
        }


        return SUCCESS();
    } // serialize_keyValPair_ptr

    static error serialize_userInfo_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            userInfo_t* l = std::any_cast<userInfo_t*>(_p);

            if (l) {
                _out["user_name"] = l->userName;
                _out["rods_zone"] = l->rodsZone;
                _out["user_type"] = l->userType;
                _out["sys_uid"] = std::to_string(l->sysUid);
                _out["auth_info_auth_scheme"] = l->authInfo.authScheme;
                _out["auth_info_auth_flag"] = std::to_string(l->authInfo.authFlag);
                _out["auth_info_flag"] = std::to_string(l->authInfo.flag);
                _out["auth_info_ppid"] = std::to_string(l->authInfo.ppid);
                _out["auth_info_host"] = l->authInfo.host;
                _out["auth_info_auth_str"] = l->authInfo.authStr;
                _out["user_other_info_user_info"] = l->userOtherInfo.userInfo;
                _out["user_other_info_user_comments"] = l->userOtherInfo.userComments;
                _out["user_other_info_user_create"] = l->userOtherInfo.userCreate;
                _out["user_other_info_user_modify"] = l->userOtherInfo.userModify;
            } else {
                _out["userInfo_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast userInfo ptr" );
        }


        return SUCCESS();
    } // serialize_userInfo_ptr

    static error serialize_collInfo_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            collInfo_t* l = std::any_cast<collInfo_t*>(_p);

            if (l) {
                _out["coll_id"] = std::to_string(l->collId);
                _out["coll_name"] = l->collName;
                _out["coll_parent_name"] = l->collParentName;
                _out["coll_owner_name"] = l->collOwnerName;
                _out["coll_owner_zone"] = l->collOwnerZone;
                _out["coll_map_id"] = std::to_string(l->collMapId);
                _out["coll_access_index"] = std::to_string(l->collAccessInx);
                _out["coll_comments"] = l->collComments;
                _out["coll_inheritance"] = l->collInheritance;
                _out["coll_expiry"] = l->collExpiry;
                _out["coll_create"] = l->collCreate;
                _out["coll_modify"] = l->collModify;
                _out["coll_access"] = l->collAccess;
                _out["coll_type"] = l->collType;
                _out["coll_info1"] = l->collInfo1;
                _out["coll_info2"] = l->collInfo2;

                serialize_keyValPair(l->condInput, _out);

            } else {
                _out["collInfo_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast collInfo ptr" );
        }

        return SUCCESS();
    } // serialize_collInfo_ptr

    static error serialize_collInp_ptr(
            std::any                _p,
            serialized_parameter_t& _out) { 
        try {
            collInp_t* l = std::any_cast<collInp_t*>(_p);

            if (l) {
                _out["coll_name"] = l->collName;
                _out["flags"] = l->flags;
                _out["opr_type"] = l->oprType;

                serialize_keyValPair(l->condInput, _out);

            } else {
                _out["collInp_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast collInp ptr" );
        }

        return SUCCESS();
    } // serialize_collInp_ptr

    static error serialize_modAVUMetaInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            modAVUMetadataInp_t* l = std::any_cast<modAVUMetadataInp_t*>(_p);

            if (l) {
                if( l->arg0 ) { _out["arg0"] = l->arg0; }
                if( l->arg1 ) { _out["arg1"] = l->arg1; }
                if( l->arg2 ) { _out["arg2"] = l->arg2; }
                if( l->arg3 ) { _out["arg3"] = l->arg3; }
                if( l->arg4 ) { _out["arg4"] = l->arg4; }
                if( l->arg5 ) { _out["arg5"] = l->arg5; }
                if( l->arg6 ) { _out["arg6"] = l->arg6; }
                if( l->arg7 ) { _out["arg7"] = l->arg7; }
                if( l->arg8 ) { _out["arg8"] = l->arg8; }
                if( l->arg9 ) { _out["arg9"] = l->arg9; }
            } else {
                _out["modAVUMetaInp_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast modAVUMetaInp ptr" );
        }

        return SUCCESS();
    } // serialize_modAVUMetaInp_ptr

    static error serialize_modAccessControlInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            modAccessControlInp_t* l = std::any_cast<modAccessControlInp_t*>(_p);

            if (l) {
                _out["recursive_flag"] = std::to_string(l->recursiveFlag);
                if( l->accessLevel ) { _out["access_level"] = l->accessLevel; }
                if( l->userName ) { _out["user_name"] = l->userName; }
                if( l->zone ) { _out["zone"] = l->zone; }
                if( l->path ) { _out["path"] = l->path; }
            } else {
                _out["modAccessControlInp_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast modAccessControlInp ptr" );
        }

        return SUCCESS();
    } // serialize_modAccessControlInp_ptr

    static error serialize_modDataObjMeta_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            modDataObjMeta_t* l = std::any_cast<modDataObjMeta_t*>(_p);

            if (l) {
                error ret = serialize_dataObjInfo_ptr(
                        l->dataObjInfo,
                        _out);
                if(!ret.ok()) {
                    irods::log(PASS(ret));
                }

                if( l->regParam ) {
                    for(int i = 0; i < l->regParam->len; ++i) {
                        _out[l->regParam->keyWord[i]] = l->regParam->value[i];
                    }
                }
            } else {
                _out["modDataObjMeta_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast modDataObjMeta ptr" );
        }

        return SUCCESS();
    } // serialize_modDataObjMeta_ptr

    static error serialize_ruleExecSubmitInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            ruleExecSubmitInp_t* l = std::any_cast<ruleExecSubmitInp_t*>(_p);

            if (l) {
                _out["rule_name"] = l->ruleName; 
                _out["rei_file_path"] = l->reiFilePath; 
                _out["user_name"] = l->userName; 
                _out["exe_address"] = l->exeAddress; 
                _out["exe_time"] = l->exeTime; 
                _out["exe_frequency"] = l->exeFrequency; 
                _out["priority"] = l->priority; 
                _out["last_exec_time"] = l->lastExecTime; 
                _out["exe_status"] = l->exeStatus; 
                _out["estimate_exe_time"] = l->estimateExeTime; 
                _out["notification_addr"] = l->notificationAddr; 
                _out["rule_exec_id"] = l->ruleExecId; 

                for(int i = 0; i < l->condInput.len; ++i) {
                    _out[l->condInput.keyWord[i]] = l->condInput.value[i];
                }
            } else {
                _out["ruleExecSubmitInp_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                    INVALID_ANY_CAST,
                    "failed to cast ruleExecSubmitInp ptr" );
        }

        return SUCCESS();
    } // serialize_ruleExecSubmitInp_ptr

    static error serialize_dataObjCopyInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            dataObjCopyInp_t* l = std::any_cast<dataObjCopyInp_t*>(_p);

            if (l) {
                serialized_parameter_t src;
                error ret = serialize_dataObjInp_ptr(
                        &l->srcDataObjInp,
                        src );
                if(!ret.ok()) {
                    irods::log(PASS(ret));
                }
                else {
                    for( auto p : src ) {
                        _out["src_"+p.first] = p.second;
                    }
                }


                serialized_parameter_t dst;
                ret = serialize_dataObjInp_ptr(
                        &l->destDataObjInp,
                        dst );
                if(!ret.ok()) {
                    irods::log(PASS(ret));
                }
                else {
                    for( auto p : dst ) {
                        _out["dst_"+p.first] = p.second;
                    }
                }
            } else {
                _out["dataObjCopyInp_ptr"] = "nullptr";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast dataObjCopyInp ptr" );
        }

        return SUCCESS();
    } // serialize_dataObjCopyInp_ptr

    static error serialize_rodsObjStat_ptr_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            rodsObjStat_t** tmp = std::any_cast<rodsObjStat_t**>(_p);
            if(tmp && *tmp) {
                rodsObjStat_t* l = *tmp;

                _out["obj_size"] = std::to_string(l->objSize);
                _out["obj_type"] = std::to_string(static_cast<int>(l->objType));
                _out["data_mode"] = std::to_string(l->dataMode);
                
                _out["data_id"] = l->dataId;
                _out["checksum"] = l->chksum;
                _out["ownerName"] = l->ownerName;
                _out["owner_zone"] = l->ownerZone;
                _out["create_time"] = l->createTime;
                _out["modify_time"] = l->modifyTime;
                _out["resc_hier"] = l->rescHier;

                if(l->specColl) {
                    serialize_spec_coll_info_ptr(
                        l->specColl,
                        _out );
                }
            }
            else {
                _out["null_value"] = "null_value";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast rodsObjStat ptr ptr" );
        }

        return SUCCESS();
    } // serialize_rodsObjStat_ptr_ptr

    static error serialize_rodsObjStat_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            rodsObjStat_t* tmp = std::any_cast<rodsObjStat_t*>(_p);

            if (tmp) {
                _out["obj_size"] = std::to_string(tmp->objSize);
                _out["obj_type"] = std::to_string(static_cast<int>(tmp->objType));
                _out["data_mode"] = std::to_string(tmp->dataMode);

                _out["data_id"] = tmp->dataId;
                _out["checksum"] = tmp->chksum;
                _out["ownerName"] = tmp->ownerName;
                _out["owner_zone"] = tmp->ownerZone;
                _out["create_time"] = tmp->createTime;
                _out["modify_time"] = tmp->modifyTime;
                _out["resc_hier"] = tmp->rescHier;

                if (tmp->specColl) {
                    serialize_spec_coll_info_ptr(tmp->specColl, _out);
                }
            }
            else {
                _out["null_value"] = "null_value";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast rodsObjStat ptr" );
        }

        return SUCCESS();
    } // serialize_rodsObjStat_ptr

    static error serialize_genQueryInp_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            genQueryInp_t* tmp = std::any_cast<genQueryInp_t*>(_p);

            if (tmp) {
                _out["maxRows"] = std::to_string(tmp->maxRows);
                _out["continueInx"] = std::to_string(tmp->continueInx);
                _out["rowOffset"] = std::to_string(tmp->rowOffset);
                _out["options"] = std::to_string(tmp->options);

                for (int i = 0; i < tmp->condInput.len; ++i) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    _out[tmp->condInput.keyWord[i]] = tmp->condInput.value[i];
                }

                for (int i = 0; i < tmp->selectInp.len; ++i) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    std::string index = std::to_string(tmp->selectInp.inx[i]);

                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    std::string value = std::to_string(tmp->selectInp.value[i]);
                    _out["select_" + index] = value;
                }

                for (int i = 0; i < tmp->sqlCondInp.len; ++i) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    std::string index = std::to_string(tmp->sqlCondInp.inx[i]);

                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    _out["where_" + index] = tmp->sqlCondInp.value[i];
                }
            }
            else {
                _out["null_value"] = "null_value";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast genQueryInp ptr" );
        }

        return SUCCESS();
    } // serialize_genQueryInp_ptr

    static error serialize_genQueryOut_ptr(
           std::any                    _p,
           serialized_parameter_t&  _out) {
        try {
            genQueryOut_t* tmp = std::any_cast<genQueryOut_t*>(_p);

            if (tmp) {
                _out["rowCnt"] = std::to_string(tmp->rowCnt);
                _out["attriCnt"] = std::to_string(tmp->attriCnt);
                _out["continueInx"] = std::to_string(tmp->continueInx);
                _out["totalRowCount"] = std::to_string(tmp->totalRowCount);

                for (int i = 0; i < tmp->attriCnt; ++i) {
                    for (int j = 0; j < tmp->rowCnt; ++j) {
                        std::string i_str = std::to_string(i);
                        std::string j_str = std::to_string(j);
                        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
                        _out["attriInx_" + i_str] = std::to_string(tmp->sqlResult[i].attriInx);
                        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
                        _out["len_" + i_str] = std::to_string(tmp->sqlResult[i].len);

                        // clang-format off
                        // NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result, cppcoreguidelines-pro-bounds-constant-array-index, cppcoreguidelines-pro-bounds-pointer-arithmetic)
                        _out[fmt::format("value_{}_{}", j_str, i_str)] = tmp->sqlResult[i].value + j * tmp->sqlResult[i].len;
                        // clang-format on
                    }
                }
            }
            else {
                _out["null_value"] = "null_value";
            }
        } catch ( std::exception& ) {
            return ERROR(
                    INVALID_ANY_CAST,
                    "failed to cast genQueryOut ptr" );
        }

        return SUCCESS();
    } //serialize_genQueryOut_ptr


    static error serialize_char_ptr_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            char** tmp = std::any_cast<char**>(_p);
            if(tmp && *tmp ) {
                _out["value"] = *tmp;
            }
            else {
                _out["null_value"] = "null_value";
            }
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast char ptr ptr" );
        }

        return SUCCESS();
    } // serialize_char_ptr_ptr


    static irods::error serialize_openedDataObjInp_ptr(std::any _p,
                                                       serialized_parameter_t& _out) {
        try {
            openedDataObjInp_t *ptr = std::any_cast<openedDataObjInp_t*>(_p);

            // for a valid ptr, we serialize the immediate properties + keyValPair
            if (ptr) {
                _out["l1descInx"] = std::to_string(ptr->l1descInx);
                _out["len"] = std::to_string(ptr->len);
                _out["whence"] = std::to_string(ptr->whence);
                _out["oprType"] = std::to_string(ptr->oprType);
                _out["offset"] = std::to_string(ptr->offset);
                _out["bytesWritten"] = std::to_string(ptr->bytesWritten);

                serialize_keyValPair(ptr->condInput, _out);
            }
            else {
                _out["null_value"] = "null_value";
            }
        }
        catch (std::exception&) {
            return ERROR(
                INVALID_ANY_CAST,
                "failed to cast openedDataObjInp_t ptr");
        }

        return SUCCESS();
    } // serialize_openedDataObjInp_ptr


    static irods::error serialize_openedDataObjInp_ptr_ptr(std::any _p,
                                                           serialized_parameter_t& _out)
    {
        try {
            openedDataObjInp_t **ptr = std::any_cast<openedDataObjInp_t**>(_p);

            // if we can, dereference and serialize
            if (ptr && *ptr) {
                serialize_openedDataObjInp_ptr(*ptr, _out);
            }
            else {
                _out["null_value"] = "null_value";
            }
        }
        catch (std::exception&) {
            return ERROR(
                INVALID_ANY_CAST,
                "failed to cast openedDataObjInp_t ptr ptr");
        }

        return SUCCESS();
    } // serialize_openedDataObjInp_ptr_ptr


    static irods::error serialize_bytesBuf_ptr(std::any _p,
                                               serialized_parameter_t& _out)
    {
        try {
            bytesBuf_t *ptr = std::any_cast<bytesBuf_t*>(_p);

            // we serialize the length and content (nonprintable escaped)
            if (ptr) {
                _out["len"] = std::to_string(ptr->len);
                std::string &str = _out["buf"];

                for (int i = 0; i < ptr->len; i++)
                {
                    unsigned char c = *((unsigned char*)(ptr->buf) + i);

                    if (isprint(c) || isspace(c) || isblank(c)) {
                        str += c;
                    }
                    else {
                        str += fmt::format("\\x{0:02x}", c);
                    }
                }
            }
            else {
                _out["null_value"] = "null_value";
            }
        }
        catch (std::exception&) {
            return ERROR(
                INVALID_ANY_CAST,
                "failed to cast bytesBuf_t ptr");
        }

        return SUCCESS();
    } // serialize_bytesBuf_ptr

    static irods::error serialize_Genquery2Input_ptr(std::any _p, serialized_parameter_t& _out)
    {
        try {
            const auto* v = std::any_cast<Genquery2Input*>(_p);

            _out["query_string"] = v->query_string ? v->query_string : "";
            _out["zone"] = v->zone ? v->zone : "";
            _out["sql_only"] = std::to_string(v->sql_only);
            _out["column_mappings"] = std::to_string(v->column_mappings);

            return SUCCESS();
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(INVALID_ANY_CAST,
                         fmt::format("{}: failed to cast pointer to [Genquery2Input*]: {}", __func__, e.what()));
        }
        catch (const std::exception& e) {
            return ERROR(
                SYS_LIBRARY_ERROR, fmt::format("{}: failed to serialize [Genquery2Input*]: {}", __func__, e.what()));
        }
    } // serialize_Genquery2Input_ptr

    static void serialize_vector_of_strings_ptr_impl(const std::vector<std::string>& _v, serialized_parameter_t& _out)
    {
        _out["size"] = std::to_string(_v.size());

        for (auto&& e : _v | boost::adaptors::indexed()) {
            log_re::debug("{}: element = {{index: {}, value: {}}}", __func__, e.index(), e.value());
            _out[std::to_string(e.index())] = e.value();
        }
    } // serialize_vector_of_strings_ptr_impl

    // Serializes a const vector of strings.
    //
    // Let *v represent the vector of strings in the NREP.
    // To get the number of elements in the vector, use: *v."size". The count will be of type string.
    // To get an element via its index, use: *v."<index>". The value will be of type string.
    static irods::error serialize_const_vector_of_strings_ptr(std::any _p, serialized_parameter_t& _out)
    {
        try {
            log_re::trace(__func__);
            serialize_vector_of_strings_ptr_impl(*std::any_cast<const std::vector<std::string>*>(_p), _out);
            return SUCCESS();
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(
                INVALID_ANY_CAST,
                fmt::format("{}: failed to cast pointer to [const std::vector<std::string>*]: {}", __func__, e.what()));
        }
        catch (const std::exception& e) {
            return ERROR(
                SYS_LIBRARY_ERROR,
                fmt::format("{}: failed to serialize [const std::vector<std::string>*]: {}", __func__, e.what()));
        }
    } // serialize_const_vector_of_strings_ptr

    // Serializes a (non-const) vector of strings.
    //
    // Let *v represent the vector of strings in the NREP.
    // To get the number of elements in the vector, use: *v."size". The count will be of type string.
    // To get an element via its index, use: *v."<index>". The value will be of type string.
    static irods::error serialize_vector_of_strings_ptr(std::any _p, serialized_parameter_t& _out)
    {
        try {
            log_re::trace(__func__);
            serialize_vector_of_strings_ptr_impl(*std::any_cast<std::vector<std::string>*>(_p), _out);
            return SUCCESS();
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(
                INVALID_ANY_CAST,
                fmt::format("{}: failed to cast pointer to [std::vector<std::string>*]: {}", __func__, e.what()));
        }
        catch (const std::exception& e) {
            return ERROR(SYS_LIBRARY_ERROR,
                         fmt::format("{}: failed to serialize [std::vector<std::string>*]: {}", __func__, e.what()));
        }
    } // serialize_vector_of_strings_ptr

    static irods::error serialize_execMyRuleInp_ptr(std::any _p, serialized_parameter_t& _out)
    {
        try {
            log_re::trace(__func__);
            const auto* casted_ptr = std::any_cast<execMyRuleInp_t*>(_p);
            if (!casted_ptr) {
                _out["execMyRuleInp_ptr"] = "nullptr";
                return SUCCESS();
            }
            _out["myRule"] = casted_ptr->myRule;
            _out["addr_hostAddr"] = casted_ptr->addr.hostAddr;
            _out["addr_zoneName"] = casted_ptr->addr.zoneName;
            _out["addr_portNum"] = std::to_string(casted_ptr->addr.portNum);
            _out["addr_dummyInt"] = std::to_string(casted_ptr->addr.dummyInt);
            serialize_keyValPair(casted_ptr->condInput, _out);
            _out["outParamDesc"] = casted_ptr->outParamDesc;

            const auto* msparamarray_ptr = casted_ptr->inpParamArray;
            if (!msparamarray_ptr) {
                _out["inpParamArray"] = "nullptr";
                return SUCCESS();
            }
            _out["inpParamArray_len"] = std::to_string(msparamarray_ptr->len);
            _out["inpParamArray_oprType"] = std::to_string(msparamarray_ptr->oprType);
            for (int i = 0; i < msparamarray_ptr->len; i++) {
                std::string prefix = fmt::format("inpParamArray_{}_", i);
                const auto* lbl = msparamarray_ptr->msParam[i]->label;
                const auto* typ = msparamarray_ptr->msParam[i]->type;
                _out[prefix + "label"] = lbl ? lbl : "nullptr";
                _out[prefix + "type"] = typ ? typ : "nullptr";
                const auto* msparam_ptr = msparamarray_ptr->msParam[i];

                if (msparam_ptr == nullptr || msparam_ptr->inOutStruct == nullptr || !typ) {
                    _out[prefix + "inOutStruct"] = "nullptr";
                }
                else if (strcmp(msparam_ptr->type, STR_MS_T) == 0) {
                    _out[prefix + "inOutStruct"] = static_cast<char*>(msparam_ptr->inOutStruct);
                }
                else if (strcmp(msparam_ptr->type, INT_MS_T) == 0) {
                    _out[prefix + "inOutStruct"] = std::to_string(parseMspForPosInt(const_cast<MsParam*>(msparam_ptr)));
                }
                else if (strcmp(msparam_ptr->type, DOUBLE_MS_T) == 0) {
                    double out = 0.0;
                    if (parseMspForDouble(const_cast<MsParam*>(msparam_ptr), &out)) {
                        THROW(SYS_INVALID_INPUT_PARAM, "Failed to convert microservice argument to float.");
                    }
                    _out[prefix + "inOutStruct"] = std::to_string(out);
                }
                else if (strcmp(msparam_ptr->type, FLOAT_MS_T) == 0) {
                    float out = 0.0;
                    if (parseMspForFloat(const_cast<MsParam*>(msparam_ptr), &out)) {
                        THROW(SYS_INVALID_INPUT_PARAM, "Failed to convert microservice argument to float.");
                    }
                    _out[prefix + "inOutStruct"] = std::to_string(out);
                }
                else {
                    _out[prefix + "inOutStruct"] = fmt::format("unrepresentable_type_{}", msparam_ptr->type);
                }
                const auto* ptr = msparamarray_ptr->msParam[i]->inpOutBuf;
                if (ptr) {
                    _out[prefix + "inpOutBuf_len"] = std::to_string(ptr->len);
                    std::string& str = _out[prefix + "inpOutBuf_buf"];

                    for (int j = 0; j < ptr->len; j++) {
                        unsigned char c = *(static_cast<unsigned char*>(ptr->buf) + j);

                        if (isprint(c) || isspace(c) || isblank(c)) {
                            str += c;
                        }
                        else {
                            str += fmt::format("\\x{0:02x}", c);
                        }
                    }
                }
                else {
                    _out[prefix + "null_value"] = "null_value";
                }
            }
            return SUCCESS();
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(INVALID_ANY_CAST,
                         fmt::format("{}: failed to cast pointer to [execMyRuleInp_t*]: {}", __func__, e.what()));
        }
        catch (const irods::exception& e) {
            return ERROR(
                e.code(),
                fmt::format(
                    "{}: failed to cast pointer to [execMyRuleInp_t*]: {}, ec: {}", __func__, e.what(), e.code()));
        }
        catch (const std::exception& e) {
            return ERROR(
                SYS_LIBRARY_ERROR, fmt::format("{}: failed to serialize [execMyRuleInp_t*]: {}", __func__, e.what()));
        }
    } // serialize_execMyRuleInp_ptr

    static irods::error serialize_structFileExtAndRegInp_ptr(std::any _p, serialized_parameter_t& _out)
    {
        try {
            log_re::trace(__func__);
            const auto* casted_ptr = std::any_cast<structFileExtAndRegInp_t*>(_p);
            _out["obj_path"] = casted_ptr->objPath;
            _out["collection_path"] = casted_ptr->collection;
            _out["opr_type"] = std::to_string(casted_ptr->oprType);
            _out["flags"] = std::to_string(casted_ptr->flags);
            serialize_keyValPair(casted_ptr->condInput, _out);
            return SUCCESS();
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(
                INVALID_ANY_CAST,
                fmt::format("{}: failed to cast pointer to [structFileExtAndRegInp_t*]: {}", __func__, e.what()));
        }
        catch (const std::exception& e) {
            return ERROR(SYS_LIBRARY_ERROR,
                         fmt::format("{}: failed to serialize [structFileExtAndRegInp_t*]: {}", __func__, e.what()));
        }
    }

    static irods::error serialize_msParamArray_ptr_ptr(std::any _p, serialized_parameter_t& _out)
    {
        try {
            log_re::trace(__func__);
            const auto* const* casted_ptr = std::any_cast<msParamArray_t**>(_p);
            if (!casted_ptr) {
                _out["msParamArray_ptr_ptr"] = "nullptr";
                return SUCCESS();
            }
            if (!*casted_ptr) {
                _out["msParamArray_ptr"] = "nullptr";
                return SUCCESS();
            }
            const auto* derefed_ptr = *casted_ptr;
            _out["len"] = std::to_string(derefed_ptr->len);
            _out["oprType"] = std::to_string(derefed_ptr->oprType);
            for (int i = 0; i < derefed_ptr->len; i++) {
                std::string prefix = std::to_string(i) + "_";
                const auto* lbl = derefed_ptr->msParam[i]->label;
                const auto* typ = derefed_ptr->msParam[i]->type;
                _out[prefix + "label"] = lbl ? lbl : "nullptr";
                _out[prefix + "type"] = typ ? typ : "nullptr";
                const auto* msparam_ptr = derefed_ptr->msParam[i];

                if (msparam_ptr == nullptr || msparam_ptr->inOutStruct == nullptr || !typ) {
                    _out[prefix + "inOutStruct"] = "nullptr";
                }
                else if (strcmp(msparam_ptr->type, STR_MS_T) == 0) {
                    _out[prefix + "inOutStruct"] = static_cast<char*>(msparam_ptr->inOutStruct);
                }
                else if (strcmp(msparam_ptr->type, INT_MS_T) == 0) {
                    _out[prefix + "inOutStruct"] = std::to_string(parseMspForPosInt(const_cast<MsParam*>(msparam_ptr)));
                }
                else if (strcmp(msparam_ptr->type, DOUBLE_MS_T) == 0) {
                    double out = 0.0;
                    if (parseMspForDouble(const_cast<MsParam*>(msparam_ptr), &out)) {
                        THROW(SYS_INVALID_INPUT_PARAM, "Failed to convert microservice argument to float.");
                    }
                    _out[prefix + "inOutStruct"] = std::to_string(out);
                }
                else if (strcmp(msparam_ptr->type, FLOAT_MS_T) == 0) {
                    float out = 0.0;
                    if (parseMspForFloat(const_cast<MsParam*>(msparam_ptr), &out)) {
                        THROW(SYS_INVALID_INPUT_PARAM, "Failed to convert microservice argument to float.");
                    }
                    _out[prefix + "inOutStruct"] = std::to_string(out);
                }
                else {
                    _out[prefix + "inOutStruct"] = fmt::format("unrepresentable_type_{}", msparam_ptr->type);
                }

                const auto* ptr = derefed_ptr->msParam[i]->inpOutBuf;
                if (ptr) {
                    _out[prefix + "inpOutBuf_len"] = std::to_string(ptr->len);
                    std::string& str = _out[prefix + "inpOutBuf_buf"];

                    for (int j = 0; j < ptr->len; j++) {
                        unsigned char c = *(static_cast<unsigned char*>(ptr->buf) + j);

                        if (isprint(c) || isspace(c)) {
                            str += c;
                        }
                        else {
                            str += fmt::format("\\x{0:02x}", c);
                        }
                    }
                }
                else {
                    _out[prefix + "null_value"] = "null_value";
                }
            }
            return SUCCESS();
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(INVALID_ANY_CAST,
                         fmt::format("{}: failed to cast pointer to [msParamArray_t**]: {}", __func__, e.what()));
        }
        catch (const irods::exception& e) {
            return ERROR(
                e.code(),
                fmt::format(
                    "{}: failed to cast pointer to [msParamArray_t**]: {}, ec: {}", __func__, e.what(), e.code()));
        }
        catch (const std::exception& e) {
            return ERROR(
                SYS_LIBRARY_ERROR, fmt::format("{}: failed to serialize [msParamArray_t**]: {}", __func__, e.what()));
        }
    } // serialize_msParamArray_ptr_ptr

    // NOLINTNEXTLINE(misc-use-anonymous-namespace)
    static error serialize_ticketAdminInp_ptr(std::any _p, serialized_parameter_t& _out)
    {
        try {
            const TicketAdminInput* inp = std::any_cast<TicketAdminInput*>(_p);

            if (inp) {
                _out["arg1"] = inp->arg1 ? inp->arg1 : "";
                _out["arg2"] = inp->arg2 ? inp->arg2 : "";
                _out["arg3"] = inp->arg3 ? inp->arg3 : "";
                _out["arg4"] = inp->arg4 ? inp->arg4 : "";
                _out["arg5"] = inp->arg5 ? inp->arg5 : "";
                _out["arg6"] = inp->arg6 ? inp->arg6 : "";

                serialize_keyValPair(inp->condInput, _out);
            }
            else {
                _out["ticketAdminInp_ptr"] = "nullptr";
            }
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(INVALID_ANY_CAST,
                         fmt::format("{}: failed to cast pointer to [TicketAdminInput*]: {}", __func__, e.what()));
        }
        catch (const std::exception& e) {
            return ERROR(
                SYS_LIBRARY_ERROR, fmt::format("{}: failed to serialize [TicketAdminInput*]: {}", __func__, e.what()));
        }

        return SUCCESS();
    } // serialize_ticketAdminInp_ptr

    static error serialize_ptr_to_vector_of_tuples_of_std_string_four_int64s_and_std_string(
        std::any _p,
        serialized_parameter_t& _out)
    {
        try {
            const irods::logical_quotas::quota_vector* inp = std::any_cast<irods::logical_quotas::quota_vector*>(_p);
            if (inp) {
                _out["size"] = std::to_string(inp->size());

                // TODO(#8941): Params with one key do not serialize correctly.
                // Must be here, or fails in the empty case
                _out["_size"] = std::to_string(inp->size());

                for (size_t i = 0; i < inp->size(); i++) {
                    irods::logical_quotas::quota_vector::value_type tup = inp->at(i);
                    _out[std::to_string(i) + ":string_1"] = std::get<0>(tup);
                    _out[std::to_string(i) + ":int_1"] = std::to_string(std::get<1>(tup));
                    _out[std::to_string(i) + ":int_2"] = std::to_string(std::get<2>(tup));
                    _out[std::to_string(i) + ":int_3"] = std::to_string(std::get<3>(tup));
                    _out[std::to_string(i) + ":int_4"] = std::to_string(std::get<4>(tup));
                    _out[std::to_string(i) + ":string_2"] = std::get<5>(tup);
                }
            }
            else {
                _out["vector_ptr"] = "nullptr";
            }
        }
        catch (const std::bad_any_cast& e) {
            return ERROR(INVALID_ANY_CAST,
                         fmt::format("{}: failed to cast pointer to [std::vector<std::tuple<std::string, std::int64_t, "
                                     "std::int64_t, std::int64_t, std::string>>*]: {}",
                                     __func__,
                                     e.what()));
        }
        catch (const std::exception& e) {
            return ERROR(SYS_LIBRARY_ERROR,
                         fmt::format("{}: failed to serialize [std::vector<std::tuple<std::string, std::int64_t, "
                                     "std::int64_t, std::int64_t, std::string>>*]: {}",
                                     __func__,
                                     e.what()));
        }

        return SUCCESS();
    }

#if 0
    static error serialize_XXXX_ptr(
            std::any                 _p,
            serialized_parameter_t& _out) { 
        try {
            XXXX_t* l = std::any_cast<XXXX_t*>(_p);
        }
        catch ( std::exception& ) {
            return ERROR(
                     INVALID_ANY_CAST,
                     "failed to cast XXXX ptr" );
        }

        return SUCCESS();
    } // serialize_XXXX_ptr
#endif

    serialization_map_t& get_serialization_map() {
        static serialization_map_t the_map{
            {std::type_index(typeid(float*)), serialize_float_ptr},
            {std::type_index(typeid(const std::string*)), serialize_const_std_string_ptr},
            {std::type_index(typeid(std::string*)), serialize_std_string_ptr},
            {std::type_index(typeid(std::string)), serialize_std_string},
            {std::type_index(typeid(hierarchy_parser*)), serialize_hierarchy_parser_ptr},
            {std::type_index(typeid(rodsLong_t)), serialize_rodslong},
            {std::type_index(typeid(rodsLong_t*)), serialize_rodslong_ptr},
            {std::type_index(typeid(size_t)), serialize_sizet},
            {std::type_index(typeid(int)), serialize_int},
            {std::type_index(typeid(int*)), serialize_int_ptr},
            {std::type_index(typeid(char*)), serialize_char_ptr},
            {std::type_index(typeid(const char*)), serialize_const_char_ptr},
            {std::type_index(typeid(rsComm_t*)), serialize_rsComm_ptr},
            {std::type_index(typeid(plugin_context)), serialize_plugin_context},
            {std::type_index(typeid(dataObjInp_t*)), serialize_dataObjInp_ptr},
            {std::type_index(typeid(bulkOprInp_t*)), serialize_bulkOprInp_ptr},
            {std::type_index(typeid(authResponseInp_t*)), serialize_authResponseInp_ptr},
            {std::type_index(typeid(dataObjInfo_t*)), serialize_dataObjInfo_ptr},
            {std::type_index(typeid(keyValPair_t*)), serialize_keyValPair_ptr},
            {std::type_index(typeid(userInfo_t*)), serialize_userInfo_ptr},
            {std::type_index(typeid(collInfo_t*)), serialize_collInfo_ptr},
            {std::type_index(typeid(collInp_t*)), serialize_collInp_ptr},
            {std::type_index(typeid(modAVUMetadataInp_t*)), serialize_modAVUMetaInp_ptr},
            {std::type_index(typeid(modAccessControlInp_t*)), serialize_modAccessControlInp_ptr},
            {std::type_index(typeid(modDataObjMeta_t*)), serialize_modDataObjMeta_ptr},
            {std::type_index(typeid(ruleExecSubmitInp_t*)), serialize_ruleExecSubmitInp_ptr},
            {std::type_index(typeid(dataObjCopyInp_t*)), serialize_dataObjCopyInp_ptr},
            {std::type_index(typeid(rodsObjStat_t**)), serialize_rodsObjStat_ptr_ptr},
            {std::type_index(typeid(rodsObjStat_t*)), serialize_rodsObjStat_ptr},
            {std::type_index(typeid(genQueryInp_t*)), serialize_genQueryInp_ptr},
            {std::type_index(typeid(genQueryOut_t*)), serialize_genQueryOut_ptr},
            {std::type_index(typeid(char**)), serialize_char_ptr_ptr},
            {std::type_index(typeid(openedDataObjInp_t*)), serialize_openedDataObjInp_ptr},
            {std::type_index(typeid(openedDataObjInp_t**)), serialize_openedDataObjInp_ptr_ptr},
            {std::type_index(typeid(bytesBuf_t*)), serialize_bytesBuf_ptr},
            {std::type_index(typeid(Genquery2Input*)), serialize_Genquery2Input_ptr},
            {std::type_index(typeid(const std::vector<std::string>*)), serialize_const_vector_of_strings_ptr},
            {std::type_index(typeid(std::vector<std::string>*)), serialize_vector_of_strings_ptr},
            {std::type_index(typeid(execMyRuleInp_t*)), serialize_execMyRuleInp_ptr},
            {std::type_index(typeid(structFileExtAndRegInp_t*)), serialize_structFileExtAndRegInp_ptr},
            {std::type_index(typeid(msParamArray_t**)), serialize_msParamArray_ptr_ptr},
            {std::type_index(typeid(TicketAdminInput*)), serialize_ticketAdminInp_ptr},
            {std::type_index(typeid(irods::logical_quotas::quota_vector*)),
             serialize_ptr_to_vector_of_tuples_of_std_string_four_int64s_and_std_string}};
        return the_map;
    } // get_serialization_map

    error add_operation(
        const index_t& _index,
        operation_t    _operation ) {

        serialization_map_t& the_map = get_serialization_map();
        if(the_map.find(_index) != the_map.end() ) {
            return ERROR(
                       KEY_NOT_FOUND,
                       "type_index exists");
        }

        the_map[ _index ] = _operation;

        return SUCCESS();

    } // add_operation

    static std::string demangle(const char* name) {
        int status = -4; // some arbitrary value to eliminate the compiler warning
        std::unique_ptr<char, void(*)(void*)> res {
            abi::__cxa_demangle(name, NULL, NULL, &status),
                std::free
        };
        return (status==0) ? res.get() : name ;
    }

    error serialize_parameter(
        std::any                 _in_param,
        serialized_parameter_t&  _out_param ) {
        index_t idx = std::type_index(_in_param.type());
        serialization_map_t& the_map = get_serialization_map();
        if(the_map.find(idx) == the_map.end() ) {
            std::string err = "[";
            err += demangle( _in_param.type().name() );
            err += "] not supported";
            _out_param["ERROR"] = err;
            return SUCCESS();
        }

        return the_map[idx](_in_param, _out_param);

    } // serialize_parameter
} // namespace irods::re_serialization
