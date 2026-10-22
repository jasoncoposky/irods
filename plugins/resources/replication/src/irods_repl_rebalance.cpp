#include "irods/private/irods_repl_rebalance.hpp"
#include "irods/irods_exception.hpp"
#include "irods/irods_resource_plugin.hpp"
#include "irods/irods_file_object.hpp"
#include "irods/irods_hierarchy_parser.hpp"
#include "irods/irods_virtual_path.hpp"
#include "irods/private/irods_repl_retry.hpp"
#include "irods/private/irods_repl_types.hpp"
#include "irods/icatHighLevelRoutines.hpp"
#include "irods/dataObjRepl.h"
#include "irods/filesystem/path.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/rodsLog.h"
#include "irods/rodsType.h"
#include "irods/rs_genquery2.hpp"
#include "irods/rodsError.h"

#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <charconv>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <vector>

namespace {
    irods::error repl_for_rebalance(
        irods::plugin_context& _ctx,
        const std::string& _obj_path,
        const std::string& _current_resc,
        const std::string& _src_hier,
        const std::string& _dst_hier,
        const std::string& _src_resc,
        const std::string& _dst_resc,
        const int          _mode ) {
        // =-=-=-=-=-=-=-
        // generate a resource hierarchy that ends at this resource for pdmo
        irods::hierarchy_parser parser;
        parser.set_string( _src_hier );
        std::string sub_hier;
        parser.str( sub_hier, _current_resc );

        dataObjInp_t data_obj_inp{};
        const irods::at_scope_exit clear_cond{[&data_obj_inp] { clearKeyVal(&data_obj_inp.condInput); }};
        rstrcpy( data_obj_inp.objPath, _obj_path.c_str(), MAX_NAME_LEN );
        data_obj_inp.createMode = _mode;
        addKeyVal( &data_obj_inp.condInput, RESC_HIER_STR_KW,      _src_hier.c_str() );
        addKeyVal( &data_obj_inp.condInput, DEST_RESC_HIER_STR_KW, _dst_hier.c_str() );
        addKeyVal( &data_obj_inp.condInput, RESC_NAME_KW,          _src_resc.c_str() );
        addKeyVal( &data_obj_inp.condInput, DEST_RESC_NAME_KW,     _dst_resc.c_str() );
        addKeyVal( &data_obj_inp.condInput, IN_PDMO_KW,             sub_hier.c_str() );
        addKeyVal( &data_obj_inp.condInput, ADMIN_KW,              "" );

        try {
            // =-=-=-=-=-=-=-
            // process the actual call for replication
            const auto status = data_obj_repl_with_retry( _ctx, data_obj_inp );
            if ( status < 0 ) {
                return ERROR( status,
                              boost::format( "Failed to replicate the data object [%s]" ) %
                              _obj_path );
            }
        }
        catch( const irods::exception& e ) {
            irods::log(e);
            return irods::error( e );
        }

        return SUCCESS();
    }

    auto flatten_leaf_bundles_to_strings(const std::vector<leaf_bundle_t>& _bundles) -> std::vector<std::string>
    {
        std::vector<std::string> resc_ids;
        for (const auto& b : _bundles) {
            for (const auto id : b) {
                resc_ids.push_back(fmt::to_string(id));
            }
        }
        return resc_ids;
    }

    struct ReplicationSourceInfo {
        std::string object_path;
        std::string resource_hierarchy;
        int data_mode;
    };

    // throws irods::exception
    ReplicationSourceInfo get_source_data_object_attributes(
        rsComm_t* _comm,
        const rodsLong_t _data_id,
        const std::vector<leaf_bundle_t>& _leaf_bundles) {

        if (!_comm) {
            THROW(SYS_INTERNAL_NULL_INPUT_ERR, "null comm ptr");
        }

        namespace fs = irods::filesystem;
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto resc_ids = flatten_leaf_bundles_to_strings(_leaf_bundles);
        const auto q = gq2::builder::select({"DATA_NAME", "COLL_NAME", "DATA_MODE", "DATA_RESC_ID"})
            .where(col("DATA_ID") == fmt::to_string(_data_id) &&
                   col("DATA_RESC_ID").in(resc_ids) &&
                   col("DATA_REPL_STATUS") == fmt::to_string(GOOD_REPLICA))
            .limit(1)
            .build();

        nlohmann::json rows;
        if (const int ec = gq2::try_execute(*_comm, q, rows); ec < 0) {
            THROW(ec, fmt::format("GenQuery2 failed for data_id [{}]: [{}]", _data_id, gq2::to_query_string(q)));
        }
        if (rows.empty()) {
            THROW(CAT_NO_ROWS_FOUND, fmt::format("No good source replica found for data_id [{}]", _data_id));
        }

        const auto& row = rows[0];
        const auto& data_name = row[0].get_ref<const std::string&>();
        const auto& coll_name = row[1].get_ref<const std::string&>();
        const auto& data_mode_str = row[2].get_ref<const std::string&>();
        const auto& resc_id_str = row[3].get_ref<const std::string&>();

        rodsLong_t resc_id = 0;
        if (const auto [ptr, ec] = std::from_chars(resc_id_str.data(), resc_id_str.data() + resc_id_str.size(), resc_id);
            ec != std::errc{} || ptr != resc_id_str.data() + resc_id_str.size()) {
            THROW(INVALID_LEXICAL_CAST, fmt::format("Failed to parse DATA_RESC_ID [{}]", resc_id_str));
        }

        // It is possible that some catalogs may contain invalid data modes (e.g. empty string, non-integer value).
        // In these cases, the system will interpret invalid strings as zero (0).
        int data_mode = 0;
        std::from_chars(data_mode_str.data(), data_mode_str.data() + data_mode_str.size(), data_mode);

        ReplicationSourceInfo ret;
        ret.object_path = (fs::path{coll_name} / data_name).string();
        irods::error err = resc_mgr.leaf_id_to_hier(resc_id, ret.resource_hierarchy);
        if (!err.ok()) {
            THROW(err.code(), fmt::format("leaf_id_to_hier failed for resc id [{}]", resc_id));
        }
        ret.data_mode = data_mode;
        return ret;
    }

    struct ReplicaAndRescId {
        rodsLong_t data_id;
        rodsLong_t replica_number;
        rodsLong_t resource_id;
    };

    // throws irods::exception
    std::vector<ReplicaAndRescId> get_out_of_date_replicas_batch(rsComm_t* _comm,
                                                                 const std::vector<leaf_bundle_t>& _bundles,
                                                                 const std::string& _invocation_timestamp,
                                                                 const int _batch_size,
                                                                 const int _offset)
    {
        if (!_comm) {
            THROW(SYS_INTERNAL_NULL_INPUT_ERR, "null rsComm");
        }
        if (_bundles.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM, "empty bundles");
        }
        if (_batch_size <= 0) {
            THROW(SYS_INVALID_INPUT_PARAM, boost::format("invalid batch size [%d]") % _batch_size);
        }
        if (_invocation_timestamp.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM, "empty invocation timestamp");
        }

        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        const auto resc_ids = flatten_leaf_bundles_to_strings(_bundles);
        const auto q = gq2::builder::select({"DATA_ID", "DATA_REPL_NUM", "DATA_RESC_ID"})
            .where(col("DATA_RESC_ID").in(resc_ids) &&
                   col("DATA_REPL_STATUS") == fmt::to_string(STALE_REPLICA) &&
                   col("DATA_MODIFY_TIME") <= _invocation_timestamp)
            .limit(static_cast<std::size_t>(_batch_size), static_cast<std::size_t>(_offset))
            .build();

        nlohmann::json rows;
        if (const int ec = gq2::try_execute(*_comm, q, rows); ec < 0) {
            if (CAT_NO_ROWS_FOUND == ec) {
                return {};
            }
            THROW(ec, fmt::format("GenQuery2 failed: [{}]", gq2::to_query_string(q)));
        }

        std::vector<ReplicaAndRescId> ret;
        ret.reserve(rows.size());
        auto parse_long = [](const std::string& _s) -> rodsLong_t {
            rodsLong_t val = 0;
            if (const auto [ptr, ec] = std::from_chars(_s.data(), _s.data() + _s.size(), val);
                ec != std::errc{} || ptr != _s.data() + _s.size()) {
                THROW(INVALID_LEXICAL_CAST, fmt::format("Failed to parse integer [{}]", _s));
            }
            return val;
        };

        for (const auto& row : rows) {
            ReplicaAndRescId repl_and_resc{};
            repl_and_resc.data_id        = parse_long(row[0].get_ref<const std::string&>());
            repl_and_resc.replica_number = parse_long(row[1].get_ref<const std::string&>());
            repl_and_resc.resource_id    = parse_long(row[2].get_ref<const std::string&>());
            ret.push_back(repl_and_resc);
        }
        return ret;
    }

    // throws irods::exception
    std::string get_child_name_that_is_ancestor_of_bundle(
        const std::string&   _resc_name,
        const leaf_bundle_t& _bundle) {
        std::string hier;
        irods::error err = resc_mgr.leaf_id_to_hier(_bundle[0], hier);
        if (!err.ok()) {
            THROW(err.code(), err.result());
        }

        irods::hierarchy_parser parse;
        err = parse.set_string(hier);
        if (!err.ok()) {
            THROW(err.code(), err.result());
        }

        std::string ret;
        err = parse.next(_resc_name, ret);
        if (!err.ok()) {
            THROW(err.code(), err.result());
        }
        return ret;
    }

    std::string leaf_bundles_to_string(
        const std::vector<leaf_bundle_t>& _leaf_bundles) {
        std::stringstream ss;
        for (auto& b : _leaf_bundles) {
            ss << '[';
            for (auto d : b) {
                ss << d << ", ";
            }
            ss << "], ";
        }
        return ss.str();
    }

    // throws irods::exception
    void proc_results_for_rebalance(irods::plugin_context& _ctx,
                                    const std::string& _parent_resc_name,
                                    const std::string& _child_resc_name,
                                    const size_t _bun_idx,
                                    const std::vector<leaf_bundle_t>& _bundles,
                                    const dist_child_result_t& _data_ids_to_replicate,
                                    int& _num_repls_to_skip)
    {
        if (!_ctx.comm()) {
            THROW(SYS_INVALID_INPUT_PARAM,
                  boost::format("null comm pointer. resource [%s]. child resource [%s]. bundle index [%d]. bundles [%s]") %
                  _parent_resc_name %
                  _child_resc_name %
                  _bun_idx %
                  leaf_bundles_to_string(_bundles));
        }

        if (_data_ids_to_replicate.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM,
                  boost::format("empty data id list. resource [%s]. child resource [%s]. bundle index [%d]. bundles [%s]") %
                  _parent_resc_name %
                  _child_resc_name %
                  _bun_idx %
                  leaf_bundles_to_string(_bundles));
        }

        //irods::file_object_ptr file_obj{boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco())};

        irods::error first_rebalance_error = SUCCESS();
        for (auto data_id_to_replicate : _data_ids_to_replicate) {
            ReplicationSourceInfo source_info;
            try {
                source_info = get_source_data_object_attributes(_ctx.comm(), data_id_to_replicate, _bundles);
            }
            catch (const irods::exception& _e) {
                if (_e.code() != CAT_NO_ROWS_FOUND) {
                    throw;
                }
                _num_repls_to_skip++;
                continue;
            }

            // create a file object so we can resolve a valid hierarchy to which to replicate
            irods::file_object_ptr f_ptr(new irods::file_object(_ctx.comm(), source_info.object_path, "", "", 0, source_info.data_mode, 0));
            // short circuit the magic re-repl
            f_ptr->in_pdmo(irods::hierarchy_parser{source_info.resource_hierarchy}.str(_parent_resc_name));

            // init the parser with the fragment of the upstream hierarchy not including the repl node as it should add itself
            const size_t pos = source_info.resource_hierarchy.find(_parent_resc_name);
            if (std::string::npos == pos) {
                THROW(SYS_INVALID_INPUT_PARAM, boost::format("missing repl name [%s] in source hier string [%s]") % _parent_resc_name % source_info.resource_hierarchy);
            }

            // Trim hierarchy up to parent because the resource adds itself later in hierarchy resolution
            std::string src_frag = irods::hierarchy_parser{source_info.resource_hierarchy}.str(_parent_resc_name);
            irods::hierarchy_parser parser{src_frag};

            // resolve the target child resource plugin
            irods::resource_ptr dst_resc;
            const irods::error err_resolve = resc_mgr.resolve(_child_resc_name, dst_resc);
            if (!err_resolve.ok()) {
                THROW(err_resolve.code(), boost::format("failed to resolve resource plugin. child resc [%s] parent resc [%s] bundle index [%d] bundles [%s] data id [%lld]. resolve message [%s]") %
                      _child_resc_name %
                      _parent_resc_name %
                      _bun_idx %
                      leaf_bundles_to_string(_bundles) %
                      data_id_to_replicate %
                      err_resolve.result());
            }

            // then we need to query the target resource and ask it to determine a dest resc hier for the repl
            std::string host_name{};
            float vote = 0.0;
            const irods::error err_vote = dst_resc->call<const std::string*, const std::string*, irods::hierarchy_parser*, float*>(
                _ctx.comm(),
                irods::RESOURCE_OP_RESOLVE_RESC_HIER,
                f_ptr,
                &irods::CREATE_OPERATION,
                &host_name,
                &parser,
                &vote );
            if (!err_vote.ok()) {
                THROW(err_resolve.code(), boost::format("failed to get dest hierarchy. child resc [%s] parent resc [%s] bundle index [%d] bundles [%s] data id [%lld]. vote message [%s]") %
                      _child_resc_name %
                      _parent_resc_name %
                      _bun_idx %
                      leaf_bundles_to_string(_bundles) %
                      data_id_to_replicate %
                      err_vote.result());
            }

            const std::string root_resc = parser.first_resc();
            const std::string dst_hier = parser.str();
            rodsLog(LOG_NOTICE, "%s: creating new replica for data id [%lld] (%s) from [%s] on [%s]", __FUNCTION__, data_id_to_replicate, source_info.object_path.c_str(), source_info.resource_hierarchy.c_str(), dst_hier.c_str());

            const irods::error err_rebalance = repl_for_rebalance(
                _ctx,
                source_info.object_path,
                _parent_resc_name,
                source_info.resource_hierarchy,
                dst_hier,
                root_resc,
                root_resc,
                source_info.data_mode);
            if (!err_rebalance.ok()) {
                if (first_rebalance_error.ok()) {
                    first_rebalance_error = err_rebalance;
                }
                rodsLog(LOG_ERROR, "%s: repl_for_rebalance failed. object path [%s] parent resc [%s] source hier [%s] dest hier [%s] root resc [%s] data mode [%d]",
                        __FUNCTION__, source_info.object_path.c_str(), _parent_resc_name.c_str(), source_info.resource_hierarchy.c_str(), dst_hier.c_str(), root_resc.c_str(), source_info.data_mode);
                irods::log(PASS(err_rebalance));
                if (_ctx.comm()->rError.len < MAX_ERROR_MESSAGES) {
                    addRErrorMsg(&_ctx.comm()->rError, err_rebalance.code(), err_rebalance.result().c_str());
                }
            }
        }

        if (!first_rebalance_error.ok()) {
            THROW(first_rebalance_error.code(),
                  boost::format("%s: repl_for_rebalance failed. child_resc [%s] parent resc [%s]. rebalance message [%s]") %
                  __FUNCTION__ %
                  _child_resc_name %
                  _parent_resc_name %
                  first_rebalance_error.result());
        }
    }
} // anonymous namespace

namespace irods {
    // throws irods::exception
    bool update_out_of_date_replicas(irods::plugin_context& _ctx,
                                     const std::vector<leaf_bundle_t>& _leaf_bundles,
                                     const int _batch_size,
                                     const std::string& _invocation_timestamp,
                                     const std::string& _resource_name)
    {
        int replicas_to_skip{};
        while (true) {
            const std::vector<ReplicaAndRescId> replicas_to_update = get_out_of_date_replicas_batch(
                _ctx.comm(), _leaf_bundles, _invocation_timestamp, _batch_size, replicas_to_skip);

            if (replicas_to_update.empty()) {
                break;
            }

            error first_error = SUCCESS();
            for (const auto& replica_to_update : replicas_to_update) {
                std::string destination_hierarchy;
                const error err_dst_hier = resc_mgr.leaf_id_to_hier(replica_to_update.resource_id, destination_hierarchy);
                if (!err_dst_hier.ok()) {
                    THROW(err_dst_hier.code(),
                          boost::format("leaf_id_to_hier failed. data id [%lld]. replica number [%d] resource id [%lld]") %
                          replica_to_update.data_id %
                          replica_to_update.replica_number %
                          replica_to_update.resource_id);
                }

                ReplicationSourceInfo source_info;
                try {
                    source_info =
                        get_source_data_object_attributes(_ctx.comm(), replica_to_update.data_id, _leaf_bundles);
                }
                catch (const irods::exception& _e) {
                    if (_e.code() != CAT_NO_ROWS_FOUND) {
                        throw;
                    }
                    replicas_to_skip++;
                    continue;
                }

                hierarchy_parser hierarchy_parser;
                const error err_parser = hierarchy_parser.set_string(source_info.resource_hierarchy);
                if (!err_parser.ok()) {
                    THROW(
                        err_parser.code(),
                        boost::format("set_string failed. resource hierarchy [%s]. object path [%s]") %
                                      source_info.resource_hierarchy %
                        source_info.object_path);
                }
                std::string root_resc;
                const error err_first_resc = hierarchy_parser.first_resc(root_resc);
                if (!err_first_resc.ok()) {
                    THROW(
                        err_first_resc.code(),
                        boost::format("first_resc failed. resource hierarchy [%s]. object path [%s]") %
                        source_info.resource_hierarchy %
                        source_info.object_path);
                }

                rodsLog(LOG_NOTICE, "update_out_of_date_replicas: updating out-of-date replica for data id [%ji] from [%s] to [%s]",
                        static_cast<intmax_t>(replica_to_update.data_id),
                        source_info.resource_hierarchy.c_str(),
                        destination_hierarchy.c_str());
                const error err_repl = repl_for_rebalance(
                    _ctx,
                    source_info.object_path,
                    _resource_name,
                    source_info.resource_hierarchy,
                    destination_hierarchy,
                    root_resc,
                    root_resc,
                    source_info.data_mode);

                if (!err_repl.ok()) {
                    if (first_error.ok()) {
                        first_error = err_repl;
                    }
                    const error error_to_log = PASS(err_repl);
                    if (_ctx.comm()->rError.len < MAX_ERROR_MESSAGES) {
                        addRErrorMsg(&_ctx.comm()->rError, error_to_log.code(), error_to_log.result().c_str());
                    }
                    rodsLog(LOG_ERROR,
                            "update_out_of_date_replicas: repl_for_rebalance failed with code [%ji] and message [%s]. object [%s] source hierarchy [%s] data id [%ji] destination repl num [%ji] destination hierarchy [%s]",
                            static_cast<intmax_t>(err_repl.code()), err_repl.result().c_str(), source_info.object_path.c_str(), source_info.resource_hierarchy.c_str(),
                            static_cast<intmax_t>(replica_to_update.data_id), static_cast<intmax_t>(replica_to_update.replica_number), destination_hierarchy.c_str());
                }
            }
            if (!first_error.ok()) {
                THROW(first_error.code(), first_error.result());
            }
        }
        return static_cast<bool>(replicas_to_skip);
    }

    // throws irods::exception
    bool create_missing_replicas(irods::plugin_context& _ctx,
                                 const std::vector<leaf_bundle_t>& _leaf_bundles,
                                 const int _batch_size,
                                 const std::string& _invocation_timestamp,
                                 const std::string& _resource_name)
    {
        bool did_skip_some_repls{};
        for (size_t i=0; i<_leaf_bundles.size(); ++i) {
            int repls_to_skip{};
            const std::string child_name = get_child_name_that_is_ancestor_of_bundle(_resource_name, _leaf_bundles[i]);
            while (true) {
                dist_child_result_t data_ids_needing_new_replicas;
                const int status_chlGetReplListForLeafBundles =
                    chlGetReplListForLeafBundlesOffset(_batch_size,
                                                       i,
                                                       &_leaf_bundles,
                                                       &_invocation_timestamp,
                                                       &data_ids_needing_new_replicas,
                                                       repls_to_skip);
                if (status_chlGetReplListForLeafBundles != 0) {
                    THROW(status_chlGetReplListForLeafBundles,
                          boost::format("failed to get data objects needing new replicas for resource [%s] bundle index [%d] bundles [%s]")
                          % _resource_name
                          % i
                          % leaf_bundles_to_string(_leaf_bundles));
                }

                if (repls_to_skip > 0) {
                    did_skip_some_repls = true;
                }

                if (data_ids_needing_new_replicas.empty()) {
                    break;
                }
                proc_results_for_rebalance(
                    _ctx, _resource_name, child_name, i, _leaf_bundles, data_ids_needing_new_replicas, repls_to_skip);
            }
        }
        return did_skip_some_repls;
    }
} // namespace irods
