// =-=-=-=-=-=-=-
//
#include "irods_resource_plugin_impostor.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_resource_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_physical_object.hpp"
#include "irods_collection_object.hpp"
#include "irods_string_tokenize.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_stacktrace.hpp"
#include "irods_server_properties.hpp"
#include "irods_hierarchy_parser.hpp"

#include "miscServerFunct.hpp"

namespace irods {
    impostor_resource::impostor_resource(
        const std::string& i
      , const std::string& c ) :
        resource_interface(i, c)
    {
    } // ctor

    error impostor_resource::start(plugin_property_map&) { return SUCCESS(); }
    error impostor_resource::stop(plugin_property_map&) { return SUCCESS(); }

    error impostor_resource::registered_event(plugin_context&) {
         return SUCCESS();
    }

    error impostor_resource::unregistered_event(
        plugin_context& ) {
        return SUCCESS();
    }

    error impostor_resource::modified_event(
        plugin_context& ) {
        return SUCCESS();
    }

    error impostor_resource::notify_event(
        plugin_context&,
        const std::string* ) {
        return SUCCESS();
    }

    error impostor_resource::get_freespace(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::getfs_freespace

    error impostor_resource::create(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::create

    error impostor_resource::open(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::open

    error impostor_resource::read(
        plugin_context& _ctx,
        void*,
        int ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::read

    error impostor_resource::write(
        plugin_context& _ctx,
        void*,
        int ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::write

    error impostor_resource::close(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::close

    error impostor_resource::unlink(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::unlink

    error impostor_resource::stat(
        plugin_context& _ctx,
        struct stat* ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::stat

    error impostor_resource::lseek(
        plugin_context& _ctx,
        long long ,
        int ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::lseek

    error impostor_resource::mkdir(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::mkdir

    error impostor_resource::rmdir(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::rmdir

    error impostor_resource::opendir(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::opendir

    error impostor_resource::closedir(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::closedir

    error impostor_resource::readdir(
        plugin_context& _ctx,
        struct rodsDirent** ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::readdir

    error impostor_resource::rename(
        plugin_context& _ctx,
        const char* ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::rename

    error impostor_resource::truncate(
        plugin_context& _ctx ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::truncate

    error impostor_resource::stage_archive_to_cache(
        plugin_context& _ctx,
        const char* ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::stage_archive_to_cache

    error impostor_resource::sync_cache_to_archive(
        plugin_context& _ctx,
        const char* ) {
        return impostor_resource::report_error( _ctx );
    } // impostor_resource::sync_cache_to_archive

    // =-=-=-=-=-=-=-
    // redirect_create - code to determine redirection for create operation
    error impostor_resource::resolve_hierarchy_create(
        plugin_property_map&   _prop_map,
        const std::string&             _resc_name,
        const std::string&             _curr_host,
        float&                         _out_vote ) {
        error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // determine if the resource is down
        int resc_status = 0;
        error get_ret = _prop_map.get< int >( RESOURCE_STATUS, resc_status );
        if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"status\" property." ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if ( INT_RESC_STATUS_DOWN == resc_status ) {
                _out_vote = 0.0;
                result.code( SYS_RESC_IS_DOWN );
                // result = PASS( result );
            }
            else {

                // =-=-=-=-=-=-=-
                // get the resource host for comparison to curr host
                std::string host_name;
                get_ret = _prop_map.get< std::string >( RESOURCE_LOCATION, host_name );
                if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"location\" property." ) ).ok() ) {

                    // =-=-=-=-=-=-=-
                    // vote higher if we are on the same host
                    if ( _curr_host == host_name ) {
                        _out_vote = 1.0;
                    }
                    else {
                        _out_vote = 0.5;
                    }
                }

                rodsLog(
                    LOG_DEBUG,
                    "create :: resc name [%s] curr host [%s] resc host [%s] vote [%f]",
                    _resc_name.c_str(),
                    _curr_host.c_str(),
                    host_name.c_str(),
                    _out_vote );

            }
        }
        return result;

    } // impostor_resource::resolve_hierarchy_create

    // =-=-=-=-=-=-=-
    // resolve_hierarchy_open - code to determine redirection for open operation
    error impostor_resource::resolve_hierarchy_open(
        plugin_property_map&   _prop_map,
        file_object_ptr        _file_obj,
        const std::string&             _resc_name,
        const std::string&             _curr_host,
        float&                         _out_vote ) {
        error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // initially set a good default
        _out_vote = 0.0;

        // =-=-=-=-=-=-=-
        // determine if the resource is down
        int resc_status = 0;
        error get_ret = _prop_map.get< int >( RESOURCE_STATUS, resc_status );
        if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"status\" property." ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if ( INT_RESC_STATUS_DOWN != resc_status ) {

                // =-=-=-=-=-=-=-
                // get the resource host for comparison to curr host
                std::string host_name;
                get_ret = _prop_map.get< std::string >( RESOURCE_LOCATION, host_name );
                if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"location\" property." ) ).ok() ) {

                    // =-=-=-=-=-=-=-
                    // set a flag to test if were at the curr host, if so we vote higher
                    bool curr_host = ( _curr_host == host_name );

                    // =-=-=-=-=-=-=-
                    // make some flags to clarify decision making
                    bool need_repl = ( _file_obj->repl_requested() > -1 );

                    // =-=-=-=-=-=-=-
                    // set up variables for iteration
                    error final_ret = SUCCESS();
                    std::vector< physical_object > objs = _file_obj->replicas();
                    std::vector< physical_object >::iterator itr = objs.begin();

                    // =-=-=-=-=-=-=-
                    // check to see if the replica is in this resource, if one is requested
                    for ( ; itr != objs.end(); ++itr ) {
                        // =-=-=-=-=-=-=-
                        // run the hier string through the parser and get the last
                        // entry.
                        std::string last_resc;
                        hierarchy_parser parser;
                        parser.set_string( itr->resc_hier() );
                        parser.last_resc( last_resc );

                        // =-=-=-=-=-=-=-
                        // more flags to simplify decision making
                        bool repl_us  = ( _file_obj->repl_requested() == itr->repl_num() );
                        bool resc_us  = ( _resc_name == last_resc );
                        bool is_dirty = ( itr->is_dirty() != 1 );

                        // =-=-=-=-=-=-=-
                        // success - correct resource and don't need a specific
                        //           replication, or the repl nums match
                        if ( resc_us ) {
                            // =-=-=-=-=-=-=-
                            // if a specific replica is requested then we
                            // ignore all other criteria
                            if ( need_repl ) {
                                if ( repl_us ) {
                                    _out_vote = 1.0;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // repl requested and we are not it, vote
                                    // very low
                                    _out_vote = 0.25;
                                }
                            }
                            else {
                                // =-=-=-=-=-=-=-
                                // if no repl is requested consider dirty flag
                                if ( is_dirty ) {
                                    // =-=-=-=-=-=-=-
                                    // repl is dirty, vote very low
                                    _out_vote = 0.25;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // if our repl is not dirty then a local copy
                                    // wins, otherwise vote middle of the road
                                    if ( curr_host ) {
                                        _out_vote = 1.0;
                                    }
                                    else {
                                        _out_vote = 0.5;
                                    }
                                }
                            }

                            rodsLog(
                                LOG_DEBUG,
                                "open :: resc name [%s] curr host [%s] resc host [%s] vote [%f]",
                                _resc_name.c_str(),
                                _curr_host.c_str(),
                                host_name.c_str(),
                                _out_vote );

                            break;

                        } // if resc_us

                    } // for itr
                }
            }
            else {
                result.code( SYS_RESC_IS_DOWN );
                result = PASS( result );
            }
        }

        return result;

    } // impostor_resource::resolve_hierarchy_open

    // =-=-=-=-=-=-=-
    // used to allow the resource to determine which host
    // should provide the requested operation
    error impostor_resource::resolve_hierarchy(
        plugin_context&    _ctx,
        const std::string* _opr,
        const std::string* _curr_host,
        hierarchy_parser*  _out_parser,
        float*             _out_vote ) {
        error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // check the context validity
        error ret = _ctx.valid< file_object >();
        if ( ( result = ASSERT_PASS( ret, "Invalid resource context." ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // check incoming parameters
            if ( ( result = ASSERT_ERROR( _opr && _curr_host && _out_parser && _out_vote, SYS_INVALID_INPUT_PARAM, "Invalid input parameter." ) ).ok() ) {
                // =-=-=-=-=-=-=-
                // cast down the chain to our understood object type
                file_object_ptr file_obj = boost::dynamic_pointer_cast< file_object >( _ctx.fco() );

                // =-=-=-=-=-=-=-
                // get the name of this resource
                std::string resc_name;
                ret = _ctx.prop_map().get< std::string >( RESOURCE_NAME, resc_name );
                if ( ( result = ASSERT_PASS( ret, "Failed in get property for name." ) ).ok() ) {
                    // =-=-=-=-=-=-=-
                    // add ourselves to the hierarchy parser by default
                    _out_parser->add_child( resc_name );

                    // =-=-=-=-=-=-=-
                    // test the operation to determine which choices to make
                    if ( OPEN_OPERATION  == ( *_opr ) ||
                            WRITE_OPERATION == ( *_opr ) ) {
                        // =-=-=-=-=-=-=-
                        // call redirect determination for 'get' operation
                        ret = impostor_resource::resolve_hierarchy_open( _ctx.prop_map(), file_obj, resc_name, ( *_curr_host ), ( *_out_vote ) );
                        result = ASSERT_PASS( ret, "Failed redirecting for open." );

                    }
                    else if ( CREATE_OPERATION == ( *_opr ) ) {
                        // =-=-=-=-=-=-=-
                        // call redirect determination for 'create' operation
                        ret = impostor_resource::resolve_hierarchy_create( _ctx.prop_map(), resc_name, ( *_curr_host ), ( *_out_vote ) );
                        result = ASSERT_PASS( ret, "Failed redirecting for create." );
                    }

                    else {
                        // =-=-=-=-=-=-=-
                        // must have been passed a bad operation
                        result = ASSERT_ERROR( false, INVALID_OPERATION, "Operation not supported." );
                    }
                }
            }
        }

        return result;

    } // impostor_resource::resolve_hierarchy

    error impostor_resource::rebalance(
        plugin_context& _ctx ) {
        return SUCCESS();
    } // impostor_resource::rebalance_plugin

    error impostor_resource::report_error(
        plugin_context& _ctx ) {
        std::string resc_name;
        error ret = _ctx.prop_map().get< std::string >( RESOURCE_NAME, resc_name );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        std::string resc_type;
        ret = _ctx.prop_map().get< std::string >( RESOURCE_TYPE, resc_type );
        if ( !ret.ok() ) {
            return PASS( ret );
        }

        std::string msg( "NOTE :: Direct Access of Impostor Resource [" );
        msg += resc_name + "] of Given Type [" + resc_type + "]";

        addRErrorMsg( &_ctx.comm()->rError, STDOUT_STATUS, msg.c_str() );

        return ERROR(
                   INVALID_ACCESS_TO_IMPOSTOR_RESOURCE,
                   msg );

    }

}; // namespace irods

