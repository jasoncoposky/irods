#ifndef IRODS_RESOURCE_PLUGIN_IMPOSTOR_HPP
#define IRODS_RESOURCE_PLUGIN_IMPOSTOR_HPP

#include "resource_interface.hpp"
#include "irods_file_object.hpp"

namespace irods {

    class impostor_resource : public resource_interface {
        public:
            impostor_resource(
                const std::string& _inst_name,
                const std::string& _context );
            error report_error(plugin_context&);

            // resource plugin interface
            error start(plugin_property_map&);
            error stop(plugin_property_map&);

            error registered_event(plugin_context&);
            error unregistered_event(plugin_context&);
            error modified_event(plugin_context&);
            error notify_event(plugin_context&, const std::string*);
            error get_freespace(plugin_context&);

            error create(plugin_context&);
            error open(plugin_context&);
            error read(plugin_context&, void*, int);
            error write(plugin_context&, void*, int);
            error close(plugin_context&);
            error unlink(plugin_context&);
            error stat(plugin_context&, struct stat*);
            error lseek(plugin_context&, long long, int);
            error rename(plugin_context&, const char*);
            error truncate(plugin_context&);

            error mkdir(plugin_context&);
            error rmdir(plugin_context&);
            error opendir(plugin_context&);
            error closedir(plugin_context&);
            error readdir(plugin_context&, struct rodsDirent**);

            error stage_archive_to_cache(plugin_context&, const char*);
            error sync_cache_to_archive(plugin_context&, const char*);
            error resolve_hierarchy(plugin_context&, const std::string*, const std::string*, hierarchy_parser*, float*);
            error rebalance(irods::plugin_context&);

            error resolve_hierarchy_open(
                plugin_property_map& _prop_map,
                file_object_ptr      _file_obj,
                const std::string&   _resc_name,
                const std::string&   _curr_host,
                float&               _out_vote );
            error resolve_hierarchy_create(
                plugin_property_map& _prop_map,
                const std::string&   _resc_name,
                const std::string&   _curr_host,
                float&               _out_vote );

    }; // class impostor_resource

}; // namespace irods


#endif // IRODS_RESOURCE_PLUGIN_IMPOSTOR_HPP



