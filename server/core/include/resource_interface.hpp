#ifndef RESOURCE_INTERFACE_HPP
#define RESOURCE_INTERFACE_HPP

#include "irods_resource_plugin.hpp"
#include "irods_hierarchy_parser.hpp"

namespace irods {

    struct resource_interface : public resource
    {
        resource_interface(const std::string& i, const std::string& c) : resource(i, c)
        {
            add_operation(RESOURCE_OP_CREATE, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return create(c); }));
            add_operation(RESOURCE_OP_OPEN,   std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return open(c); }));
            add_operation(RESOURCE_OP_READ,   std::function<error(plugin_context&, void*, int)>([&](plugin_context& c, void* b, int l) -> error { return read(c, b, l); }));
            add_operation(RESOURCE_OP_WRITE,  std::function<error(plugin_context&, void*, int)>([&](plugin_context& c, void* b, int l) -> error { return write(c, b, l); }));
            add_operation(RESOURCE_OP_CLOSE,  std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return close(c); }));
            add_operation(RESOURCE_OP_UNLINK, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return unlink(c); }));
            add_operation(RESOURCE_OP_STAT,   std::function<error(plugin_context&, struct stat*)>([&](plugin_context& a, struct stat* b) -> error { return stat(a, b); }));
            add_operation(RESOURCE_OP_MKDIR,  std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return mkdir(c); }));
            add_operation(RESOURCE_OP_OPENDIR, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return opendir(c); }));
            add_operation(RESOURCE_OP_READDIR, std::function<error(plugin_context&, struct rodsDirent**)>([&](plugin_context& c, rodsDirent** d) -> error { return readdir(c, d); }));
            add_operation(RESOURCE_OP_RENAME, std::function<error(plugin_context&, const char*)>( [&](plugin_context& c, const char* e) -> error { return rename(c, e); }));
            add_operation(RESOURCE_OP_FREESPACE, std::function<error(plugin_context&)>( [&](plugin_context& c) -> error { return get_freespace(c); }));
            add_operation(RESOURCE_OP_LSEEK, std::function<error(plugin_context&, long long, int)>( [&](plugin_context& c, long long l, int w) -> error { return lseek(c, l, w); }));
            add_operation(RESOURCE_OP_RMDIR, std::function<error(plugin_context&)>( [&](plugin_context& c) -> error { return rmdir(c); }));
            add_operation(RESOURCE_OP_CLOSEDIR, std::function<error(plugin_context&)>( [&](plugin_context& c) -> error { return closedir(c); }));
            add_operation(RESOURCE_OP_STAGETOCACHE, std::function<error(plugin_context&, const char*)>( [&](plugin_context& c, const char* p) -> error { return stage_archive_to_cache(c, p); }));
            add_operation(RESOURCE_OP_SYNCTOARCH, std::function<error(plugin_context&, const char*)>( [&](plugin_context& c, const char* p) -> error { return sync_cache_to_archive(c, p); }));
            add_operation(RESOURCE_OP_REGISTERED, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return registered_event(c); }));
            add_operation(RESOURCE_OP_UNREGISTERED, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return unregistered_event(c); }));
            add_operation(RESOURCE_OP_MODIFIED, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return modified_event(c); }));
            add_operation(RESOURCE_OP_NOTIFY, std::function<error(plugin_context&, const std::string*)>([&](plugin_context& c, const std::string* s) -> error { return notify_event(c, s); }));
            add_operation(RESOURCE_OP_TRUNCATE, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return truncate(c); }));
            add_operation(RESOURCE_OP_RESOLVE_RESC_HIER, std::function<error(plugin_context&, const std::string*, const std::string*, irods::hierarchy_parser*, float*)>(
                        [&](plugin_context& a, const std::string* b, const std::string* c, irods::hierarchy_parser* d, float* e) -> error { return resolve_hierarchy(a, b, c, d, e); }));
            add_operation(RESOURCE_OP_REBALANCE, std::function<error(plugin_context&)>([&](plugin_context& c) -> error { return rebalance(c); }));

            // =-=-=-=-=-=-=-
            // set some properties necessary for backporting to iRODS legacy code
            set_property<int>(irods::RESOURCE_CHECK_PATH_PERM, 2); // DO_CHK_PATH_PERM
            set_property<int>(irods::RESOURCE_CREATE_PATH,     1); // CREATE_PATH
        }

        error need_post_disconnect_maintenance_operation(bool&) { return SUCCESS(); }
        error post_disconnect_maintenance_operation(irods::pdmo_type&) { return ERROR(SYS_INVALID_INPUT_PARAM, "nop"); }

        // resource plugin interface
        error registered_event(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error unregistered_event(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error modified_event(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error notify_event(plugin_context&, const std::string*) { return CODE(SYS_NOT_SUPPORTED); }
        error get_freespace(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }

        error create(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error open(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error read(plugin_context&, void*, int) { return CODE(SYS_NOT_SUPPORTED); }
        error write(plugin_context&, void*, int) { return CODE(SYS_NOT_SUPPORTED); }
        error close(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error unlink(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error stat(plugin_context&, struct stat*) { return CODE(SYS_NOT_SUPPORTED); }
        error lseek(plugin_context&, long long, int) { return CODE(SYS_NOT_SUPPORTED); }
        error rename(plugin_context&, const char*) { return CODE(SYS_NOT_SUPPORTED); }
        error truncate(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }

        error mkdir(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error rmdir(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error opendir(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error closedir(plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
        error readdir(plugin_context&, struct rodsDirent**) { return CODE(SYS_NOT_SUPPORTED); }

        error stage_archive_to_cache(plugin_context&, const char*) { return CODE(SYS_NOT_SUPPORTED); }
        error sync_cache_to_archive(plugin_context&, const char*) { return CODE(SYS_NOT_SUPPORTED); }
        error resolve_hierarchy(plugin_context&, const std::string*, const std::string*, hierarchy_parser*, float*) { return CODE(SYS_NOT_SUPPORTED); }
        error rebalance(irods::plugin_context&) { return CODE(SYS_NOT_SUPPORTED); }
    }; // struct resource_interface

} // namespace irods

#endif // RESOURCE_INTERFACE_HPP
