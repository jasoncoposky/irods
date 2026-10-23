#ifndef __IRODS_AUTH_TYPES_HPP__

// =-=-=-=-=-=-=-
#include <any>
#include <memory>

// =-=-=-=-=-=-=-
#include "irods/irods_plugin_base.hpp"
#include "irods/irods_lookup_table.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include "irods/rcConnect.h"

namespace irods {
// =-=-=-=-=-=-=-
// auth plugin pointer type
    class auth;
    using auth_ptr = std::shared_ptr< auth >;

// =-=-=-=-=-=-=-
// fwd decl of network manager for fco resolve
    class auth_manager;

}; // namespace

#define __IRODS_AUTH_TYPES_HPP__
#endif // __IRODS_AUTH_TYPES_HPP__



