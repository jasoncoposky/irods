#ifndef __IRODS_NETWORK_TYPES_HPP__

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
// network plugin pointer type
    class network;
    using network_ptr = std::shared_ptr< network >;

// =-=-=-=-=-=-=-
// fwd decl of network manager for fco resolve
    class network_manager;

}; // namespace

#define __IRODS_NETWORK_TYPES_HPP__
#endif // __IRODS_NETWORK_TYPES_HPP__



