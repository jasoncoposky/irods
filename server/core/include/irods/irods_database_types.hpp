#ifndef __IRODS_DATABASE_TYPES_HPP__
#define __IRODS_DATABASE_TYPES_HPP__

#include <any>
#include <memory>

// =-=-=-=-=-=-=-
// irods includes
#include "irods/irods_plugin_base.hpp"
#include "irods/irods_lookup_table.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include "irods/rcConnect.h"

namespace irods {
// =-=-=-=-=-=-=-
// database plugin pointer type
    class database;
    typedef std::shared_ptr< database > database_ptr;

// =-=-=-=-=-=-=-
// fwd decl of database manager for fco resolve
    class database_manager;

}; // namespace

#endif // __IRODS_DATABASE_TYPES_HPP__



