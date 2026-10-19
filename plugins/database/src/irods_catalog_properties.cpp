/*
 * catalog_properties.cpp
 *
 *  Created on: Oct 9, 2013
 *      Author: adt
 */

#include "irods/private/irods_catalog_properties.hpp"

#include "irods/catalog.hpp"
#include "irods/irods_error.hpp"
#include "irods/irods_exception.hpp"
#include "irods/rodsErrorTable.h"

#include "irods/private/database_session.hpp"
#include "irods/private/nanodbc_executor.hpp"
#include <nanodbc/nanodbc.h>

namespace irods {

    catalog_properties& catalog_properties::instance() {
        static catalog_properties singleton;
        return singleton;
    }

    void catalog_properties::capture_if_needed( icatSessionStruct * _icss ) {
        if ( !captured_ ) {
            capture( _icss );
        }
    }

// Query iCAT settings and fill catalog_properties::instance
    void catalog_properties::capture( icatSessionStruct* _icss ) {
        if (_icss != nullptr && _icss->databaseType != DB_TYPE_POSTGRES) {
            THROW( SYS_NOT_IMPLEMENTED, "Capturing catalog properties is not available for this database" );
        }

        try {
            auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
            auto res = executor.execute_query(db_conn, "select name, setting from pg_settings");
            while (res.next()) {
                properties[res.get<std::string>(0)] = boost::any(res.get<std::string>(1));
            }
            captured_ = true;
        }
        catch (const nanodbc::database_error& e) {
            THROW(CAT_SQL_ERR, e.what());
        }
        catch (const std::exception& e) {
            THROW(SYS_INTERNAL_ERR, e.what());
        }
    } // catalog_properties::capture()

} // namespace irods
