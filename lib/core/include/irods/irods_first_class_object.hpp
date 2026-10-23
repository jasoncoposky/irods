#ifndef __IRODS_FIRST_CLASS_OBJECT_HPP__
#define __IRODS_FIRST_CLASS_OBJECT_HPP__

// =-=-=-=-=-=-=-
#include "irods/irods_log.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include "irods/rcConnect.h"

#include <map>
#include <memory>

namespace irods {

    class plugin_base;
    using plugin_ptr = std::shared_ptr<plugin_base>;
    using rule_engine_vars_t = std::map<std::string, std::string>;
    // =-=-=-=-=-=-=-
    // base class for all object types
    class first_class_object {
        public:
            // =-=-=-=-=-=-=-
            // Constructors
            first_class_object() {};

            // =-=-=-=-=-=-=-
            // Destructor
            virtual ~first_class_object() {};

            // =-=-=-=-=-=-=-
            // plugin resolution operators
            virtual error resolve(
                const std::string&, // plugin interface
                plugin_ptr& ) = 0;  // resolved plugin

            // =-=-=-=-=-=-=-
            // accessor for rule engine variables
            virtual error get_re_vars( rule_engine_vars_t& ) = 0;

    }; // class first_class_object

    /// =-=-=-=-=-=-=-
    /// @brief shared pointer to first_class_object
    using first_class_object_ptr = std::shared_ptr<first_class_object>;

}; // namespace irods

#endif // __IRODS_FIRST_CLASS_OBJECT_HPP__



