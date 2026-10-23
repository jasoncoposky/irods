#ifndef __PAM_AUTH_OBJECT_HPP__
#define __PAM_AUTH_OBJECT_HPP__

#include "irods/irods_error.hpp"
#include "irods/irods_auth_object.hpp"

#include "irods/rcConnect.h"

// =-=-=-=-=-=-=-
#include <memory>

namespace irods {

/// =-=-=-=-=-=-=-
/// @brief constant defining the PAM auth scheme string
    const std::string AUTH_PAM_SCHEME( "pam" );

/// =-=-=-=-=-=-=-
/// @brief object for a PAM irods authentication scheme
    class pam_auth_object : public auth_object {
        public:
            /// =-=-=-=-=-=-=-
            /// @brief Ctor
            pam_auth_object( rError_t* _r_error );
            virtual ~pam_auth_object();
            pam_auth_object( const pam_auth_object& );

            /// =-=-=-=-=-=-=-
            /// @brief assignment operator
            virtual pam_auth_object&  operator=( const pam_auth_object& );

            /// =-=-=-=-=-=-=-
            /// @brief Comparison operator
            virtual bool operator==( const pam_auth_object& ) const;

            /// =-=-=-=-=-=-=-
            /// @brief Plugin resolution operation
            virtual error resolve(
                const std::string&, // interface for which to resolve
                plugin_ptr& );      // ptr to resolved plugin

            /// =-=-=-=-=-=-=-
            /// @brief serialize object to key-value pairs
            virtual error get_re_vars( rule_engine_vars_t& );

        private:

    }; // class pam_auth_object

/// @brief Helpful typedef
    using pam_auth_object_ptr = std::shared_ptr<pam_auth_object>;

}; // namespace irods

#endif // __PAM_AUTH_OBJECT_HPP__
