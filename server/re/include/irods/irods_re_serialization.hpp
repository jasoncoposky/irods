

#ifndef IRODS_AVRO_SERIALIZATION_HPP
#define IRODS_AVRO_SERIALIZATION_HPP

#include "irods/irods_hierarchy_parser.hpp"
#include "irods/apiHeaderAll.h"
#include "irods/icatStructs.hpp"

#include <any>
#include <functional>
#include <map>
#include <memory>
#include <typeindex>
#include <vector>

typedef std::vector<rodsLong_t> leaf_bundle_t;

namespace irods {
    namespace re_serialization {
        typedef std::type_index                                          index_t;
        typedef std::map<std::string,std::string>                        serialized_parameter_t;
        typedef std::function<error(std::any,serialized_parameter_t&)>   operation_t;
        typedef std::map<index_t, operation_t>                           serialization_map_t;
        
        serialization_map_t& get_serialization_map();
        error add_operation(
            const index_t& _index,
            operation_t    _operation );
        error serialize_parameter(
            std::any                 _in_param,
            serialized_parameter_t&  _out_param );

    }; // re_serialization

}; // namespace irods

#endif // IRODS_AVRO_SERIALIZATION_HPP




