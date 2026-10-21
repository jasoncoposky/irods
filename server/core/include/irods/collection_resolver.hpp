#ifndef IRODS_COLLECTION_RESOLVER_HPP
#define IRODS_COLLECTION_RESOLVER_HPP

#include "irods/collection_types.hpp"
#include "irods/rodsConnect.h"

#include <string_view>

namespace irods::experimental::collections
{
    /// Returns the per-agent special-collection resolution cache.
    auto global_resolution_cache() noexcept -> collection_resolution_cache&;

    /// Invalidates any cached positive or negative resolution entries matching _logical_path
    /// (as well as any matching legacy SpecCollCacheHead nodes).
    auto invalidate_collection_cache(std::string_view _logical_path) -> void;

    /// Clears the entire special-collection resolution cache and legacy SpecCollCacheHead list.
    auto clear_collection_cache() noexcept -> void;

    /// Resolves a logical path against the catalog and per-agent resolution cache,
    /// translating LINKED_COLL chains according to _mode without mutating caller buffers.
    auto resolve_collection_path(rsComm_t& _comm,
                                 std::string_view _input_path,
                                 link_resolution_mode _mode,
                                 resolved_path& _out_resolved) -> int;
} // namespace irods::experimental::collections

#endif // IRODS_COLLECTION_RESOLVER_HPP
