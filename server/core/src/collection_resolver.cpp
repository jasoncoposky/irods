#include "irods/collection_resolver.hpp"

#include "irods/rsGlobalExtern.hpp"
#include "irods/specColl.hpp"

#include <cstdlib>
#include <cstring>
#include <string>

namespace irods::experimental::collections
{
    namespace
    {
        auto is_same_or_related_path(std::string_view _a, std::string_view _b) noexcept -> bool
        {
            while (_a.size() > 1 && _a.back() == '/') {
                _a.remove_suffix(1);
            }
            while (_b.size() > 1 && _b.back() == '/') {
                _b.remove_suffix(1);
            }
            if (_a == _b) {
                return true;
            }
            if (_a.size() > _b.size() && _a.substr(0, _b.size()) == _b && _a[_b.size()] == '/') {
                return true;
            }
            if (_b.size() > _a.size() && _b.substr(0, _a.size()) == _a && _b[_a.size()] == '/') {
                return true;
            }
            return false;
        }
    } // anonymous namespace

    auto global_resolution_cache() noexcept -> collection_resolution_cache&
    {
        static collection_resolution_cache instance;
        return instance;
    }

    auto invalidate_collection_cache(std::string_view _logical_path) -> void
    {
        global_resolution_cache().invalidate(_logical_path);

        specCollCache_t* prev = nullptr;
        specCollCache_t* curr = SpecCollCacheHead;
        while (curr != nullptr) {
            specCollCache_t* next = curr->next;
            if (is_same_or_related_path(curr->specColl.collection, _logical_path)) {
                if (prev == nullptr) {
                    SpecCollCacheHead = next;
                }
                else {
                    prev->next = next;
                }
                std::free(curr);
            }
            else {
                prev = curr;
            }
            curr = next;
        }
    }

    auto clear_collection_cache() noexcept -> void
    {
        global_resolution_cache().clear();

        specCollCache_t* curr = SpecCollCacheHead;
        SpecCollCacheHead = nullptr;
        while (curr != nullptr) {
            specCollCache_t* next = curr->next;
            std::free(curr);
            curr = next;
        }
    }

    auto resolve_collection_path(rsComm_t& _comm,
                                 std::string_view _input_path,
                                 link_resolution_mode _mode,
                                 resolved_path& _out_resolved) -> int
    {
        auto lookup_fn = [&_comm](std::string_view _query_path,
                                  std::string& _out_governing_coll,
                                  collection_info_variant& _out_info) -> int {
            const std::string query_str{_query_path};
            specCollCache_t* cached = nullptr;
            const int status = getSpecCollCache(&_comm, query_str.c_str(), 0, &cached);
            if (status < 0 || cached == nullptr) {
                return (status < 0) ? status : CAT_NO_ROWS_FOUND;
            }

            _out_governing_coll = cached->specColl.collection;
            _out_info = from_legacy_spec_coll(cached->specColl);
            return 0;
        };

        return resolve_collection_path_with_lookup(
            _input_path, _mode, global_resolution_cache(), lookup_fn, _out_resolved);
    }
} // namespace irods::experimental::collections
