/* For copyright information please refer to files in the COPYRIGHT directory
 */
#include "irods/region.h"
#include "irods/rodsErrorTable.h"

#include <cstring>
#include <limits>
#include <memory_resource>
#include <new>

struct region {
    std::pmr::monotonic_buffer_resource arena;
    std::size_t total_allocated{0};
    struct region_error error{};

    explicit region(std::size_t initial_size)
        : arena(initial_size > 0 ? initial_size : DEFAULT_BLOCK_SIZE,
                std::pmr::new_delete_resource())
    {
    }
};

extern "C" {

Region* make_region(size_t is, void* /*label*/)
{
    return new (std::nothrow) Region(is == 0 ? DEFAULT_BLOCK_SIZE : roundToAlignment(is));
}

void* region_alloc(Region* r, size_t size)
{
    if (!r) {
        return nullptr;
    }

    const size_t desc_size = CACHE_SIZE(RegionDesc, 1);
    if (size > std::numeric_limits<size_t>::max() - desc_size - REGION_ALIGNMENT) {
        return nullptr;
    }
    const size_t alloc_size = size > DEFAULT_BLOCK_SIZE ? (size + desc_size) : roundToAlignment(size + desc_size);

    void* raw = nullptr;
    try {
        raw = r->arena.allocate(alloc_size, REGION_ALIGNMENT);
    }
    catch (const std::bad_alloc&) {
        return nullptr;
    }

    std::memset(raw, 0, alloc_size);

    auto* desc = static_cast<RegionDesc*>(raw);
    desc->region = r;
    desc->size = alloc_size;
    desc->del = 0;

    r->total_allocated += alloc_size;

    return static_cast<unsigned char*>(raw) + desc_size;
}

void region_free(Region* r)
{
    delete r;
}

size_t region_size(Region* r)
{
    return r ? r->total_allocated : 0;
}

} // extern "C"
