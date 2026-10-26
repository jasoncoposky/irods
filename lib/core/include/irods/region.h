/* For copyright information please refer to files in the COPYRIGHT directory
 */
#ifndef _REGION_H__
#define _REGION_H__

#include <stddef.h>
#include <stdio.h>

#define DEFAULT_BLOCK_SIZE 1024
/* the alignment in the region in bytes */
#define REGION_ALIGNMENT 16
#define roundToAlignment(x) ((x)%REGION_ALIGNMENT == 0?(x):(((x)/REGION_ALIGNMENT)+1)*REGION_ALIGNMENT)
/* the point the elem i of type t starting from p in the cache, aligned */
#define CACHE_ELEM(t, p, i) (((t)p)+i)
/* the size of an array of type t and length i in the cache, aligned */
#define CACHE_SIZE(t, l) roundToAlignment(sizeof(t)*l)

struct region_error {
    int code;
    char msg[1024];
    void *obj; /* a generic point to an object which is the source of the error */
};

typedef struct region Region;

typedef struct region_desc {
    Region *region;
    size_t size;
    int del;
} RegionDesc;

#define REGION_DESC(x) ((RegionDesc *)(((const unsigned char*)(x))-CACHE_SIZE(RegionDesc, 1)))
#define IN_REGION(x,r) (REGION_DESC(x)->region == (r))
#define SET_DELETE(x) (REGION_DESC(x)->del=1)
#define DELETED(x) (REGION_DESC(x)->del)
#define SIZE(x) (REGION_DESC(x)->size)

#ifdef __cplusplus
extern "C" {
#endif

/* create a region with initial size is */
/* if s == 0 then the initial size is DEFAULT_BLOCK_SIZE */
/* returns NULL if it runs out of memory */
Region *make_region( size_t is, void *label );
/* allocation s bytes in region r */
/* return NULL if it runs out of memory */
void *region_alloc( Region *r, size_t s );
/* free region r */
void region_free( Region *r );
size_t region_size( Region *r );

#ifdef __cplusplus
}
#endif

#endif // _REGION_H__
