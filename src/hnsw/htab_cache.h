#ifndef LDB_HNSW_HTAB_CACHE_H
#define LDB_HNSW_HTAB_CACHE_H
#include "postgres.h"

#include <utils/hsearch.h>
#include <utils/memutils.h>

typedef int HTABCacheKey;
typedef struct HTABCacheEntry
{
    HTABCacheKey key;
    void        *value;
} HTABCacheEntry;

typedef struct HTABCache
{
    HTAB   *htab;
    HASHCTL hctl;
} HTABCache;

HTABCache cache_create(char *name, MemoryContext ctx);
bool      cache_remove(HTABCache *cache, HTABCacheKey *key);
void     *cache_get_item(HTABCache *cache, HTABCacheKey *key);
void      cache_set_item(HTABCache *cache, HTABCacheKey *key, void *entry);
void      cache_destroy(HTABCache *cache);

#endif  // LDB_HNSW_HTAB_CACHE_H
