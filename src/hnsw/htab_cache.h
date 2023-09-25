#ifndef LDB_HNSW_HTAB_CACHE_H
#define LDB_HNSW_HTAB_CACHE_H

#include <postgres.h>

#include <utils/hsearch.h>
#include <utils/memutils.h>

/*
 * An abstract hash table (HTAB) cache that stores void pointers.
 * Casting of pointers should be done externally when retrieving items.
 *
 * Note:
 * - This cache stores void pointers, so type casting should be performed externally
 *   when retrieving items from the cache.
 * - The key and value pointers supplied for cache operations should have lifetimes
 *   that outlive the cache itself.
 * - If an item is not found in the cache, a NULL value will be returned.
 * - A new memory context will be created in CacheMemoryContext when calling
 *   cache_create, and it will be destroyed when calling cache_destroy.
 */

typedef int32 HTABCacheKey;
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

HTABCache cache_create(const char *name);
bool      cache_remove(HTABCache *cache, HTABCacheKey *key);
void     *cache_get_item(HTABCache *cache, HTABCacheKey *key);
void      cache_set_item(HTABCache *cache, HTABCacheKey *key, void *entry);
void      cache_destroy(HTABCache *cache);

#endif  // LDB_HNSW_HTAB_CACHE_H
