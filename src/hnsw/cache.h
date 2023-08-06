#ifndef LDB_HNSW_CACHE_H
#define LDB_HNSW_CACHE_H
#include "postgres.h"

#include <storage/block.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

typedef int CacheKey;
typedef struct CacheEntry
{
    CacheKey    key;
    BlockNumber value;
} CacheEntry;
typedef struct Cache
{
    HTAB   *htab;
    HASHCTL hctl;
} Cache;

Cache       cache_create();
bool        cache_remove(Cache *cache, CacheKey *key);
BlockNumber cache_get_item(Cache *cache, CacheKey *key);
void        cache_set_item(Cache *cache, CacheKey *key, BlockNumber entry);
void        cache_destroy(Cache *cache);

#endif  // LDB_HNSW_CACHE_H
