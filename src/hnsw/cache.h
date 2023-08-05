#ifndef LDB_HNSW_CACHE_H
#define LDB_HNSW_CACHE_H
#include "postgres.h"

#include <storage/block.h>
#include <utils/dynahash.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

typedef int CacheKey;
typedef struct CacheEntry
{
    CacheKey    key;
    BlockNumber value;
} CacheEntry;

HTAB       *cache_create();
bool        cache_remove(HTAB *cache, CacheKey *key);
BlockNumber cache_get_item(HTAB *cache, CacheKey *key);
void        cache_set_item(HTAB *cache, CacheKey *key, BlockNumber entry);
void        cache_destroy(HTAB *cache);

#endif  // LDB_HNSW_CACHE_H
