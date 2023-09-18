#ifndef LDB_HNSW_BLN_CACHE_H
#define LDB_HNSW_BLN_CACHE_H
#include "postgres.h"

#include <storage/block.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

typedef int BlockNumberCacheKey;
typedef struct BlockNumberCacheEntry
{
    BlockNumberCacheKey key;
    BlockNumber         value;
} BlockNumberCacheEntry;
typedef struct BlockNumberCache
{
    HTAB   *htab;
    HASHCTL hctl;
} BlockNumberCache;

BlockNumberCache bln_cache_create();
bool             bln_cache_remove(BlockNumberCache *cache, BlockNumberCacheKey *key);
BlockNumber      bln_cache_get_item(BlockNumberCache *cache, BlockNumberCacheKey *key);
void             bln_cache_set_item(BlockNumberCache *cache, BlockNumberCacheKey *key, BlockNumber entry);
void             bln_cache_destroy(BlockNumberCache *cache);

#endif  // LDB_HNSW_CACHE_HH
