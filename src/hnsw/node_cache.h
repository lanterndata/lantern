#ifndef LDB_HNSW_NODE_CACHE_H
#define LDB_HNSW_NODE_CACHE_H
#include "postgres.h"

#include <storage/block.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

typedef int NodeCacheKey;
typedef struct NodeCacheEntry
{
    NodeCacheKey key;
    void        *value;
} NodeCacheEntry;
typedef struct NodeCache
{
    HTAB   *htab;
    HASHCTL hctl;
} NodeCache;

NodeCache node_cache_create();
bool      node_cache_remove(NodeCache *cache, NodeCacheKey *key);
void     *node_cache_get_item(NodeCache *cache, NodeCacheKey *key);
void      node_cache_set_item(NodeCache *cache, NodeCacheKey *key, void *entry);
void      node_cache_destroy(NodeCache *cache);

#endif  // LDB_HNSW_NODE_CACHE_H
