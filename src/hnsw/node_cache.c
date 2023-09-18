#include "node_cache.h"

NodeCache node_cache_create()
{
    NodeCache     cache;
    MemoryContext ctx = AllocSetContextCreate(CacheMemoryContext, "Node cache", ALLOCSET_DEFAULT_SIZES);

    HASHCTL hctl;
    hctl.keysize = sizeof(NodeCacheKey);
    hctl.entrysize = sizeof(NodeCacheEntry);
    hctl.hcxt = ctx;
    HTAB *htab = hash_create("NodeCache", 1, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
    cache.hctl = hctl;
    cache.htab = htab;
    return cache;
}

bool node_cache_remove(NodeCache *cache, NodeCacheKey *key)
{
    bool status;

    hash_search(cache->htab, key, HASH_REMOVE, &status);

    return status;
}

void *node_cache_get_item(NodeCache *cache, NodeCacheKey *key)
{
    bool status;

    NodeCacheEntry *item = (NodeCacheEntry *)hash_search(cache->htab, key, HASH_FIND, &status);

    if(!status) {
        return NULL;
    }

    return item->value;
}

void node_cache_set_item(NodeCache *cache, NodeCacheKey *key, void *node)
{
    NodeCacheEntry *entry;

    entry = (NodeCacheEntry *)hash_search(cache->htab, key, HASH_ENTER, NULL);
    entry->value = node;
}

void node_cache_destroy(NodeCache *cache)
{
    MemoryContext old_context = MemoryContextSwitchTo(cache->hctl.hcxt);
    hash_destroy(cache->htab);
    MemoryContextDelete(cache->hctl.hcxt);
    MemoryContextSwitchTo(old_context);
}
