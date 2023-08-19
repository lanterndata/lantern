#include "cache.h"

Cache cache_create()
{
    Cache         cache;
    MemoryContext ctx = AllocSetContextCreate(CacheMemoryContext, "BlockNumber cache", ALLOCSET_DEFAULT_SIZES);

    HASHCTL hctl;
    hctl.keysize = sizeof(CacheKey);
    hctl.entrysize = sizeof(CacheEntry);
    hctl.hcxt = ctx;
    HTAB *htab = hash_create("BlockNumberCache", 1, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
    cache.hctl = hctl;
    cache.htab = htab;
    return cache;
}

bool cache_remove(Cache *cache, CacheKey *key)
{
    bool status;

    hash_search(cache->htab, key, HASH_REMOVE, &status);

    return status;
}

BlockNumber cache_get_item(Cache *cache, CacheKey *key)
{
    bool status;

    CacheEntry *item = (CacheEntry *)hash_search(cache->htab, key, HASH_FIND, &status);

    if(!status) {
        return InvalidBlockNumber;
    }

    return item->value;
}

void cache_set_item(Cache *cache, CacheKey *key, BlockNumber blockno)
{
    CacheEntry *entry;

    entry = (CacheEntry *)hash_search(cache->htab, key, HASH_ENTER, NULL);
    entry->value = blockno;
}

void cache_destroy(Cache *cache)
{
    MemoryContext old_context = MemoryContextSwitchTo(cache->hctl.hcxt);
    hash_destroy(cache->htab);
    MemoryContextDelete(cache->hctl.hcxt);
    MemoryContextSwitchTo(old_context);
}
