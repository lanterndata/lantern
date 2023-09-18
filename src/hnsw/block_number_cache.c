#include "block_number_cache.h"

BlockNumberCache bln_cache_create()
{
    BlockNumberCache cache;
    MemoryContext    ctx = AllocSetContextCreate(CacheMemoryContext, "BlockNumber cache", ALLOCSET_DEFAULT_SIZES);

    HASHCTL hctl;
    hctl.keysize = sizeof(BlockNumberCacheKey);
    hctl.entrysize = sizeof(BlockNumberCacheEntry);
    hctl.hcxt = ctx;
    HTAB *htab = hash_create("BlockNumberCache", 1, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
    cache.hctl = hctl;
    cache.htab = htab;
    return cache;
}

bool bln_cache_remove(BlockNumberCache *cache, BlockNumberCacheKey *key)
{
    bool status;

    hash_search(cache->htab, key, HASH_REMOVE, &status);

    return status;
}

BlockNumber bln_cache_get_item(BlockNumberCache *cache, BlockNumberCacheKey *key)
{
    bool status;

    BlockNumberCacheEntry *item = (BlockNumberCacheEntry *)hash_search(cache->htab, key, HASH_FIND, &status);

    if(!status) {
        return InvalidBlockNumber;
    }

    return item->value;
}

void bln_cache_set_item(BlockNumberCache *cache, BlockNumberCacheKey *key, BlockNumber blockno)
{
    BlockNumberCacheEntry *entry;

    entry = (BlockNumberCacheEntry *)hash_search(cache->htab, key, HASH_ENTER, NULL);
    entry->value = blockno;
}

void bln_cache_destroy(BlockNumberCache *cache)
{
    MemoryContext old_context = MemoryContextSwitchTo(cache->hctl.hcxt);
    hash_destroy(cache->htab);
    MemoryContextDelete(cache->hctl.hcxt);
    MemoryContextSwitchTo(old_context);
}
