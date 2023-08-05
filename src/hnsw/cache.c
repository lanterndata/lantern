#include "cache.h"

HTAB *cache_create()
{
    MemoryContext ctx = AllocSetContextCreate(TopMemoryContext, "BlockNumer cache", ALLOCSET_DEFAULT_SIZES);

    HASHCTL hctl;
    hctl.keysize = sizeof(CacheKey);
    hctl.entrysize = sizeof(CacheEntry);
    hctl.hcxt = ctx;
    return hash_create("BlockNumberCache", 1, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
}

bool cache_remove(HTAB *cache, CacheKey *key)
{
    bool status;

    hash_search(cache, key, HASH_REMOVE, &status);

    return status;
}

BlockNumber cache_get_item(HTAB *cache, CacheKey *key)
{
    bool status;

    CacheEntry *item = (CacheEntry *)hash_search(cache, key, HASH_FIND, &status);

    if(!status) {
        return InvalidBlockNumber;
    }

    return item->value;
}

void cache_set_item(HTAB *cache, CacheKey *key, BlockNumber blockno)
{
    CacheEntry *entry;

    entry = (CacheEntry *)hash_search(cache, key, HASH_ENTER, NULL);
    entry->value = blockno;
}

void cache_destroy(HTAB *cache)
{
    // MemoryContextDelete(cache->hctl.hcxt);
    hash_destroy(cache);
}
