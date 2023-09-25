#include "htab_cache.h"

#include "utils.h"

HTABCache cache_create(const char *name)
{
    HTABCache cache;
    HASHCTL   hctl;

    hctl.keysize = sizeof(HTABCacheKey);
    hctl.entrysize = sizeof(HTABCacheEntry);
    hctl.hcxt = AllocSetContextCreate(CacheMemoryContext, "HTABCache", ALLOCSET_DEFAULT_SIZES);
    HTAB *htab = hash_create(name, 1, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
    cache.hctl = hctl;
    cache.htab = htab;
    return cache;
}

bool cache_remove(HTABCache *cache, HTABCacheKey *key)
{
    bool status;

    hash_search(cache->htab, key, HASH_REMOVE, &status);

    return status;
}

void *cache_get_item(HTABCache *cache, HTABCacheKey *key)
{
    bool status;

    HTABCacheEntry *item = (HTABCacheEntry *)hash_search(cache->htab, key, HASH_FIND, &status);

    if(!status) {
        return NULL;
    }

    return item->value;
}

void cache_set_item(HTABCache *cache, HTABCacheKey *key, void *value)
{
    HTABCacheEntry *entry;

    entry = (HTABCacheEntry *)hash_search(cache->htab, key, HASH_ENTER, NULL);
    entry->value = value;
}

void cache_destroy(HTABCache *cache)
{
    MemoryContext old_context = MemoryContextSwitchTo(cache->hctl.hcxt);
    hash_destroy(cache->htab);
    MemoryContextDelete(cache->hctl.hcxt);
    MemoryContextSwitchTo(old_context);
}
