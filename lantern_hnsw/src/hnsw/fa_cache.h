#ifndef LDB_HNSW_FA_CACHE_H
#define LDB_HNSW_FA_CACHE_H
#include <stddef.h>

/* A fixed-size fully associative FIFO cache meant to be embedded
 * in other data structures for inline caching.
 * Currently a naive loop is used for lookup, but we could use
 * intrinsics to speed this up
 * We could also experiment with better cache replacement policies here
 * (CLOCK, 2 lists, etc)
 */

#define FA_CACHE_SIZE 64

typedef struct
{
    int   keys[ FA_CACHE_SIZE ];
    void* values[ FA_CACHE_SIZE ];
    int   next;
} FullyAssociativeCache;

// Initalize the cache so all lookups return NULL
static inline void fa_cache_init(FullyAssociativeCache* cache)
{
    // All values are set to NULL with the below
    // so if key 0 is looked up before key 0 is inserted
    // the data strucutre will returned the default value for key
    // which will be NULL
    MemSet(cache, 0, sizeof(FullyAssociativeCache));
}

// Insert the key value pair into an already initialized cache
static inline void fa_cache_insert(FullyAssociativeCache* cache, int key, void* value)
{
    cache->keys[ cache->next ] = key;
    cache->values[ cache->next ] = value;
    cache->next = (cache->next + 1) % FA_CACHE_SIZE;
}

// Get the value associated with the key
static inline void* fa_cache_get(FullyAssociativeCache* cache, int key)
{
    for(int i = 0; i < FA_CACHE_SIZE; i++) {
        if(cache->keys[ i ] == key) {
            return cache->values[ i ];
        }
    }
    return NULL;
}

#endif  // LDB_HNSW_FA_CACHE_H
