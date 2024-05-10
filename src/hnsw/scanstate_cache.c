#include <postgres.h>

#include <assert.h>

#define MAX_CACHED_INDEXES 100
static Oid   index_oids[ MAX_CACHED_INDEXES ];
static void *usearch_indexes[ MAX_CACHED_INDEXES ];

void ldb_scanstate_cache_init()
{
    memset(usearch_indexes, 0, sizeof(usearch_indexes));
    memset(index_oids, InvalidOid, sizeof(index_oids));
}

// Adds the index to the cache if there is space
void ldb_index_cache_add(Oid oid, void *index)
{
    assert(oid != InvalidOid);
    for(int i = 0; i < MAX_CACHED_INDEXES; i++) {
        if(InvalidOid == index_oids[ i ]) {
            index_oids[ i ] = oid;
            assert(usearch_indexes[ i ] == NULL);
            usearch_indexes[ i ] = index;
            return;
        }
        // if there is no space, we do nothing
    }
}

void *ldb_index_cache_get(Oid oid)
{
    assert(oid != InvalidOid);
    for(int i = 0; i < MAX_CACHED_INDEXES; i++) {
        if(oid == index_oids[ i ]) {
            return usearch_indexes[ i ];
        }
    }
    return NULL;
}
