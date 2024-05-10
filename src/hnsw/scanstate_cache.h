#ifndef LDB_HNSW_SCANSTATE_CACHE_H
#define LDB_HNSW_SCANSTATE_CACHE_H

#include <postgres.h>
/* The function initializes the per-process global space used to park
 * prepared hnsw index search caches. This way, e.g. visits, contexts, and
 * other buffers necessary for an index search will not need reinitialization for every
 * index search
 * */
void ldb_scanstate_cache_init();

void  ldb_index_cache_add(Oid tid, void *index);
void *ldb_index_cache_get(Oid tid);
#endif  // LDB_HNSW_SCANSTATE_CACHE_H
