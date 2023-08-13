#ifndef LDB_RETRIEVER_H
#define LDB_RETRIEVER_H
#include <storage/buf.h>
#include <utils/hsearch.h>

#include "cache.h"

#define TAKENBUFFERS_MAX 1000
// this area is used to return pointers back to usearch

typedef struct
{
    Cache   block_numbers_cache;
    Buffer* takenbuffers;
    int     takenbuffers_next;
} RetrieverCtx;

RetrieverCtx* ldb_wal_retriever_area_init();
// can be used after each usearch_search to tell the retriever that the pointers given out
// will no longer be used
void ldb_wal_retriever_area_reset(RetrieverCtx* ctx);

void ldb_wal_retriever_area_free(RetrieverCtx* ctx);

void* ldb_wal_index_node_retriever(void* ctx, int id);
void* ldb_wal_index_node_retriever_mut(void* ctx, int id);

#endif  // LDB_RETRIEVER_H