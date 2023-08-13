#ifndef LDB_RETRIEVER_H
#define LDB_RETRIEVER_H
#include <storage/buf.h>
#include <storage/bufpage.h>
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "cache.h"
#include "extra_dirtied.h"

#define TAKENBUFFERS_MAX 1000
// this area is used to return pointers back to usearch

typedef struct
{
    Cache block_numbers_cache;

    Relation index_rel;

    // HnswIndexHeaderPage HEADER_FOR_EXTERNAL_RETRIEVER;
    // uint32*      blockmap_page_group_index;
    ExtraDirtiedBufs* extra_dirted;

#if LANTERNDB_COPYNODES
    char* wal_retriever_area = NULL;
    int   wal_retriever_area_size = 0;
    int   wal_retriever_area_offset = 0;
#else

    Buffer* takenbuffers;
    int     takenbuffers_next;
#endif
} RetrieverCtx;

RetrieverCtx* ldb_wal_retriever_area_init(Relation index_rel);
// can be used after each usearch_search to tell the retriever that the pointers given out
// will no longer be used
void ldb_wal_retriever_area_reset(RetrieverCtx* ctx);

void ldb_wal_retriever_area_free(RetrieverCtx* ctx);

void* ldb_wal_index_node_retriever(void* ctx, int id);
void* ldb_wal_index_node_retriever_mut(void* ctx, int id);

#endif  // LDB_RETRIEVER_H