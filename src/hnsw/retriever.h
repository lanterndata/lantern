#ifndef LDB_HNSW_RETRIEVER_H
#define LDB_HNSW_RETRIEVER_H
#include <storage/buf.h>
#include <storage/bufpage.h>
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "external_index.h"

#define TAKENBUFFERS_MAX 1000
// this area is used to return pointers back to usearch

RetrieverCtx* ldb_wal_retriever_area_init(Relation index_rel, HnswIndexHeaderPage* header_page_under_wal);
// can be used after each usearch_search to tell the retriever that the pointers given out
// will no longer be used
void ldb_wal_retriever_area_reset(RetrieverCtx* ctx, HnswIndexHeaderPage* header_page_under_wal);

void ldb_wal_retriever_area_fini(RetrieverCtx* ctx);

void* ldb_wal_index_node_retriever(void* ctx, int id);
void* ldb_wal_index_node_retriever_mut(void* ctx, int id);

#endif  // LDB_HNSW_RETRIEVER_H