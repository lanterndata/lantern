#ifndef LDB_HNSW_INSERT_H
#define LDB_HNSW_INSERT_H
#include <access/genam.h>     // IndexUniqueCheck
#include <nodes/execnodes.h>  // IndexInfo
#include <storage/bufmgr.h>
#include <utils/memutils.h>

#include "retriever.h"
#include "usearch.h"

// todo:: remove me!! added because did not wantt o refactor the uses of this
// #define LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS 5
// this now includes buffers dirtied by the usearch
// hnsw updates (= buffers of new neighbors of the inserted vector)
// #define LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS 100
#include "extra_dirtied.h"

typedef struct
{
    usearch_index_t uidx;
    RetrieverCtx   *retriever_ctx;
} HnswInsertState;

bool ldb_aminsert(Relation         index,
                  Datum           *values,
                  bool            *isnull,
                  ItemPointer      heap_tid,
                  Relation         heap,
                  IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                  ,
                  bool indexUnchanged
#endif
                  ,
                  IndexInfo *indexInfo);

#endif  // INSERT_H
