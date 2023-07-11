#ifndef LDB_HNSW_BUILD_H
#define LDB_HNSW_BUILD_H

#include <access/genam.h>
#include <nodes/execnodes.h>
#include <utils/relcache.h>

#include "lib_interface.h"
#include "usearch.h"

/* Variables */
extern const char *GLOBAL_HNSW_IDX_NAME;

typedef struct HnswBuildState
{
    /* Info */
    Relation   heap;
    Relation   index;
    IndexInfo *indexInfo;

    /* Settings */
    int dimensions;

    /* Statistics */
    double tuples_indexed;
    double reltuples;

    /* hnsw */
    hnsw_t          hnsw;
    usearch_index_t usearch_index;

    /* Memory */
    MemoryContext tmpCtx;
} HnswBuildState;

IndexBuildResult *ldb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo);
void              ldb_ambuildempty(Relation index);
#endif // LDB_HNSW_BUILD_H