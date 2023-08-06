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

typedef enum
{
    REAL_ARRAY,
    VECTOR,
    UNKNOWN
} HnswDataType;

IndexBuildResult *ldb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo);
void              ldb_ambuildempty(Relation index);
HnswDataType      GetIndexDataType(Relation index);
int               GetHnswIndexDimensions(Relation index);
void              CheckHnswIndexDimensions(Relation index, Datum value, int deimensions);
#endif  // LDB_HNSW_BUILD_H
