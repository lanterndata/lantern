#ifndef LDB_HNSW_BUILD_H
#define LDB_HNSW_BUILD_H

#include <access/genam.h>
#include <nodes/execnodes.h>
#include <utils/relcache.h>

#include "hnsw.h"
#include "usearch.h"

typedef struct
{
    /* Info */
    Relation   heap;
    Relation   index;
    IndexInfo *indexInfo;

    /* Settings */
    int            dimensions;
    HnswColumnType columnType;
    char          *index_file_path;

    /* Statistics */
    double tuples_indexed;
    double reltuples;

    /* hnsw */
    usearch_index_t       usearch_index;
    usearch_scalar_kind_t usearch_scalar;

    float *pq_codebook;

    /* Memory */
    MemoryContext tmpCtx;
} ldb_HnswBuildState;

IndexBuildResult *ldb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo);
void              ldb_ambuildunlogged(Relation index);
int               GetHnswIndexDimensions(Relation index, IndexInfo *indexInfo);
void              CheckHnswIndexDimensions(Relation index, Datum arrayDatum, int deimensions);
void              ldb_reindex_external_index(Oid indrelid);
// todo: does this render my check unnecessary
#endif  // LDB_HNSW_BUILD_H
