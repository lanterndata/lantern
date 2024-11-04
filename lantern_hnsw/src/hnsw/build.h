#ifndef LDB_HNSW_BUILD_H
#define LDB_HNSW_BUILD_H

#include <access/genam.h>
#include <nodes/execnodes.h>
#include <utils/relcache.h>

#include "hnsw.h"
#include "usearch.h"

typedef struct external_index_socket_t external_index_socket_t;

typedef struct
{
    /* Info */
    Relation   heap;
    Relation   index;
    IndexInfo *indexInfo;

    /* Settings */
    int                      dimensions;
    int                      index_file_fd;
    uint64                   index_buffer_size;
    HnswColumnType           columnType;
    char                    *index_file_path;
    char                    *index_buffer;
    bool                     external;
    external_index_socket_t *external_socket;

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
// todo: does this render my check unnecessary
#endif  // LDB_HNSW_BUILD_H
