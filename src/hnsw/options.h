#ifndef LDB_HNSW_OPTIONS_H
#define LDB_HNSW_OPTIONS_H
#include <postgres.h>

#include <access/reloptions.h>
#include <utils/relcache.h>  // Relation

#include "usearch.h"

// todo:: add hnsw dynamic vector dimension constraints
// based on vector element size

/* HNSW vector dim constraints */
#define HNSW_DEFAULT_DIM -1
#define HNSW_MAX_DIM     2000

/* 32 in faiss */
#define HNSW_DEFAULT_M 16
#define HNSW_MAX_M     128
/* 40 in faiss */
#define HNSW_DEFAULT_EF_CONSTRUCTION 128
#define HNSW_MAX_EF_CONSTRUCTION     400
/* 10 in faiss*/
#define HNSW_DEFAULT_EF 64
#define HNSW_MAX_EF     400

/* quantization options */
typedef enum
{
    QUANT_BITS_UNSET = 0,
    QUANT_BITS_1,
    QUANT_BITS_2,
    QUANT_BITS_4,
    QUANT_BITS_8,
    QUANT_BITS_16,
    QUANT_BITS_32,

} QuantBitsEnum;

#define LDB_HNSW_DEFAULT_K 10
#define LDB_HNSW_MAX_K     1000

/* HNSW index options */
typedef struct ldb_HnswOptions
{
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int   dim;
    int   m;
    int   ef_construction;
    int   ef;
    bool  pq;

    QuantBitsEnum quant_bits;
    int           experimantal_index_path_offset;
} ldb_HnswOptions;

int                   ldb_HnswGetDim(Relation index);
int                   ldb_HnswGetM(Relation index);
int                   ldb_HnswGetEfConstruction(Relation index);
int                   ldb_HnswGetEf(Relation index);
char*                 ldb_HnswGetIndexFilePath(Relation index);
bool                  ldb_HnswGetPq(Relation index);
usearch_metric_kind_t ldb_HnswGetMetricKind(Relation index);

bytea* ldb_amoptions(Datum reloptions, bool validate);

extern int  ldb_hnsw_init_k;
extern int  ldb_hnsw_ef_search;
extern bool ldb_is_test;
extern bool ldb_pgvector_compat;

#endif  // LDB_HNSW_OPTIONS_H
