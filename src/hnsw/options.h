#ifndef LDB_HNSW_OPTIONS_H
#define LDB_HNSW_OPTIONS_H
#include <postgres.h>

#include <utils/relcache.h>  // Relation

#include "external_index.h"

// todo:: add hnsw dynamic vector dimension constraints
// based on vector element size

/* HNSW vector dim constraints */
#define HNSW_DEFAULT_DIMS 3
#define HNSW_MAX_DIMS     2000

/* 32 in faiss */
#define HNSW_DEFAULT_M 16
#define HNSW_MAX_M     128
/* 40 in faiss */
#define HNSW_DEFAULT_EF_CONSTRUCTION 128
#define HNSW_MAX_EF_CONSTRUCTION     400
/* 10 in faiss*/
#define HNSW_DEFAULT_EF               64
#define HNSW_MAX_EF                   400
#define HNSW_DEFAULT_PROVIDER         "usearch"
#define HNSW_MAX_ELEMENT_LIMIT        200000000
#define ALG_OPTION_MAX_STRING_LEN     32
#define HNSWLIB_DEFAULT_ELEMENT_LIMIT 2000000

#define LDB_HNSW_DEFAULT_K 10
#define LDB_HNSW_MAX_K     1000
/* HNSW index options */
typedef struct HnswOptions
{
    // max elements the table will ever have. required for hnswlib
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int   dims;
    int   element_limit;
    int   m;
    int   ef_construction;
    int   ef;
    int   alg;
    // char[ALG_OPTION_MAX_STRING_LEN] alg;
} HnswOptions;

int                   HnswGetDims(Relation index);
int                   HnswGetM(Relation index);
int                   HnswGetEfConstruction(Relation index);
int                   HnswGetEf(Relation index);
int                   HnswGetElementLimit(Relation index);
usearch_metric_kind_t HnswGetMetricKind(Relation index);

bytea *ldb_amoptions(Datum reloptions, bool validate);

extern int ldb_hnsw_init_k;

#endif  // LDB_HNSW_OPTIONS_H
