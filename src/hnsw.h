#ifndef LDB_HNSW_H
#define LDB_HNSW_H

#include "postgres.h"

#if PG_VERSION_NUM < 110000
#error "Requires PostgreSQL 11+"
#endif

#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_HNSW_PHASE_IN_MEMORY_INSERT 2
#define PROGRESS_HNSW_PHASE_LOAD             3

/* Exported functions */
PGDLLEXPORT void _PG_init(void);

/* Distance function enum */
typedef enum {
    L2SQUARE,
    COSINE,
    L1,
    L2,
} distance_function;
#endif // LDB_HNSW_H
