#ifndef LDB_PQTABLE_H
#define LDB_PQTABLE_H

#include <postgres.h>

#include <fmgr.h>
#include <utils/relcache.h>

/* Exported functions */
PGDLLEXPORT Datum create_pq_codebook(PG_FUNCTION_ARGS);

float *load_pq_codebook(Relation index, size_t vector_dimensions, size_t *num_centroids, size_t *num_subvectors);

#endif  // LDB_PQTABLE_H
