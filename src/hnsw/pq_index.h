
#ifndef LDB_HNSW_PQ_INDEX_H
#define LDB_HNSW_PQ_INDEX_H

#include <postgres.h>

#include <utils/rel.h>

float *pq_codebook(Relation index, size_t vector_dimensions, size_t *num_centroids, size_t *num_subvectors);

#endif  // LDB_HNSW_PQ_INDEX_H
