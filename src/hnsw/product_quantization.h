#ifndef LDB_HNSW_PRODUCT_QUANTIZATION_H
#define LDB_HNSW_PRODUCT_QUANTIZATION_H
#include <postgres.h>

#include <utils/builtins.h>

#include "usearch.h"

typedef struct Codebook
{
    uint8    id;
    uint32   dim;
    float4 **centroids;

} Codebook;

typedef struct
{
    float4  *center;
    float4 **points;
    uint32   point_cnt;
} Cluster;

Codebook **product_quantization(uint32                cluster_count,
                                uint32                subset_count,
                                float4              **dataset,
                                uint32                dataset_size,
                                uint32                dim,
                                usearch_metric_kind_t distance_metric,
                                uint32                iter);
#endif  // LDB_HNSW_PRODUCT_QUANTIZATION_H
