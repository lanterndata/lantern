#include <postgres.h>

#include "product_quantization.h"

#include <assert.h>
#include <float.h>
#include <miscadmin.h>
#include <string.h>
#include <time.h>
#include <utils/builtins.h>
#if PG_VERSION_NUM >= 150000
#include <common/pg_prng.h>
#endif

#include "usearch.h"

/*
 * Get random index which is not already used
 * This will be used to pick random centroids from the dataset
 * When initializing clusters
 * */
static int get_random_tid(const int max_tid, const int *used_tids, const int used_tids_size)
{
    int  idx;
    int  i;
    bool used = false;
    while(true) {
#if PG_VERSION_NUM >= 150000 && !defined EMSCRIPTEN
        uint32 rand_num = pg_prng_uint32(&pg_global_prng_state);
#else
        uint32 rand_num = (uint32)random();
#endif
        idx = rand_num % (max_tid + 1);
        used = false;

        for(i = 0; i < used_tids_size; i++) {
            if(used_tids[ i ] == idx) {
                used = true;
                break;
            }
        }

        if(!used) break;
    }
    return idx;
}

/*
 * Initialize k clusters by chosing random points from the dataset
 */
static Cluster **initialize_clusters(uint32 k, float4 **dataset, uint32 dataset_size, uint32 subvector_start)
{
    uint32    i;
    Cluster **clusters = palloc(sizeof(Cluster *) * k);
    int      *used_tids = palloc(sizeof(int) * k);
    int       used_tids_size = 0;
    Cluster  *current_cluster = NULL;

    for(i = 0; i < k; i++) {
        int idx = get_random_tid(dataset_size - 1, (const int *)used_tids, used_tids_size);
        used_tids[ used_tids_size ] = idx;
        used_tids_size += 1;
        current_cluster = palloc(sizeof(Cluster));
        current_cluster->center = &dataset[ idx ][ subvector_start ];
        current_cluster->point_cnt = 0;
        clusters[ i ] = current_cluster;
    }

    return clusters;
}

/*
 * Iterate over whole dataset and assign points to their nearest centroids (clusters)
 * The points will be stored in an array to be used later when updating the center point
 * subvector_start will be helpful in case when the subvector count is more than 1.
 * So if we have dataset with 12 dimensional vectors and we want to split them into 3 subvectors
 * On first subvector iteration subvector_start will be 0 and subvector_dim 4, then subvector_start: 4, subvector_dim: 4
 * and so on
 * */
static void assign_to_clusters(float4              **dataset,
                               uint32                dataset_size,
                               uint32                subvector_start,
                               uint32                subvector_dim,
                               Cluster             **clusters,
                               uint32                k,
                               usearch_metric_kind_t distance_metric)
{
    uint32          i;
    uint32          j;
    float4          distance;
    float4          min_dist;
    uint32          min_dist_cluster_idx = 0;
    Cluster        *current_cluster;
    float4         *current_subvector;
    usearch_error_t error = NULL;

    for(i = 0; i < dataset_size; i++) {
        current_subvector = &dataset[ i ][ subvector_start ];
        min_dist = FLT_MAX;
        for(j = 0; j < k; j++) {
            current_cluster = clusters[ j ];
            distance = usearch_distance(current_subvector,
                                        current_cluster->center,
                                        usearch_scalar_f32_k,
                                        subvector_dim,
                                        distance_metric,
                                        &error);
            assert(!error);
            if(distance < min_dist) {
                min_dist = distance;
                min_dist_cluster_idx = j;
            }
        }
        current_cluster = clusters[ min_dist_cluster_idx ];

        if(!current_cluster->point_cnt) {
            // This is not optimal but as long as this is just array of pointers
            // It won't consume much memory
            current_cluster->points = palloc(sizeof(size_t) * dataset_size);
        }
        current_cluster->points[ current_cluster->point_cnt ] = current_subvector;
        current_cluster->point_cnt += 1;
    }
}

/*
 * Calculate mean value of given 2d array
 * */
static float4 *calculate_mean(float4 **points, uint32 point_cnt, uint32 subvector_dim)
{
    float4 *mean = palloc0(sizeof(float4) * subvector_dim);
    uint32  i;  // rows
    uint32  j;  // cols

    for(i = 0; i < point_cnt; i++) {
        for(j = 0; j < subvector_dim; j++) {
            mean[ j ] += points[ i ][ j ];
        }
    }

    for(i = 0; i < subvector_dim; i++) {
        mean[ i ] = mean[ i ] / point_cnt;
    }

    return mean;
}

/*
 * Update cluster centers with the mean values of assigned points
 * And remove points from cluster
 * */
static void update_centers(Cluster **clusters, uint32 k, uint32 subvector_dim)
{
    uint32   i;
    Cluster *current_cluster;

    for(i = 0; i < k; i++) {
        current_cluster = clusters[ i ];
        if(!current_cluster->point_cnt) continue;

        current_cluster->center = calculate_mean(current_cluster->points, current_cluster->point_cnt, subvector_dim);

        current_cluster->points = NULL;
        current_cluster->point_cnt = 0;
    }
}

/*
 * Checks whether distances between new and old centers
 * are less then the defined threshold to stop iterations
 * and return current centers as result.
 * */
bool should_stop_iterations(float4              **old_centers,
                            Cluster             **clusters,
                            uint32                cluster_count,
                            uint32                subvector_dim,
                            usearch_metric_kind_t distance_metric)
{
    usearch_error_t error = NULL;
    uint32          i;
    float4          threshold = 0.1f;
    float4          distance = 0.0f;

    for(i = 0; i < cluster_count; i++) {
        distance += usearch_distance(
            old_centers[ i ], clusters[ i ]->center, usearch_scalar_f32_k, subvector_dim, distance_metric, &error);
        assert(!error);
    }

    distance /= cluster_count;

    return distance <= threshold;
}

/*
 * Run k-means clustering over the dataset in a subvector of vectors
 * args:
 *   k - cluster count
 *   dataset - 2d array of float numbers
 *   dataset_size - length of dataset (axis 0)
 *   subvector_start - index from which the subvector will be taken (in case if subvector count is 1 or this is the
 *                     first subvector this will be 0)
 *   subvector_dim - length of the slice to take from dataset vectors starting from subvector_start index
 *   iter - number of iterations to run the algorithm returns: 2D array of floats. [ [f32] x k ]
 *          each sub array will be a centroid point
 */
float4 **k_means(uint32                k,
                 float4              **dataset,
                 uint32                dataset_size,
                 uint32                subvector_start,
                 uint32                subvector_dim,
                 usearch_metric_kind_t distance_metric,
                 uint32                iter)
{
    uint32    i, j;
    float4  **centroids = palloc(sizeof(size_t) * k);
    Cluster **clusters = initialize_clusters(k, dataset, dataset_size, subvector_start);
    float4  **old_centers = palloc(sizeof(size_t) * k);

    for(i = 0; i < iter; i++) {
        assign_to_clusters(dataset, dataset_size, subvector_start, subvector_dim, clusters, k, distance_metric);

        // Keep old centers to check wether we should stop iterations
        for(j = 0; j < k; j++) {
            old_centers[ j ] = clusters[ j ]->center;
        }
        update_centers(clusters, k, subvector_dim);
        // Check for CTRL-C interrupts
        CHECK_FOR_INTERRUPTS();

        if(should_stop_iterations(old_centers, clusters, k, subvector_dim, distance_metric)) {
            break;
        }
    }

    for(i = 0; i < k; i++) {
        centroids[ i ] = clusters[ i ]->center;
    }
    return centroids;
}

/*
 * Run k_means over each subvector of dataset vectors
 * args:
 *   cluster_count - number of clusters to initialize for each subvector.
 *   subvector_count - how many parts split the vectors of dataset.
 *                     Each subvector will be trained separately and have it's own centroids
 *   dataset - 2D array of f32 values where each row is a vector that we are PQ-quantizing
 *   dataset_size - length of dataset (axis 0)
 *   dim - dimension of the vectors in dataset
 *   distance_metric - distance metric to use when training dataset. One of (l2sq, cos, hamming)
 *   iter - iterations to run the k-means
 *
 *  This function will return codebook for each subvector of the vector
 *  Each PQCodebook will have
 * - { id, centroids: [ [f32 x subvector_dim] x k ], dim: subvector_dim }
 */
PQCodebook **product_quantization(uint32                cluster_count,
                                  uint32                subvector_count,
                                  float4              **dataset,
                                  uint32                dataset_size,
                                  uint32                dim,
                                  usearch_metric_kind_t distance_metric,
                                  uint32                iter)
{
    uint32       i;
    uint32       subvector_start;
    uint32       subvector_dim;
    float4     **subvector_centroids;
    PQCodebook  *current_codebook;
    PQCodebook **codebooks = palloc(sizeof(PQCodebook *) * subvector_count);

    subvector_dim = dim / subvector_count;
    for(i = 0; i < subvector_count; i++) {
        subvector_start = i * subvector_dim;
        if(i == subvector_count - 1 && dim % subvector_count != 0) {
            // If the vector is not divisible to subvector_count
            // Dimensions of the last subvector will be the remaining length of the vector
            subvector_dim = dim - subvector_start;
        }

        subvector_centroids
            = k_means(cluster_count, dataset, dataset_size, subvector_start, subvector_dim, distance_metric, iter);

        current_codebook = palloc(sizeof(PQCodebook));
        current_codebook->id = i;
        current_codebook->centroids = subvector_centroids;
        current_codebook->dim = subvector_dim;
        codebooks[ i ] = current_codebook;
    }

    return codebooks;
}
