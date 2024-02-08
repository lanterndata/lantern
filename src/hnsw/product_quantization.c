#include <postgres.h>

#include "product_quantization.h"

#include <assert.h>
#include <float.h>
#include <string.h>
#include <time.h>
#include <utils/builtins.h>

#include "usearch.h"

/*
 * Get random index which is not already used
 * This will be used to pick random centroids from the dataset
 * When initializing clusters
 * */
static int get_available_index(int max_idx, int *used_indexes, int *used_indexes_size)
{
    int  idx;
    int  i;
    bool used = false;
    while(true) {
        idx = rand() % (max_idx + 1);
        used = false;

        for(i = 0; i < *used_indexes_size; i++) {
            if(used_indexes[ i ] == idx) {
                used = true;
                break;
            }
        }

        if(!used) break;
    }
    used_indexes[ *used_indexes_size ] = idx;
    *used_indexes_size += 1;
    return idx;
}

/*
 * Initialize k clusters by chosing random points from the dataset
 */
static Cluster **initialize_clusters(uint32 k, float4 **dataset, uint32 dataset_size, uint32 subset_start)
{
    uint32    i;
    Cluster **clusters = palloc(sizeof(Cluster) * k);
    int      *used_indexes = palloc(sizeof(int) * k);
    int       used_indexes_size = 0;
    Cluster  *current_cluster = NULL;

    for(i = 0; i < k; i++) {
        int idx = get_available_index(dataset_size - 1, used_indexes, &used_indexes_size);
        current_cluster = palloc(sizeof(Cluster));
        current_cluster->center = &dataset[ idx ][ subset_start ];
        current_cluster->point_cnt = 0;
        clusters[ i ] = current_cluster;
    }

    return clusters;
}

/*
 * Iterate over whole dataset and assign points to their nearest centroids (clusters)
 * The points will be stored in an array to be used later when updating the center point
 * subset_start will be helpful in case when the subset count is more than 1.
 * So if we have dataset with 12 dimensional vectors and we want to split them into 3 subsets
 * On first subset iteration subset_start will be 0 and subset_dim 4, then subset_start: 4, subset_dim: 4 and so on
 * */
static void assign_to_clusters(float4              **dataset,
                               uint32                dataset_size,
                               uint32                subset_start,
                               uint32                subset_dim,
                               Cluster             **clusters,
                               uint32                k,
                               usearch_metric_kind_t distance_metric)
{
    uint32          i;
    uint32          j;
    float4          distance;
    float4          min_dist;
    uint32          min_dist_cluster_idx;
    Cluster        *current_cluster;
    float4         *current_subset;
    usearch_error_t error = NULL;

    for(i = 0; i < dataset_size; i++) {
        current_subset = &dataset[ i ][ subset_start ];
        min_dist = FLT_MAX;
        for(j = 0; j < k; j++) {
            current_cluster = clusters[ j ];
            distance = usearch_distance(
                current_subset, current_cluster->center, usearch_scalar_f32_k, subset_dim, distance_metric, &error);
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
        current_cluster->points[ current_cluster->point_cnt ] = current_subset;
        current_cluster->point_cnt += 1;
    }
}

/*
 * Calculate mean value of given 2d array
 * */
static float4 *calculate_mean(float4 **points, uint32 point_cnt, uint32 subset_dim)
{
    float4 *mean = palloc0(sizeof(float4) * subset_dim);
    uint32  i;  // rows
    uint32  j;  // cols

    for(i = 0; i < point_cnt; i++) {
        for(j = 0; j < subset_dim; j++) {
            if(!mean[ j ]) mean[ j ] = 0;
            mean[ j ] += points[ i ][ j ];
        }
    }

    for(i = 0; i < subset_dim; i++) {
        mean[ i ] = mean[ i ] / point_cnt;
    }

    return mean;
}

/*
 * Update cluster centers with the mean values of assigned points
 * And remove points from cluster
 * */
static void update_centers(Cluster **clusters, uint32 k, uint32 subset_dim)
{
    uint32   i;
    Cluster *current_cluster;

    for(i = 0; i < k; i++) {
        current_cluster = clusters[ i ];
        if(!current_cluster->point_cnt) continue;

        current_cluster->center = calculate_mean(current_cluster->points, current_cluster->point_cnt, subset_dim);

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
                            uint32                subset_dim,
                            usearch_metric_kind_t distance_metric)
{
    usearch_error_t error = NULL;
    uint32          i;
    float4          threshold = 0.15f;
    float4          distance = 0.0f;

    for(i = 0; i < cluster_count; i++) {
        distance += usearch_distance(
            old_centers[ i ], clusters[ i ]->center, usearch_scalar_f32_k, subset_dim, distance_metric, &error);
        assert(!error);
    }

    distance /= cluster_count;

    return distance <= threshold;
}

/*
 * Run k-means clustering over the dataset in a subset of vectors
 * args:
 *   k - cluster count
 *   dataset - 2d array of float numbers
 *   dataset_size - length of dataset (axis 0)
 *   subset_start - index from which the subset will be taken (in case if subset count is 1 this will be 0)
 *   subset_dim - length of the slice to take from dataset vectors starting from subset_start index
 *   iter - number of iterations to run the algorithm
 * returns:
 *   2D array of floats. [ [f32] x k ] each sub array will be a centroid point
 */
float4 **k_means(uint32                k,
                 float4              **dataset,
                 uint32                dataset_size,
                 uint32                subset_start,
                 uint32                subset_dim,
                 usearch_metric_kind_t distance_metric,
                 uint32                iter)
{
    uint32    i, j;
    float4  **centroids = palloc(sizeof(size_t) * k);
    Cluster **clusters = initialize_clusters(k, dataset, dataset_size, subset_start);
    float4  **old_centers = palloc(sizeof(size_t) * k);

    for(i = 0; i < iter; i++) {
        assign_to_clusters(dataset, dataset_size, subset_start, subset_dim, clusters, k, distance_metric);

        // Keep old centers to check wether we should stop iterations
        for(j = 0; j < k; j++) {
            old_centers[ j ] = clusters[ j ]->center;
        }
        update_centers(clusters, k, subset_dim);

        if(should_stop_iterations(old_centers, clusters, k, subset_dim, distance_metric)) {
            break;
        }
    }

    for(i = 0; i < k; i++) {
        centroids[ i ] = clusters[ i ]->center;
    }
    return centroids;
}

/*
 * Run k_means over each subset of dataset vectors
 * args:
 *   cluster_count - number of clusters to initialize for each subset. Then each subset will have centroids equal to
 *   subset_count - how many parts split the vectors of dataset. Each subset will be trained separately and
 *   dataset - 2D array of f32 values
 *   dataset_size - length of dataset (axis 0)
 *   dim - dimension of the vectors in dataset
 *   distance_metric - distance metric to use when training dataset. One of (l2sq, cos, hamming)
 *   iter - iterations to run the k-means
 * have it's own centroids This function will return codebook for each subset of the vector Each Codebook will have - {
 * id, centroids: [ [f32 x subset_dim] x k ], dim: subset_dim }
 */
Codebook **product_quantization(uint32                cluster_count,
                                uint32                subset_count,
                                float4              **dataset,
                                uint32                dataset_size,
                                uint32                dim,
                                usearch_metric_kind_t distance_metric,
                                uint32                iter)
{
    uint32     i;
    uint32     subset_start;
    uint32     subset_dim;
    float4   **subset_centroids;
    Codebook  *current_codebook;
    Codebook **codebooks = palloc(sizeof(Codebook) * subset_count);

    subset_dim = dim / subset_count;
    for(i = 0; i < subset_count; i++) {
        subset_start = i * subset_dim;
        if(i == subset_count - 1 && dim % subset_count != 0) {
            // If the vector is not divisible to subset_count
            // Dimensions of the last subset will be the remaining length of the vector
            subset_dim = dim - subset_start;
        }

        subset_centroids
            = k_means(cluster_count, dataset, dataset_size, subset_start, subset_dim, distance_metric, iter);

        current_codebook = palloc(sizeof(Codebook));
        current_codebook->id = i;
        current_codebook->centroids = subset_centroids;
        current_codebook->dim = subset_dim;
        codebooks[ i ] = current_codebook;
    }

    return codebooks;
}
