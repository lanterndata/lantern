#include <postgres.h>

#include <access/heapam.h>
#include <catalog/pg_type.h>
#include <miscadmin.h>
#include <storage/bufmgr.h>

#include "hnsw.h"

#if PG_VERSION_NUM <= 120000
#include <access/htup_details.h>
#endif

#include "hnsw/product_quantization.h"
#include "hnsw/utils.h"
#include "usearch.h"

#if PG_VERSION_NUM < 130000
#define TYPALIGN_INT 'i'
#endif

/*
 * Create codebook to be used for product quantization of vectors
 * args:
 *   tablerelid Oid -> 'table'::regclass
 *   column Name -> 'column_name'
 *   cluster_cnt Int -> 10 -- count of cluster for k-means
 *   subvector_cnt Int -> 3 --  vectors in dataset will be split in subvector_cnt parts
 *                       --  and k-means will be run for each part returning centroids
 *                       --  equal to cluster count for each subvector
 *   iter Int -> 5 -- how many times the k-means will be run to correct the centroids
 *   distance_metric Text -> (l2sq, cosine, hamming) -- distance function will be chosed based on this metric
 * returns:
 *   3d array -> [ [ [ [f32 x subvector_dim ] ] x cluster_cnt ] x subvector_cnt ]
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(create_pq_codebook);
Datum       create_pq_codebook(PG_FUNCTION_ARGS)
{
    // Function arguments
    Oid                   tablerelid = PG_GETARG_OID(0);
    Name                  column = PG_GETARG_NAME(1);
    uint32                cluster_cnt = PG_GETARG_UINT32(2);
    uint32                subvector_cnt = PG_GETARG_UINT32(3);
    text                 *distance_metric_text = PG_GETARG_TEXT_P(4);
    usearch_metric_kind_t distance_metric;
    // -----------------
    // Dataset variables
    Relation table;
#if PG_VERSION_NUM < 120000
    HeapScanDesc scan;
#else
    TableScanDesc scan;
#endif
    Datum     datum;
    HeapTuple tuple;
    float4  **dataset;
    uint32    dataset_size = 0;
    uint32    dataset_dim = 0;
    uint32    current_tuple_dim = 0;
    uint32_t  estimated_row_count;
    bool      is_null;
    int32     colid;
    // -----------------
    // Return value variables
    int          codebook_dims[ 3 ];
    int          codebook_lbs[ 3 ];
    int          codebook_number_of_elements = 0;
    int          codebook_elem_size = 0;
    int          codebook_dim = 0;
    Datum       *codebook_datums;
    PQCodebook **codebooks;
    ArrayType   *array;
    // -----------------
    uint32 i, j, n;  // variables used in loops

    if(subvector_cnt == 0) {
        elog(ERROR, "Subvector count can not be zero");
    }

    if(cluster_cnt > 1 << 8) {
        elog(ERROR, "Cluster count can not be greater than %d", 1 << 8);
    }

    distance_metric = GetMetricKindFromStr(text_to_cstring(distance_metric_text));

    table = relation_open(tablerelid, AccessShareLock);
    estimated_row_count = EstimateRowCount(table);
#if PG_VERSION_NUM < 120000
    scan = heap_beginscan(table, GetTransactionSnapshot(), 0, NULL);
#else
    scan = heap_beginscan(table, GetTransactionSnapshot(), 0, NULL, NULL, SO_TYPE_SEQSCAN);
#endif

    colid = GetColumnAttributeNumber(table, column->data);
    if(colid == -1) {
        elog(ERROR, "Column %s not found in table", column->data);
    }

    dataset = palloc(estimated_row_count * sizeof(size_t));

    while((tuple = heap_getnext(scan, ForwardScanDirection))) {
        datum = heap_getattr(tuple, colid, RelationGetDescr(table), &is_null);
        if(is_null) continue;
        array = DatumGetArrayTypePCopy(datum);
        current_tuple_dim = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
        if(dataset_dim == 0) {
            dataset_dim = current_tuple_dim;
            // TODO:: this check can be removed as soon as we resolve return type issue from this function
            if(dataset_dim % subvector_cnt != 0) {
                elog(ERROR, "Dataset dimensions should be divisible by subvector count");
            }
        } else if(current_tuple_dim != dataset_dim) {
            heap_endscan(scan);
            elog(ERROR, "Table should have equally sized array: expected %d got %d", dataset_dim, current_tuple_dim);
        }

        dataset[ dataset_size++ ] = (float4 *)ARR_DATA_PTR(array);

        if(estimated_row_count == dataset_size - 1) {
            dataset = repalloc(dataset, estimated_row_count * 2 * sizeof(size_t));
            estimated_row_count *= 2;
        }
    }

    heap_endscan(scan);
    relation_close(table, AccessShareLock);
    elog(INFO, "Table scanned. Dataset size %d", dataset_size);

    if(dataset_size < cluster_cnt) {
        elog(ERROR, "Dataset size can not be smaller than cluster count");
    }

    if(dataset_dim < subvector_cnt) {
        elog(ERROR, "Dataset dimension can not be smaller than subvector count");
    }

    elog(INFO, "Starting k-means over dataset with (subvectors=%d, clusters=%d)", subvector_cnt, cluster_cnt);
    codebooks
        = product_quantization(cluster_cnt, subvector_cnt, dataset, dataset_size, dataset_dim, distance_metric, 200);
    elog(INFO, "Codebooks created");

    // Lower bounds for result arrays
    codebook_lbs[ 0 ] = 1;
    codebook_lbs[ 1 ] = 1;
    codebook_lbs[ 2 ] = 1;

    // Dimensions for parent array the subvector count
    codebook_dims[ 0 ] = subvector_cnt;
    // Dimensions for each codebook array the cluster count
    codebook_dims[ 1 ] = cluster_cnt;

    for(i = 0; i < subvector_cnt; i++) {
        codebook_dim = codebooks[ i ]->dim;
        for(j = 0; j < cluster_cnt; j++) {
            codebook_number_of_elements += codebook_dim;
        }
    }
    // TODO:: curerntly we can not have subvectors of different
    // Dimensions because of this
    // Dimensions for each centroid array the subvector dimension
    codebook_dims[ 2 ] = codebook_dim;
    codebook_datums = (Datum *)palloc(sizeof(Datum) * codebook_number_of_elements);

    for(i = 0; i < subvector_cnt; i++) {
        for(j = 0; j < cluster_cnt; j++) {
            for(n = 0; n < codebooks[ i ]->dim; n++) {
                codebook_datums[ codebook_elem_size++ ] = Float4GetDatum(codebooks[ i ]->centroids[ j ][ n ]);
            }
        }
    }

    array = construct_md_array(
        codebook_datums, NULL, 3, codebook_dims, codebook_lbs, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

    PG_RETURN_POINTER(array);
}
