#include <postgres.h>

#include <access/heapam.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <hnsw.h>
#include <miscadmin.h>
#include <storage/bufmgr.h>
#include <utils/lsyscache.h>

#if PG_VERSION_NUM <= 120000
#include <access/htup_details.h>
#include <utils/rel.h>
#endif

#include "hnsw/product_quantization.h"
#include "hnsw/utils.h"
#include "usearch.h"

#if PG_VERSION_NUM < 130000
#define TYPALIGN_INT 'i'
#endif

#define COOKBOOK_RELATION_FMT "pq_%s_%s"

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
    uint32                dataset_size_limit = PG_GETARG_UINT32(5);
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

    if(dataset_size_limit > 0 && dataset_size_limit < cluster_cnt) {
        elog(ERROR, "Dataset size limit should be greater or equal to cluster_cnt count");
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
                heap_endscan(scan);
                relation_close(table, AccessShareLock);
                elog(ERROR, "Dataset dimensions should be divisible by subvector count");
            }
        } else if(current_tuple_dim != dataset_dim) {
            heap_endscan(scan);
            relation_close(table, AccessShareLock);
            elog(ERROR, "Table should have equally sized array: expected %d got %d", dataset_dim, current_tuple_dim);
        }

        dataset[ dataset_size++ ] = (float4 *)ARR_DATA_PTR(array);

        if(dataset_size_limit > 0 && dataset_size == dataset_size_limit) {
            break;
        }

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

float *load_pq_codebook(Relation index, size_t vector_dimensions, size_t *out_num_centroids, size_t *out_num_subvectors)
{
    /*
     *
     *          Original Dataset of Vectors
     *                    |
     *                    | PQ-Quantization
     *                    v
     *          Split into M Subvectors -> Quantize each to nearest Centroid in Subspace
     *
     *          Quantized Vectors (Codebook Structure):
     *            +----------------+----------------+       +----------------+
     *            | Centroid ID: 1 | Centroid ID: 2 |  ...  | Centroid ID: N |
     *            +----------------+----------------+       +----------------+
     *            |+--------------+|+--------------+|       |+--------------+|
     *            || Subvector 1.1|| Subvector 2.1 ||       || Subvector N.1 ||
     *            |+--------------+|+--------------+|       |+--------------+|
     *            |+--------------+|+--------------+|       |+--------------+|
     *            || Subvector 1.2|| Subvector 2.2 ||       || Subvector N.2 ||
     *            |+--------------+|+--------------+|       |+--------------+|
     *            |       ...      |       ...      |       |       ...      |
     *            |+--------------+|+--------------+|       |+--------------+|
     *            ||Subvector 1.M || Subvector 2.M ||       || Subvector N.M ||
     *            |+--------------+|+--------------+|       |+--------------+|
     *            +----------------+----------------+       +----------------+
     *
     *          In the returned float array here, we merge all subvectors together,
     *          to have a list of NUM_CENTROID vectors, each comprising of
     *          NUM_SUBVECTORS equidimensional subvector components
     *          Storage as a List of concatenated subvectors:
     *            +---------------------------------------------------------------+
     *            | Vector 1 (Centroid ID: 1)                                     |
     *            | +--------------+--------------+             +--------------+ |
     *            | | Subvector 1.1| Subvector 1.2|  ...        |Subvector 1.M | |
     *            | +--------------+--------------+             +--------------+ |
     *            +---------------------------------------------------------------+
     *            | Vector 2 (Centroid ID: 2)                                     |
     *            | +--------------+--------------+             +--------------+ |
     *            | | Subvector 2.1| Subvector 2.2|  ...        |Subvector 2.M | |
     *            | +--------------+--------------+             +--------------+ |
     *            +---------------------------------------------------------------+
     *                                            ...
     *            +---------------------------------------------------------------+
     *            | Vector N (Centroid ID: N)                                     |
     *            | +--------------+--------------+             +--------------+ |
     *            | | Subvector N.1| Subvector N.2|  ...        |Subvector N.M | |
     *            | +--------------+--------------+             +--------------+ |
     *            +---------------------------------------------------------------+
     *
     *          Each "Vector" represents the concatenation of all M subvectors
     *          for a centroid of a given ID. Subvectors are consecutive in memory
     * */
    float *codebook = (float4 *)palloc0(vector_dimensions * sizeof(float4) * 256);

    Relation  pq_rel;
    HeapTuple pq_tuple;
    char      pq_relname[ NAMEDATALEN ];
    char     *relname = get_rel_name(index->rd_index->indrelid);
    int16     attrNum = index->rd_index->indkey.values[ 0 ];
    // take attrNum of parent table, and lookup its name on the table being indexed
    char *colname = get_attname(index->rd_index->indrelid, attrNum, true);
    int   formatted_pq_len;
#if PG_VERSION_NUM < 120000
    HeapScanDesc pq_scan;
#else
    TableScanDesc pq_scan;
#endif

    if(relname == NULL) {
        elog(ERROR, "index relation not found");
    }

    if(colname == NULL) {
        elog(ERROR, "vector column not found");
    }

    formatted_pq_len = snprintf(pq_relname, NAMEDATALEN, COOKBOOK_RELATION_FMT, relname, colname);

    if(formatted_pq_len >= NAMEDATALEN) {
        elog(ERROR, "formatted codebook table name is too long");
    }
    // assuming the index and the cookbook have the same namespace
    Oid pq_oid = get_relname_relid(pq_relname, LookupNamespaceNoError(LANTERN_INTERNAL_SCHEMA_NAME));
    if(pq_oid == InvalidOid) {
        elog(ERROR, "PQ-codebook for relation \"%s\" not found", relname);
    }

#if PG_VERSION_NUM < 120000
    pq_rel = heap_open(pq_oid, AccessShareLock);
#else
    pq_rel = table_open(pq_oid, AccessShareLock);
#endif
    TupleDesc pq_tuple_desc = RelationGetDescr(pq_rel);
    Snapshot  snapshot = GetTransactionSnapshot();
#if PG_VERSION_NUM < 120000
    pq_scan = heap_beginscan(pq_rel, snapshot, 0, NULL);
#else
    pq_scan = heap_beginscan(pq_rel, snapshot, 0, NULL, NULL, SO_TYPE_SEQSCAN);
#endif

    bool isNull = false;
    int  subvector_dim;
    int  num_centroids = 0;
    while((pq_tuple = heap_getnext(pq_scan, ForwardScanDirection)) != NULL) {
        Datum   pq_cols[ 3 ];
        int     centroid_id;
        int     subvector_id;
        float4 *subvector;

        pq_cols[ 0 ] = heap_getattr(pq_tuple, 1, pq_tuple_desc, &isNull);
        assert(!isNull);
        pq_cols[ 1 ] = heap_getattr(pq_tuple, 2, pq_tuple_desc, &isNull);
        assert(!isNull);
        pq_cols[ 2 ] = heap_getattr(pq_tuple, 3, pq_tuple_desc, &isNull);
        assert(!isNull);

        subvector_id = DatumGetInt32(pq_cols[ 0 ]);
        centroid_id = DatumGetInt32(pq_cols[ 1 ]);
        ArrayType *v = DatumGetArrayTypeP(pq_cols[ 2 ]);
        subvector = ToFloat4Array(v, &subvector_dim);
        num_centroids++;

        memcpy(codebook + centroid_id * vector_dimensions + subvector_id * subvector_dim,
               subvector,
               subvector_dim * sizeof(float4));
    }
    // we counted each centroid for all subvectors
    int num_subvectors = (int)vector_dimensions / subvector_dim;
    num_centroids /= num_subvectors;
    *out_num_centroids = num_centroids;
    *out_num_subvectors = num_subvectors;

    heap_endscan(pq_scan);
#if PG_VERSION_NUM < 120000
    heap_close(pq_rel, AccessShareLock);
#else
    table_close(pq_rel, AccessShareLock);
#endif
    return codebook;
}
