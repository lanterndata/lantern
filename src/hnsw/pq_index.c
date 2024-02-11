#include <postgres.h>

#include "pq_index.h"

#include <access/heapam.h>
#include <access/relscan.h>
#include <access/table.h>
#include <assert.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>

#include "utils.h"

float *pq_codebook(Relation index, size_t vector_dimensions, size_t *out_num_centroids, size_t *out_num_subvectors)
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
     *          Storage as a List of Concatenated Vectors (Codebook):
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
     *            | | Subvector N.1| Subvector N.2|  ...        |Subvector N.M | |    pq_codebook[opts.dimenstions *
     * sizeof(float) *(N-1)] | +--------------+--------------+             +--------------+ |
     *            +---------------------------------------------------------------+
     *
     *          Each "Vector" represents the concatenation of all M subvectors
     *          for a centroid of a given ID. Subvectors are consecutive in memory
     * */
    float *codebook = (float4 *)palloc0(vector_dimensions * sizeof(float4) * 256);

    const int MAX_PQ_RELNAME_SIZE = 400;
#define COOKBOOK_RELATION_FMT "_lantern_codebook_%s"
    char   pq_relname[ MAX_PQ_RELNAME_SIZE ];
    char  *relname = get_rel_name(index->rd_index->indrelid);
    size_t formatted_pq_len = snprintf(pq_relname, MAX_PQ_RELNAME_SIZE, COOKBOOK_RELATION_FMT, relname);
    if(formatted_pq_len >= MAX_PQ_RELNAME_SIZE) {
        // todo:: test this
        elog(ERROR, "formatted codebook table name is too long");
    }
    // assuming the index and the cookbook have the same namespace
    Oid pq_oid = get_relname_relid(pq_relname, RelationGetNamespace(index));
    if(pq_oid == InvalidOid) {
        elog(ERROR, "PQ-codebook for relation \"%s\" not found", relname);
    }
    Relation      pq_rel;
    TableScanDesc pq_scan;
    HeapTuple     pq_tuple;

#if PG_VERSION_NUM < 120000
    pq_rel = heap_open(pq_oid, AccessShareLock);
#else
    pq_rel = table_open(pq_oid, AccessShareLock);
#endif
    TupleDesc pq_tuple_desc = RelationGetDescr(pq_rel);
    Snapshot  snapshot = GetTransactionSnapshot();
#if PG_VERSION_NUM < 120000
    pq_scan = heap_beginscan(heap, snapshot, 0, NULL);
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

        // elog(INFO,
        //      "N%d: reading subvector_id: %d centroid_id: %d, subvector dim: %d 3: [%.2f, %.2f, ...]",
        //      i++,
        //      subvector_id,
        //      centroid_id,
        //      subvector_dim,
        //      subvector[ 0 ],
        //      subvector[ 1 ]);

        // todo:: use a data structure here
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
    // for(int i = 0; i < opts.dimensions * 256; i++) {
    //     if(codebook[ i ] == 0) {
    //         elog(WARNING,
    //              "codebook at centroid id: %d subvector id: %d is zero",
    //              i / (opts.dimensions),
    //              i % (opts.dimensions) / subvector_dim);
    //     }
    // }
    return codebook;
}
