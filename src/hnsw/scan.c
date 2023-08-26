#include <postgres.h>

#include "scan.h"

#include <access/relscan.h>
#include <pgstat.h>
#include <utils/rel.h>

#include "bench.h"
#include "build.h"
#include "external_index.h"
#include "hnsw.h"
#include "options.h"
#include "retriever.h"
#include "vector.h"

/*
 * Prepare for an index scan
 */
IndexScanDesc ldb_ambeginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc          scan;
    HnswScanState         *scanstate;
    int                    dimensions;
    usearch_error_t        error = NULL;
    usearch_init_options_t opts;
    RetrieverCtx          *retriever_ctx = ldb_wal_retriever_area_init(index, NULL);

    elog(INFO, "began scanning with %d keys and %d orderbys", nkeys, norderbys);
    scan = RelationGetIndexScan(index, nkeys, norderbys);

    dimensions = GetHnswIndexDimensions(index);
    scanstate = (HnswScanState *)palloc0(sizeof(HnswScanState));
    scanstate->first = true;

    opts.connectivity = HnswGetM(index);
    opts.dimensions = dimensions;
    opts.expansion_add = HnswGetEfConstruction(index);
    opts.expansion_search = HnswGetEf(index);
    opts.metric_kind = HnswGetMetricKind(index);
    opts.metric = NULL;
    opts.quantization = usearch_scalar_f32_k;

    scanstate->retriever_ctx = opts.retriever_ctx = retriever_ctx;
    opts.retriever = ldb_wal_index_node_retriever;
    opts.retriever_mut = ldb_wal_index_node_retriever_mut;

    elog(INFO,
         "starting scan with dimensions=%d M=%ld efConstruction=%ld ef=%ld",
         dimensions,
         opts.connectivity,
         opts.expansion_add,
         opts.expansion_search);

    scanstate->usearch_index = usearch_init(&opts, &error);
    if(error != NULL) elog(ERROR, "error loading index: %s", error);
    assert(error == NULL);

    // ** initialize usearch data structures and set up external retriever
    Buffer               buf;
    Page                 page;
    char                *usearch_mem;
    HnswIndexHeaderPage *headerp;
    // index header is always at BlockNumber blockno = 0
    BlockNumber header_blockno = 0;

    if(!BlockNumberIsValid(header_blockno)) {
        elog(ERROR,
             "usearch index not initalized and root "
             "block not valid");
    }

    assert(scan->indexRelation == index);
    buf = ReadBuffer(scan->indexRelation, header_blockno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    headerp = (HnswIndexHeaderPage *)PageGetContents(page);
    assert(headerp->magicNumber == LDB_WAL_MAGIC_NUMBER);

    memcpy(retriever_ctx->blockmap_page_group_index_cache,
           headerp->blockmap_page_group_index,
           sizeof(retriever_ctx->block_numbers_cache));
    retriever_ctx->header_page_under_wal = NULL;

    usearch_mem = headerp->usearch_header;
    // this reserves memory for internal structures,
    // including for locks according to size indicated in usearch_mem
    usearch_view_mem_lazy(scanstate->usearch_index, usearch_mem, &error);
    assert(error == NULL);
    UnlockReleaseBuffer(buf);
    elog(INFO, "usearch index initialized");

    scan->opaque = scanstate;
    return scan;
}

/*
 * End a scan and release resources
 */
void ldb_amendscan(IndexScanDesc scan)
{
    HnswScanState *scanstate = (HnswScanState *)scan->opaque;

    // todo:: once VACUUM/DELETE are implemented, during scan we need to hold a pin
    //  on the buffer we have last returned.
    //  make sure to release that pin here

#ifdef LANTERN_USE_LIBHNSW
    if(scanstate->hnsw) hnsw_destroy(scanstate->hnsw);
#endif
#ifdef LANTERN_USE_USEARCH
    if(scanstate->usearch_index) {
        usearch_error_t error = NULL;
        usearch_free(scanstate->usearch_index, &error);
        ldb_wal_retriever_area_fini(scanstate->retriever_ctx);
        assert(error == NULL);
    }
#else
    elog(ERROR, "no index implementation specified");
#endif

    if(scanstate->distances) pfree(scanstate->distances);

    if(scanstate->labels) pfree(scanstate->labels);

    pfree(scanstate);
    scan->opaque = NULL;
}

/*
 * Restart a scan
 * from docs: In practice the restart feature is used when a new outer tuple is
 * selected by a nested-loop join and so a new key comparison value is needed,
 * but the scan key structure remains the same.
 */
void ldb_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    HnswScanState *scanstate = (HnswScanState *)scan->opaque;
    scanstate->first = true;
    LDB_UNUSED(norderbys);
    LDB_UNUSED(nkeys);

    // q:: why is this the user's responsibility?
    if(keys && scan->numberOfKeys > 0) memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

    if(orderbys && scan->numberOfOrderBys > 0)
        memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Fetch the next tuple in the given scan
 */
bool ldb_amgettuple(IndexScanDesc scan, ScanDirection dir)
{
    HnswScanState *scanstate = (HnswScanState *)scan->opaque;
    ItemPointer    tid;
    LDB_UNUSED(dir);

    // posgres does not allow backwards scan on operators
    // (todo:: look into this andcite? took from pgvector)
    // unclear how one would do backwards scan with hnsw algorithm
    // the graph is contructed with links to nearest neighbors and no info
    // about the furtheest neighbors
    Assert(ScanDirectionIsForward(dir));

    if(scanstate->first) {
        int             num_returned;
        Datum           value;
        Vector         *vec;
        usearch_error_t error = NULL;
        int             k = ldb_hnsw_init_k;

        /* Count index scan for stats */
        pgstat_count_index_scan(scan->indexRelation);

        /* Safety check */
        if(scan->orderByData == NULL) elog(ERROR, "cannot scan hnsw index without order");

        /* No items will match if null */
        if(scan->orderByData->sk_flags & SK_ISNULL) return false;

        // todo:: sk_subtype, sk_collation, sk_func. what are they?

        value = scan->orderByData->sk_argument;

        /* Value should not be compressed or toasted */
        Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
        Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

        vec = DatumGetVector(value);

        if(scanstate->distances == NULL) {
            scanstate->distances = palloc(k * sizeof(float));
        }
        if(scanstate->labels == NULL) {
            scanstate->labels = palloc(k * sizeof(usearch_label_t));
        }

        // hnsw_search(scanstate->hnsw, vec->x, k, &num_returned, scanstate->distances, scanstate->labels);
        elog(DEBUG5, "querying index for %d elements", k);
        num_returned = usearch_search(
            scanstate->usearch_index, vec->x, usearch_scalar_f32_k, k, scanstate->labels, scanstate->distances, &error);
        ldb_wal_retriever_area_reset(scanstate->retriever_ctx, NULL);

        scanstate->count = num_returned;
        scanstate->current = 0;

        scanstate->first = false;

        /* Clean up if we allocated a new value */
        if(value != scan->orderByData->sk_argument) pfree(DatumGetPointer(value));
    }

    if(scanstate->current == scanstate->count) {
        int             num_returned;
        Datum           value;
        Vector         *vec;
        usearch_error_t error = NULL;
        int             k = scanstate->count * 2;

        if(usearch_size(scanstate->usearch_index, &error) == scanstate->current) {
            return false;
        }

        value = scan->orderByData->sk_argument;

        vec = DatumGetVector(value);

        /* double k and reallocate arrays to account for increased size */
        scanstate->distances = repalloc(scanstate->distances, k * sizeof(float));
        scanstate->labels = repalloc(scanstate->labels, k * sizeof(usearch_label_t));

        elog(DEBUG5, "querying index for %d elements", k);
        num_returned = usearch_search(
            scanstate->usearch_index, vec->x, usearch_scalar_f32_k, k, scanstate->labels, scanstate->distances, &error);
        ldb_wal_retriever_area_reset(scanstate->retriever_ctx, NULL);

        scanstate->count = num_returned;

        /* Clean up if we allocated a new value */
        if(value != scan->orderByData->sk_argument) pfree(DatumGetPointer(value));
    }

    if(scanstate->current < scanstate->count) {
        unsigned long int label = scanstate->labels[ scanstate->current ];
        scanstate->iptr = (ItemPointer)&label;

        tid = scanstate->iptr;

#if PG_VERSION_NUM >= 120000
        scan->xs_heaptid = *tid;
#else
        scan->xs_ctup.t_self = *tid;
#endif

        // todo:: there is a mid-sized designed issue with index storage
        // labels must be large enought to store relblockno+ indexblockno
        // currently they only store relblockno
        // the second is needed so we can hold a pin in here on the index page
        // the good news is that this is not an issue until we support deletions
        // Now we add support for deletions via a bitmap in blockmap page
        // So, the actual index pages are append-only which means effectively we
        // always have a static pin on all index pages
        // The issue outlined above needs to be addressed but it is not critical
        // until we physically compact index pages

        // if (BufferIsValid(scanstate->buf))
        // 	ReleaseBuffer(scanstate->buf);

        // /*
        //  * An index scan must maintain a pin on the index page holding the
        //  * item last returned by amgettuple
        //  *
        //  * https://www.postgresql.org/docs/current/index-locking.html
        //  */
        // scanstate->buf = ReadBuffer(scan->indexRelation, indexblkno);
        scanstate->current++;
        scan->xs_recheckorderby = false;
        return true;
    }

    return false;
}
