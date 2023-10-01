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
#include "utils.h"
#include "vector.h"

bool ldb_amcanreturn(Relation indexRelation, int attno)
{
    elog(LOG, "ldb_amcanreturn(..., %d)", attno);
    LDB_UNUSED(indexRelation);
    return true;
}

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

    elog(LOG, "ldb_ambeginscan()");
    scan = RelationGetIndexScan(index, nkeys, norderbys);

    // ** initialize usearch data structures and set up external retriever
    Buffer               buf;
    Page                 page;
    char                *usearch_mem;
    HnswIndexHeaderPage *headerp;
    // index header is always at BlockNumber blockno = 0
    BlockNumber header_blockno = 0;

    ldb_invariant(BlockNumberIsValid(header_blockno), "invalid hnsw header blockno");

    assert(scan->indexRelation == index);
    buf = ReadBuffer(scan->indexRelation, header_blockno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    headerp = (HnswIndexHeaderPage *)PageGetContents(page);
    assert(headerp->magicNumber == LDB_WAL_MAGIC_NUMBER);

    // Initialize usearch index options based on params stored in our index header
    dimensions = headerp->vector_dim;

    opts.connectivity = headerp->m;
    opts.expansion_add = headerp->ef_construction;
    opts.expansion_search = headerp->ef;
    opts.metric_kind = headerp->metric_kind;
    opts.metric = NULL;
    opts.quantization = usearch_scalar_f32_k;

    scanstate = (HnswScanState *)palloc0(sizeof(HnswScanState));
    scanstate->first = true;
    scanstate->retriever_ctx = opts.retriever_ctx = retriever_ctx;
    scanstate->columnType = GetIndexColumnType(index);
    scanstate->dimensions = opts.dimensions = dimensions;
    scanstate->nonkey_tuple_desc = MakeNonkeyIndexTupleDesc(index);
    scanstate->current_arr_datum = palloc_array(Datum, scanstate->dimensions);
    opts.retriever = ldb_wal_index_node_retriever;
    opts.retriever_mut = ldb_wal_index_node_retriever_mut;

    ldb_dlog("starting scan with dimensions=%d M=%ld efConstruction=%ld ef=%ld",
             dimensions,
             opts.connectivity,
             opts.expansion_add,
             opts.expansion_search);

    scanstate->usearch_index = usearch_init(&opts, &error);
    if(error != NULL) elog(ERROR, "error loading index: %s", error);
    assert(error == NULL);

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

    elog(LOG, "ldb_amendscan()");
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

    if(scanstate->item_pointer_data) pfree(scanstate->item_pointer_data);

    pfree(scanstate->current_arr_datum);
    FreeTupleDesc(scanstate->nonkey_tuple_desc);
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
    LDB_UNUSED(dir);

    // posgres does not allow backwards scan on operators
    // (todo:: look into this andcite? took from pgvector)
    // unclear how one would do backwards scan with hnsw algorithm
    // the graph is contructed with links to nearest neighbors and no info
    // about the furtheest neighbors
    Assert(ScanDirectionIsForward(dir));

    elog(LOG, "ldb_amgettuple() xs_want_itup=%d", !!scan->xs_want_itup);
    if(scanstate->first) {
        int             num_returned;
        Datum           value;
        float4         *vec;
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

        vec = DatumGetSizedFloatArray(value, scanstate->columnType, scanstate->dimensions);

        if(scanstate->distances == NULL) {
            scanstate->distances = palloc(k * sizeof(float));
        }
        if(scanstate->labels == NULL) {
            scanstate->labels = palloc(k * sizeof(usearch_label_t));
        }
        if(scanstate->item_pointer_data == NULL) {
            scanstate->item_pointer_data = palloc(k * sizeof(ItemPointerData));
        }

        ldb_dlog("LANTERN querying index for %d elements", k);
        num_returned = usearch_search(
            scanstate->usearch_index, vec, usearch_scalar_f32_k, k, scanstate->labels, scanstate->distances, &error);
        ldb_wal_retriever_area_reset(scanstate->retriever_ctx, NULL);

        for (int i = 0; i < num_returned; ++i)
            UsearchLabel2ItemPointer(scanstate->labels[i], &scanstate->item_pointer_data[i]);

        scanstate->count = num_returned;
        scanstate->current = 0;

        scanstate->first = false;

        /* Clean up if we allocated a new value */
        if(value != scan->orderByData->sk_argument) pfree(DatumGetPointer(value));
    }

    if(scanstate->current == scanstate->count) {
        int             num_returned;
        Datum           value;
        float4         *vec;
        usearch_error_t error = NULL;
        int             k = scanstate->count * 2;
        int             index_size = usearch_size(scanstate->usearch_index, &error);
        assert(error == NULL);

        if(index_size == scanstate->current) {
            return false;
        }

        value = scan->orderByData->sk_argument;

        vec = DatumGetSizedFloatArray(value, scanstate->columnType, scanstate->dimensions);

        /* double k and reallocate arrays to account for increased size */
        scanstate->distances = repalloc(scanstate->distances, k * sizeof(float));
        scanstate->labels = repalloc(scanstate->labels, k * sizeof(usearch_label_t));
        scanstate->item_pointer_data = repalloc(scanstate->item_pointer_data, k * sizeof(ItemPointerData));

        ldb_dlog("LANTERN - querying index for %d elements", k);
        num_returned = usearch_search(
            scanstate->usearch_index, vec, usearch_scalar_f32_k, k, scanstate->labels, scanstate->distances, &error);
        ldb_wal_retriever_area_reset(scanstate->retriever_ctx, NULL);

        for (int i = 0; i < k; ++i)
            UsearchLabel2ItemPointer(scanstate->labels[i], &scanstate->item_pointer_data[i]);

        scanstate->count = num_returned;

        /* Clean up if we allocated a new value */
        if(value != scan->orderByData->sk_argument) pfree(DatumGetPointer(value));
    }

    if(scanstate->current < scanstate->count) {
        ItemPointer      item_ptr = &scanstate->item_pointer_data[ scanstate->current ];
        IndexTuple       itup;
        TupleDesc        itup_desc = scanstate->nonkey_tuple_desc;
        Buffer           buf;
        Page             page;
        HnswIndexTuple  *storage_tup;
        TupleDesc        tupleDesc = RelationGetDescr(scan->indexRelation);
        ItemPointerData  current_iptr_data;
        uint64           label;
        bool             first_isnull;


        elog(LOG, "ldb_amgettuple(): item_ptr=(%u, %hu)",
             ItemPointerGetBlockNumber(item_ptr), ItemPointerGetOffsetNumber(item_ptr));
        assert(itup_desc->natts == tupleDesc->natts);

        buf = ReadBuffer(scan->indexRelation, BlockIdGetBlockNumber(&item_ptr->ip_blkid));
        assert(buf != InvalidBuffer);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        storage_tup = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, item_ptr->ip_posid));
        itup = (IndexTuple)&storage_tup->node[storage_tup->size];

        if (scan->xs_want_itup) {
            Datum  *values;
            bool   *isnull;
            float4 *float_arr;

            // the array is at the end of the serialized node
            float_arr = (float4 *)&storage_tup->node[storage_tup->size - sizeof(float4) * scanstate->dimensions];

            values = palloc_array(Datum, itup_desc->natts);
            isnull = palloc_array(bool, itup_desc->natts);
            for (int i = 0; i < itup_desc->natts; ++i)
                values[i] = index_getattr(itup, i + 1, itup_desc, &isnull[i]);
            assert(!isnull[0]);
            values[0] = GetArrayFromFloats(float_arr, scanstate->current_arr_datum,
                                           scanstate->columnType, scanstate->dimensions);
            scan->xs_hitupdesc = tupleDesc;
            scan->xs_hitup = heap_form_tuple(scan->xs_hitupdesc, values, isnull);
            pfree(isnull);
            pfree(values);
        }

        label = DatumGetUInt64(index_getattr(itup, 1, itup_desc, &first_isnull));
        assert(!first_isnull);
        UsearchLabel2ItemPointer(label, &current_iptr_data);
        elog(LOG, "ldb_amgettuple(): current_iptr_data=(%u, %hu)",
             ItemPointerGetBlockNumber(&current_iptr_data), ItemPointerGetOffsetNumber(&current_iptr_data));

#if PG_VERSION_NUM >= 120000
        scan->xs_heaptid = current_iptr_data;
#else
        scan->xs_ctup.t_self = current_iptr_data;
#endif
        UnlockReleaseBuffer(buf);


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
