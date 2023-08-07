#include <postgres.h>

#include "insert.h"

#include <access/generic_xlog.h>
#include <assert.h>
#include <float.h>
#include <storage/bufmgr.h>
#include <utils/array.h>
#include <utils/rel.h>
#include <utils/relcache.h>

#include "build.h"
#include "external_index.h"
#include "hnsw.h"
#include "options.h"
#include "usearch.h"
#include "utils.h"
#include "vector.h"

/*
 * Context delete callback for insert context
 */
static void insert_done_cb(void *arg)
{
    usearch_index_t uidx = (usearch_index_t)arg;
    usearch_error_t error = NULL;

    usearch_free(uidx, &error);
    if(error != NULL) {
        elog(ERROR, "error freeing usearch index: %s", error);
    }
    ldb_wal_retriever_area_free();
}

/*
 * Insert a tuple into the index
 */
bool ldb_aminsert(Relation         index,
                  Datum           *values,
                  bool            *isnull,
                  ItemPointer      heap_tid,
                  Relation         heap,
                  IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                  ,
                  bool indexUnchanged
#endif
                  ,
                  IndexInfo *indexInfo)
{
    MemoryContext        oldCtx;
    MemoryContext        insertCtx;
    Datum                vector;
    usearch_index_t      uidx;
    usearch_error_t      error = NULL;
    usearch_metadata_t   meta;
    BlockNumber          HEADER_BLOCK = 0;
    BlockNumber          last_block;
    Buffer               hdr_buf;
    Page                 hdr_page;
    HnswIndexHeaderPage *hdr;
    int                  current_size;
    GenericXLogState    *state;
    Buffer               extra_dirtied[ LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS ];
    Page                 extra_dirtied_page[ LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS ];
    int                  new_tuple_size;
    uint32               new_tuple_id;
    HnswIndexTuple      *new_tuple;
    ArrayType           *array;
    int                  n_items;

    if(checkUnique != UNIQUE_CHECK_NO) {
        elog(ERROR, "unique constraints on hnsw vector indexes not supported");
    }

    // q:: what are realistic cases where the vector fields would be null, other than the case
    // todo:: where the column is created and not all embeddings have been computed yet?
    // perhaps we should add a null bitmap to the index and support WHERE queries for exact null lookups?
    if(isnull[ 0 ]) {
        return false;
    }

    // when there are multiple inserts in the query, avoid reinitializing some of the
    // data structures
    if(indexInfo->ii_AmCache == NULL) {
        usearch_init_options_t opts;
        MemoryContextCallback *callback;

        opts.dimensions = GetHnswIndexDimensions(index);
        CheckHnswIndexDimensions(index, values[ 0 ], opts.dimensions);
        PopulateUsearchOpts(index, &opts);

        //  read index header page to know how many pages are already inserted
        hdr_buf = ReadBuffer(index, HEADER_BLOCK);
        LockBuffer(hdr_buf, BUFFER_LOCK_SHARE);
        // header page MUST be under WAL since PrepareIndexTuple will update it
        hdr_page = BufferGetPage(hdr_buf);
        hdr = (HnswIndexHeaderPage *)PageGetContents(hdr_page);
        assert(hdr->magicNumber == LDB_WAL_MAGIC_NUMBER);

        // todo:: pass in all the additional init info for external retreiver like index size (that's all?)
        uidx = usearch_init(&opts, &error);
        if(uidx == NULL) {
            elog(ERROR, "unable to initialize usearch");
        }
        assert(!error);
        INDEX_RELATION_FOR_RETRIEVER = index;
        HEADER_FOR_EXTERNAL_RETRIEVER = *hdr;
        EXTRA_DIRTIED = &extra_dirtied[ 0 ];
        EXTRA_DIRTIED_PAGE = &extra_dirtied_page[ 0 ];
        EXTRA_DIRTIED_SIZE = 0;
        ldb_wal_retriever_area_init(BLCKSZ * 100);
        usearch_set_node_retriever(uidx, &ldb_wal_index_node_retriever, &ldb_wal_index_node_retriever_mut, &error);

        assert(usearch_size(uidx, &error) == 0);
        assert(!error);

        // this reserves memory for internal structures,
        // including for locks according to size indicated in usearch_mem
        //  ^^ do not worry about allocaitng locks above. but that has to be eliminated down the line
        usearch_view_mem_lazy(uidx, hdr->usearch_header, &error);
        assert(!error);

        indexInfo->ii_AmCache = uidx;

        callback
            = (MemoryContextCallback *)MemoryContextAllocZero(indexInfo->ii_Context, sizeof(MemoryContextCallback));
        callback->func = insert_done_cb;
        callback->arg = (void *)uidx;
        MemoryContextRegisterResetCallback(indexInfo->ii_Context, callback);

        UnlockReleaseBuffer(hdr_buf);
        // reset hdr related vars to make sure the rest of the code does not depend on them
        // as here we read everything in read-only mode while the rest of the code will likely
        // need exclusive access to the header page
        hdr_buf = InvalidBuffer;
        hdr_page = NULL;
        hdr = NULL;
    }

    uidx = (usearch_index_t)indexInfo->ii_AmCache;
    meta = usearch_metadata(uidx, &error);
    assert(!error);

    insertCtx = AllocSetContextCreate(CurrentMemoryContext, "LanternInsertContext", ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    vector = PointerGetDatum(PG_DETOAST_DATUM(values[ 0 ]));

    state = GenericXLogStart(index);

    //  read index header page to know how many pages are already inserted
    hdr_buf = ReadBufferExtended(index, MAIN_FORKNUM, HEADER_BLOCK, RBM_NORMAL, NULL);
    LockBuffer(hdr_buf, BUFFER_LOCK_EXCLUSIVE);
    // header page MUST be under WAL since PrepareIndexTuple will update it
    hdr_page = GenericXLogRegisterBuffer(state, hdr_buf, LDB_GENERIC_XLOG_DELTA_IMAGE);
    hdr = (HnswIndexHeaderPage *)PageGetContents(hdr_page);

    assert(hdr->magicNumber == LDB_WAL_MAGIC_NUMBER);
    elog(DEBUG5, "Insert: at start num vectors is %d", hdr->num_vectors);

    current_size = hdr->num_vectors;

    if(current_size >= HNSW_MAX_INDEXED_VECTORS) {
        elog(ERROR, "Index full. Cannot add more vectors. Current limit: %d", HNSW_MAX_INDEXED_VECTORS);
    }

    usearch_reserve(uidx, current_size + 1, &error);
    int level = usearch_newnode_level(uidx, &error);
    if(error != NULL) {
        elog(ERROR, "usearch newnode error: %s", error);
    }

    // todo:: ensure that generic xlog has enough space
    // MAX_GENERIC_XLOG_PAGES
    // XLogEnsureRecordSpace(4, 20);
    new_tuple_id = hdr->num_vectors;
    new_tuple = PrepareIndexTuple(
        index, state, hdr, &meta, new_tuple_id, level, &extra_dirtied, &extra_dirtied_page, &EXTRA_DIRTIED_SIZE);

    usearch_add_external(uidx,
                         *(unsigned long *)heap_tid,
                         DatumGetVector(values[ 0 ])->x,
                         new_tuple->node,
                         usearch_scalar_f32_k,
                         level,
                         &error);
    if(error != NULL) {
        elog(ERROR, "usearch insert error: %s", error);
    }

    usearch_update_header(uidx, hdr->usearch_header, &error);

    ldb_wal_retriever_area_reset();

    MarkBufferDirty(hdr_buf);
    // we only release the header buffer AFTER inserting is finished to make sure nobody else changes the block
    // structure. todo:: critical section here can definitely be shortened
    {
        XLogRecPtr ptr = GenericXLogFinish(state);
        assert(ptr != InvalidXLogRecPtr);
    }

    for(int i = 0; i < EXTRA_DIRTIED_SIZE; i++) {
        assert(BufferIsValid(extra_dirtied[ i ]));
        // header is not considered extra. we know we should not have dirtied it
        // sanity check callees that manimulate extra_dirtied did not violate this
        assert(extra_dirtied[ i ] != hdr_buf);
        MarkBufferDirty(extra_dirtied[ i ]);
        UnlockReleaseBuffer(extra_dirtied[ i ]);
    }
    EXTRA_DIRTIED_SIZE = 0;

    UnlockReleaseBuffer(hdr_buf);

    // todo:: thre is room for optimization for when indexUnchanged is true
    // InsertTuple(index, values, isnull, heap_tid, indexInfo);
    // ExternalIndexInsert(index, vector, heap_tid);
    // read header page, get num_vectors, go to the BLockNumber storing the last block
    // use this block as an insert page

    // q:: what happens when there is an error before ths and the switch back never happens?
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    // from docs at https://www.postgresql.org/docs/current/index-functions.html:
    // The function's Boolean result value is significant only when checkUnique is UNIQUE_CHECK_PARTIAL.
    // In this case a true result means the new entry is known unique, whereas false means it might be
    // non-unique (and a deferred uniqueness check must be scheduled).
    // For other cases a constant false result is recommended.
    return false;
}
