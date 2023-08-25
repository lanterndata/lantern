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
#include "retriever.h"
#include "usearch.h"
#include "utils.h"
#include "vector.h"

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
    MemoryContext          oldCtx;
    MemoryContext          insertCtx;
    Datum                  inserted_vector;
    usearch_index_t        uidx;
    usearch_error_t        error = NULL;
    usearch_metadata_t     meta;
    BlockNumber            HEADER_BLOCK = 0;
    Buffer                 hdr_buf;
    Page                   hdr_page;
    HnswIndexHeaderPage   *hdr;
    GenericXLogState      *state;
    uint32                 new_tuple_id;
    HnswIndexTuple        *new_tuple;
    usearch_init_options_t opts = {0};
    LDB_UNUSED(heap);
    LDB_UNUSED(indexInfo);
#if PG_VERSION_NUM >= 140000
    LDB_UNUSED(indexUnchanged);
#endif

        HnswInsertState *insertstate
        = palloc0(sizeof(HnswInsertState));

    if(checkUnique != UNIQUE_CHECK_NO) {
        elog(ERROR, "unique constraints on hnsw vector indexes not supported");
    }

    // q:: what are realistic cases where the vector fields would be null, other than the case
    // todo:: where the column is created and not all embeddings have been computed yet?
    // perhaps we should add a null bitmap to the index and support WHERE queries for exact null lookups?
    if(isnull[ 0 ]) {
        return false;
    }
    // todo:: thre is room for optimization for when indexUnchanged is true

    insertCtx = AllocSetContextCreate(CurrentMemoryContext, "LanternInsertContext", ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    state = GenericXLogStart(index);

    //  read index header page to know how many pages are already inserted
    hdr_buf = ReadBufferExtended(index, MAIN_FORKNUM, HEADER_BLOCK, RBM_NORMAL, NULL);
    LockBuffer(hdr_buf, BUFFER_LOCK_EXCLUSIVE);
    // header page MUST be under WAL since PrepareIndexTuple will update it
    hdr_page = GenericXLogRegisterBuffer(state, hdr_buf, LDB_GENERIC_XLOG_DELTA_IMAGE);
    hdr = (HnswIndexHeaderPage *)PageGetContents(hdr_page);
    assert(hdr->magicNumber == LDB_WAL_MAGIC_NUMBER);

    opts.dimensions = GetHnswIndexDimensions(index);
    CheckHnswIndexDimensions(index, values[ 0 ], opts.dimensions);
    PopulateUsearchOpts(index, &opts);
    opts.retriever_ctx = ldb_wal_retriever_area_init(index, hdr);
    opts.retriever = ldb_wal_index_node_retriever;
    opts.retriever_mut = ldb_wal_index_node_retriever_mut;

    // todo:: do usearch init in indexInfo->ii_AmCache
    uidx = usearch_init(&opts, &error);
    if(uidx == NULL) {
        elog(ERROR, "unable to initialize usearch");
    }
    assert(!error);

    assert(usearch_size(uidx, &error) == 0);
    assert(!error);

    usearch_view_mem_lazy(uidx, hdr->usearch_header, &error);
    assert(!error);

    insertstate->uidx = uidx;
    insertstate->retriever_ctx = opts.retriever_ctx;

    hdr_page = NULL;

    meta = usearch_metadata(uidx, &error);
    assert(!error);

    inserted_vector = PointerGetDatum(PG_DETOAST_DATUM(values[ 0 ]));

#if LANTERNDB_COPYNODES
    // currently not fully ported to the latest changes
    assert(false);
#else
    assert(insertstate->retriever_ctx->takenbuffers_next == 0);
#endif

    assert(hdr->magicNumber == LDB_WAL_MAGIC_NUMBER);
    elog(DEBUG5, "Insert: at start num vectors is %d", hdr->num_vectors);

    usearch_reserve(uidx, hdr->num_vectors + 1, &error);
    uint32 level = usearch_newnode_level(uidx, &error);
    if(error != NULL) {
        elog(ERROR, "usearch newnode error: %s", error);
    }

    new_tuple_id = hdr->num_vectors;
    // we are adding the following pages to the Generic XLog
    // 1) the header page
    // 2) the page containing the new tuple
    // 3) (sometimes) the page that used to be last page of the index
    // 4) The blockmap page for the block in which the vector was added
    // Generic XLog supports up to 4 pages in a single commit, so we are good.
    new_tuple = PrepareIndexTuple(index, state, hdr, &meta, new_tuple_id, level, insertstate);

    usearch_add_external(uidx,
                         *(unsigned long *)heap_tid,
                         DatumGetVector(inserted_vector)->x,
                         new_tuple->node,
                         usearch_scalar_f32_k,
                         level,
                         &error);
    if(error != NULL) {
        elog(ERROR, "usearch insert error: %s", error);
    }

    usearch_update_header(uidx, hdr->usearch_header, &error);

    ldb_wal_retriever_area_reset(insertstate->retriever_ctx, hdr);

    // we only release the header buffer AFTER inserting is finished to make sure nobody else changes the block
    // structure. todo:: critical section here can definitely be shortened
    {
        XLogRecPtr ptr = GenericXLogFinish(state);
        assert(ptr != InvalidXLogRecPtr);
        LDB_UNUSED(ptr);
    }

    extra_dirtied_release_all(insertstate->retriever_ctx->extra_dirted);

    usearch_free(insertstate->uidx, &error);
    if(error != NULL) {
        elog(ERROR, "error freeing usearch index: %s", error);
    }

    // unlock the header page
    assert(BufferIsValid(hdr_buf));
    MarkBufferDirty(hdr_buf);
    UnlockReleaseBuffer(hdr_buf);

    ldb_wal_retriever_area_fini(insertstate->retriever_ctx);
    pfree(insertstate);

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
