#include <postgres.h>

#include "insert.h"

#include <access/generic_xlog.h>
#include <assert.h>
#include <float.h>
#include <storage/bufmgr.h>
#include <utils/rel.h>
#include <utils/relcache.h>

#include "external_index.h"
#include "hnsw.h"
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
    Datum                  vector;
    usearch_init_options_t opts;
    usearch_index_t        uidx;
    usearch_error_t        error = NULL;
    usearch_metadata_t     meta;
    BlockNumber            HEADER_BLOCK = 0;
    BlockNumber            last_block;
    Buffer                 hdr_buf;
    Page                   hdr_page;
    HnswIndexHeaderPage   *hdr;
    int                    current_size;
    GenericXLogState      *state;
    Buffer                 extra_dirtied[ LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS ];
    int                    extra_dirtied_size = 0;

    if(checkUnique != UNIQUE_CHECK_NO) {
        elog(ERROR, "unique constraints on hnsw vector indexes not supported");
    }

    // q:: what are realistic cases where the vector fields would be null, other than the case
    // todo:: where the column is created and not all embeddings have been computed yet?
    // perhaps we should add a null bitmap to the index and support WHERE queries for exact null lookups?
    if(isnull[ 0 ]) return false;

    insertCtx = AllocSetContextCreate(CurrentMemoryContext, "LanternInsertContext", ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    vector = PointerGetDatum(PG_DETOAST_DATUM(values[ 0 ]));

    opts.dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;
    PopulateUsearchOpts(index, &opts);

    // todo:: pass in all the additional init info for external retreiver like index size (that's all?)
    uidx = usearch_init(&opts, &error);
    meta = usearch_metadata(uidx, &error);
    // USE an INSERT retriever for the above. the difference will be that the retriever will obtain exclusive locks
    // on all pages (in case it is modified by US or another thread)
    usearch_set_node_retriever(uidx, &ldb_wal_index_node_retriever, &ldb_wal_index_node_retriever, &error);

    assert(usearch_size(uidx, &error) == 0);
    assert(!error);

    state = GenericXLogStart(index);

    //  read index header page to know how many pages are already isnerted
    hdr_buf = ReadBufferExtended(index, MAIN_FORKNUM, HEADER_BLOCK, RBM_NORMAL, NULL);
    LockBuffer(hdr_buf, BUFFER_LOCK_EXCLUSIVE);
    hdr_page = GenericXLogRegisterBuffer(state, hdr_buf, LDB_GENERIC_XLOG_DELTA_IMAGE);
    hdr = (HnswIndexHeaderPage *)PageGetContents(hdr_page);

    current_size = hdr->num_vectors;

    if(current_size >= HNSW_MAX_INDEXED_VECTORS) {
        elog(ERROR, "Index full. Cannot add more vectors. Current limit: %d", HNSW_MAX_INDEXED_VECTORS);
    }

    // this reserves memory for internal structures,
    // including for locks according to size indicated in usearch_mem
    usearch_view_mem_lazy(uidx, hdr->usearch_header, &error);

    usearch_reserve(uidx, current_size + 1, &error);
    //  ^^ do not worry about allocaitng locks above. but that has to be eliminated down the line
    int level = usearch_newnode_level(uidx, &error);
    if(error != NULL) {
        elog(ERROR, "usearch newnode error: %s", error);
    }

    // reserve the added page on disk using level info from above
    // hdr is passed in so num_vectors, first_block_no, last_block_no can be updated
    ReserveIndexTuple(index, state, hdr, &meta, level, &extra_dirtied, &extra_dirtied_size);
    elog(WARNING, "finished reserving index tuple");

    usearch_add_external(uidx, *(unsigned long *)heap_tid, DatumGetVector(vector)->x, usearch_scalar_f32_k, 0, &error);
    if(error != NULL) {
        elog(ERROR, "usearch insert error: %s", error);
    }

    usearch_free(uidx, &error);

    MarkBufferDirty(hdr_buf);
    // we only release the header buffer AFTER inserting is finished to make sure nobody else changes the block
    // structure. todo:: critical section here can definitely be shortened
    GenericXLogFinish(state);
    for(int i = 0; i < extra_dirtied_size; i++) {
        assert(BufferIsValid(extra_dirtied[ i ]));
        // header is not considered extra. we know we should not have dirtied it
        // sanity check callees that manimulate extra_dirtied did not violate this
        assert(extra_dirtied[ i ] != hdr_buf);
        MarkBufferDirty(extra_dirtied[ i ]);
        UnlockReleaseBuffer(extra_dirtied[ i ]);
    }

    UnlockReleaseBuffer(hdr_buf);

    // todo:: thre is room for optimization for when indexUnchanged is true
    // InsertTuple(index, values, isnull, heap_tid, indexInfo);
    // ExternalIndexInsert(index, vector, heap_tid);
    // read header page, get num_vectors, go to the BLockNumber storing the last block
    // use this block as an insert page

    // q:: what happens when there is an error before ths and the switch back never happens?
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    elog(INFO, "hnsw insert");
    elog(ERROR, "not implemented");

    return false;
}
