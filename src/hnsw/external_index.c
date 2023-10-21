
#include <postgres.h>

#include "external_index.h"

#include <access/generic_xlog.h>  // GenericXLog
#include <assert.h>
#include <common/relpath.h>
#include <hnsw/fa_cache.h>
#include <pg_config.h>       // BLCKSZ
#include <storage/bufmgr.h>  // Buffer
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "extra_dirtied.h"
#include "htab_cache.h"
#include "insert.h"
#include "options.h"
#include "retriever.h"
#include "usearch.h"
#include "utils.h"

#if PG_VERSION_NUM >= 130000
#include <miscadmin.h>
#endif

static BlockNumber getBlockMapPageBlockNumber(const HnswBlockMapGroupDesc *blockmap_groups, int id);

uint32 UsearchNodeBytes(usearch_metadata_t *metadata, int vector_bytes, int level)
{
    const int NODE_HEAD_BYTES = sizeof(usearch_label_t) + 4 /*sizeof dim */ + 4 /*sizeof level*/;
    uint32    node_bytes = 0;
    node_bytes += NODE_HEAD_BYTES + metadata->neighbors_base_bytes;
    node_bytes += metadata->neighbors_bytes * level;
    node_bytes += vector_bytes;
    return node_bytes;
}

static char *extract_node(char               *data,
                          uint64              progress,
                          int                 dim,
                          usearch_metadata_t *metadata,
                          /*->>output*/ int  *node_size,
                          int                *level)
{
    char *tape = data + progress;

    int read_dim_bytes = -1;
    memcpy(&read_dim_bytes, tape + sizeof(usearch_label_t), 4);  //+sizeof(label)
    memcpy(level, tape + sizeof(usearch_label_t) + 4, 4);        //+sizeof(label)+sizeof(dim)
    const int VECTOR_BYTES = dim * sizeof(float);
    assert(VECTOR_BYTES == read_dim_bytes);
    *node_size = UsearchNodeBytes(metadata, VECTOR_BYTES, *level);
    return tape;
}

static BlockNumber NumberOfBlockMapsInGroup(unsigned groupno)
{
    assert(groupno < HNSW_MAX_BLOCKMAP_GROUPS);

    return 1u << groupno;
}

static bool BlockMapGroupIsFullyInitialized(HnswIndexHeaderPage *hdr, unsigned groupno)
{
    assert(groupno < HNSW_MAX_BLOCKMAP_GROUPS);

    return hdr->blockmap_groups[ groupno ].blockmaps_initialized == NumberOfBlockMapsInGroup(groupno);
}

static uint32 BlockMapGroupFirstNodeIndex(unsigned groupno)
{
    uint32 first_node_index = 0;

    if(groupno == 0) return 0;
    for(unsigned i = 0; i < groupno - 1; ++i) first_node_index += NumberOfBlockMapsInGroup(i);
    return first_node_index;
}

static bool AreEnoughBlockMapsForTupleId(uint32 blockmap_groups_nr, uint32 tuple_id)
{
    // new_tuple_id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1 is kth blockmap
    // we check if k is more than already created 2^groups
    return tuple_id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1 < ((uint32)1 << blockmap_groups_nr);
}

/*
 * Updates HnswBlockMapGroupDesc for groupno in the HnswIndexHeaderPage.
 * The header is fsynced to the WAL after this function returns if flush_log is true.
 * Assumes that the header (block 0) buffer is locked.
 *
 * In the current use cases the header page is added to a WAL record somewhere
 * up the call stack, so the changes made here must be duplicated to the
 * HnswIndexHeaderPage in that header page, otherwise they would be overwritten
 * when that WAL record up the stack is written to the log.
 */
static void UpdateHeaderBlockMapGroupDesc(
    Relation index, ForkNumber forkNum, unsigned groupno, const HnswBlockMapGroupDesc *desc, bool flush_log)
{
    GenericXLogState    *state;
    BlockNumber          HEADER_BLOCK = 0;
    Page                 hdr_page;
    Buffer               hdr_buf;
    HnswIndexHeaderPage *hdr_copy;
    XLogRecPtr           log_rec_ptr;

    state = GenericXLogStart(index);
    /* no need to look the buffer because it's the header (block 0) and it's locked already) */
    hdr_buf = ReadBufferExtended(index, forkNum, HEADER_BLOCK, RBM_NORMAL, NULL);
    hdr_page = GenericXLogRegisterBuffer(state, hdr_buf, LDB_GENERIC_XLOG_DELTA_IMAGE);

    hdr_copy = (HnswIndexHeaderPage *)PageGetContents(hdr_page);
    assert(groupno < lengthof(hdr_copy->blockmap_groups));

    hdr_copy->blockmap_groups_nr = groupno + 1;

    hdr_copy->blockmap_groups[ groupno ] = *desc;

    log_rec_ptr = GenericXLogFinish(state);
    assert(log_rec_ptr != InvalidXLogRecPtr);
    if(flush_log) XLogFlush(log_rec_ptr);
    ReleaseBuffer(hdr_buf);
}

/*
 * Continue and finish the initialization of a blockmap group.
 * If the initialization hasn't been started, then the initialization is started.
 * When this function is called a BUFFER_LOCK_EXCLUSIVE is supposed to be taken on the block 0 (header block).
 * When this function returns the block maps of the group are fully initialized.
 *
 *          HnswBlockMapGroupDesc
 * first_block          blockmaps_initialized       meaning
 * InvalidBlockNumber   0                           The blockmap group initialization hasn't started.
 * <block number>       0                           The blockmap group initialization has started, but
 *                                                  no blockmaps has been initialized. It's possible that
 *                                                  the block allocation for the group hasn't finished.
 * <block number>       >0                          Some blockmaps were initialized.
 * <block number>       NumberOfBlockMapsInGroup()  The blockmap group had been fully initialized.
 *
 * The process restarts during the blockmap creation are handled in the following way:
 * - there are only 2 cases to modify the index in the code right now: index
 *   creation and insert. If there are more cases the new code MUST do the
 *   BlockMapGroupIsFullyInitialized() check and then call
 *   ContinueBlockMapGroupInitialization if the blockmap group is not fully
 *   initialized (similar to how PrepareIndexTuple() currently does it);
 * - if the process restart happens during the index creation there is no code
 *   currently to continue an index creation after crash, so this function
 *   doesn't do anything special about it and just handles it as usual;
 * - if the process restart happens during the insert it might be possible that
 *   the blockmap group initialization hasn't been completed (and it could only
 *   happen to one group, which is the last one). We're considering the state
 *   of the blockmap group initialization after WAL replay after restart. The
 *   following cases are possible:
 *
 *   - the first header update WAL record (which sets HnswBlockMapGroupDesc.first_block
 *     for the group) hasn't been replayed. It means that
 *     ContinueBlockMapGroupInitialization() will start from the first header
 *     update and then continue as usual;
 *   - the first header update WAL record was replayed, but the block
 *     allocation for the index hasn't been finished. It will be detected by
 *     non-InvalidBlockNumber value in the first_block and a size check for the
 *     index relation. This case will be handled by finishing the block
 *     allocations and then continuing as usual;
 *   - blockmap pages initialization hasn't been complete. In this case the
 *     HnswBlockMapGroupDesc.blockmaps_initialized will be used as the first
 *     blockmap page to continue the initialization from. This value is updated
 *     in the header approximately every HNSW_BLOCKMAP_UPDATE_HEADER_EVERY
 *     initialized blockmap pages.
 *
 * - in case of failure during WAL replay we're relying on PostgreSQL to
 *   correctly recover the WAL on subsequent restart;
 * - in case of a failure in this function when it's continuing interrupted
 *   blockmap group initialization the subsequent run of this function after
 *   the restart will correctly continue from the nearest place it could
 *   continue from.
 *
 * Important note: This function creates a WAL record to update
 * HnswIndexHeaderPage.blockmap_groups and
 * HnswIndexHeaderPage.blockmap_groups_nr in the header page. Therefore it has
 * to update the same fields in the memory pointed to by hdr parameter, because
 * the header page is added to a WAL record somewhere on the higher level and
 * that WAL record would be GenericXLogFinish()ed after the header updates
 * here, so whatever is written to the header page here directly would be
 * overwritten.
 */
static void ContinueBlockMapGroupInitialization(
    HnswIndexHeaderPage *hdr, Relation index, ForkNumber forkNum, uint32 first_node_index, unsigned groupno)
{
    GenericXLogState            *state;
    BlockNumber                  blockmaps_in_group = NumberOfBlockMapsInGroup(groupno);
    const HnswBlockMapGroupDesc *group_desc;
    HnswBlockmapPage            *blockmap_page;
    unsigned                     pages_in_xlog_state;
    Buffer                       bufs[ MAX_GENERIC_XLOG_PAGES ];
    Buffer                       buf;
    Page                         page;
    HnswIndexPageSpecialBlock   *special;
    OffsetNumber                 inserted_at;

    assert(groupno < HNSW_MAX_BLOCKMAP_GROUPS);

    if(hdr->blockmap_groups[ groupno ].first_block == InvalidBlockNumber) {
        assert(hdr->blockmap_groups[ groupno ].blockmaps_initialized == 0);
        hdr->blockmap_groups[ groupno ].first_block = RelationGetNumberOfBlocksInFork(index, forkNum);
        assert(groupno == hdr->blockmap_groups_nr);
        hdr->blockmap_groups_nr = groupno + 1;
        UpdateHeaderBlockMapGroupDesc(index, forkNum, groupno, &hdr->blockmap_groups[ groupno ], true);
    }
    assert(hdr->blockmap_groups_nr == groupno + 1);

    group_desc = &hdr->blockmap_groups[ groupno ];
    if(group_desc->blockmaps_initialized == 0
       && group_desc->first_block + blockmaps_in_group > RelationGetNumberOfBlocksInFork(index, forkNum)) {
        buf = ExtendBufferedRelTo(BMR_REL(index),
                                  forkNum,
                                  NULL,
                                  EB_CLEAR_SIZE_CACHE,
                                  group_desc->first_block + blockmaps_in_group,
                                  RBM_NORMAL);
        assert(group_desc->first_block + blockmaps_in_group - 1 == BufferGetBlockNumber(buf));
        ReleaseBuffer(buf);
    }
    assert(group_desc->first_block + blockmaps_in_group <= RelationGetNumberOfBlocksInFork(index, forkNum));

    blockmap_page = palloc0(sizeof(*blockmap_page));
    pages_in_xlog_state = 0;
    for(uint32 blockmap_id = group_desc->blockmaps_initialized; blockmap_id < blockmaps_in_group; ++blockmap_id) {
        if(pages_in_xlog_state == 0) state = GenericXLogStart(index);

        buf = ReadBufferExtended(index, forkNum, group_desc->first_block + blockmap_id, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        PageInit(page, BufferGetPageSize(buf), sizeof(HnswIndexPageSpecialBlock));

        special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
        special->firstId = first_node_index + blockmap_id * HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        special->lastId = special->firstId + HNSW_BLOCKMAP_BLOCKS_PER_PAGE - 1;
        /* TODO nextblockno is incorrect for the last blockmap in the group */
        special->nextblockno = group_desc->first_block + blockmap_id + 1;

        blockmap_page->first_id = first_node_index + blockmap_id * HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        // we always add a single Blockmap page per Index page which has a fixed size that
        // always fits in postgres wal page. So this should never happen
        // (Assumes 8k BLKSZ. we can make HnswBlockmapPage size configurable by BLCKSZ)
        inserted_at = PageAddItem(page, (Item)blockmap_page, sizeof(*blockmap_page), InvalidOffsetNumber, false, false);
        ldb_invariant(inserted_at != InvalidOffsetNumber, "could not add blockmap to page %d", inserted_at);

        bufs[ pages_in_xlog_state++ ] = buf;

        if(pages_in_xlog_state == MAX_GENERIC_XLOG_PAGES || blockmap_id == blockmaps_in_group - 1) {
            bool update_header = false;

            // GenericXLogFinish also calls MarkBufferDirty(buf)
            GenericXLogFinish(state);
            for(unsigned i = 0; i < pages_in_xlog_state; ++i) {
                UnlockReleaseBuffer(bufs[ i ]);
                if(((blockmap_id - pages_in_xlog_state + 1 + i) % HNSW_BLOCKMAP_UPDATE_HEADER_EVERY) == 0)
                    update_header = true;
            }
            if(update_header || blockmap_id == blockmaps_in_group - 1) {
                hdr->blockmap_groups[ groupno ].blockmaps_initialized = blockmap_id + 1;
                UpdateHeaderBlockMapGroupDesc(
                    index, forkNum, groupno, &hdr->blockmap_groups[ groupno ], blockmap_id == blockmaps_in_group - 1);
            }
            pages_in_xlog_state = 0;
        }
    }
    pfree(blockmap_page);
    // it is possible that usearch asks for a newly added node from this blockmap range
    // we need to make sure the global header has this information
}

void StoreExternalIndexBlockMapGroup(Relation             index,
                                     usearch_index_t      external_index,
                                     HnswIndexHeaderPage *headerp,
                                     ForkNumber           forkNum,
                                     char                *data,
                                     uint64              *progress,
                                     int                  dimension,
                                     uint32               first_node_index,
                                     uint32               num_added_vectors,
                                     unsigned             blockmap_groupno)
{
    const uint32 number_of_blockmaps_in_group = NumberOfBlockMapsInGroup(blockmap_groupno);

    ContinueBlockMapGroupInitialization(headerp, index, forkNum, first_node_index, blockmap_groupno);

    // Now add nodes to data pages
    char        *node = 0;
    int          node_size = 0;
    int          node_level = 0;
    uint32       predicted_next_block = InvalidBlockNumber;
    uint32       last_block = -1;
    BlockNumber *l_wal_retriever_block_numbers
        = palloc0(sizeof(BlockNumber) * number_of_blockmaps_in_group * HNSW_BLOCKMAP_BLOCKS_PER_PAGE);

    HnswIndexTuple    *bufferpage = palloc(BLCKSZ);
    usearch_metadata_t metadata = usearch_metadata(external_index, NULL);

    /* Add all the vectors to the WAL */
    for(uint32 node_id = first_node_index; node_id < first_node_index + num_added_vectors;) {
        // 1. create HnswIndexTuple

        // 2. fill header and special

        // 3. while available space is larger than node_size
        //     3a. add node to page

        // 4. commit buffer
        Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        GenericXLogState *state = GenericXLogStart(index);
        Page              page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        PageInit(page, BufferGetPageSize(buf), sizeof(HnswIndexPageSpecialBlock));

        if(predicted_next_block != InvalidBlockNumber) {
            ldb_invariant(predicted_next_block == BufferGetBlockNumber(buf),
                          "my block number hypothesis failed. "
                          "predicted block number %d does not match "
                          "actual %d",
                          predicted_next_block,
                          BufferGetBlockNumber(buf));
        }

        HnswIndexPageSpecialBlock *special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
        special->firstId = node_id;
        special->nextblockno = InvalidBlockNumber;

        // note: even if the condition is true, nodepage may be too large
        // as the condition does not take into account the flexible array component
        while(PageGetFreeSpace(page) > sizeof(HnswIndexTuple) + dimension * sizeof(float)) {
            if(node_id >= first_node_index + num_added_vectors) break;
            memset(bufferpage, 0, BLCKSZ);
            /************* extract node from usearch index************/

            node = extract_node(data,
                                *progress,
                                dimension,
                                &metadata,
                                /*->>output*/ &node_size,
                                &node_level);
            bufferpage->id = node_id;
            bufferpage->level = node_level;
            bufferpage->size = node_size;
            ldb_invariant(node_level < 100,
                          "node level is too large. at id %d"
                          "this is likely a bug in usearch. "
                          "level: %d",
                          node_id,
                          node_level);

            // node should not be larger than the 8k bufferpage
            // invariant holds because of dimension <2000 check in index creation
            // once quantization is enabled, we can allow larger overall dims
            assert(bufferpage + offsetof(HnswIndexTuple, node) + node_size < bufferpage + BLCKSZ);
            memcpy(bufferpage->node, node, node_size);

            if(PageAddItem(
                   page, (Item)bufferpage, sizeof(HnswIndexTuple) + node_size, InvalidOffsetNumber, false, false)
               == InvalidOffsetNumber) {
                // break to get a fresh page
                // todo:: properly test this case
                break;
            }

            // we successfully recorded the node. move to the next one
            l_wal_retriever_block_numbers[ node_id - first_node_index ] = BufferGetBlockNumber(buf);
            *progress += node_size;
            node_id++;
        }

        if(node_id < num_added_vectors + first_node_index) {
            predicted_next_block = BufferGetBlockNumber(buf) + 1;
        } else {
            last_block = BufferGetBlockNumber(buf);
            predicted_next_block = InvalidBlockNumber;
        }
        special->lastId = node_id - 1;
        special->nextblockno = predicted_next_block;

        MarkBufferDirty(buf);
        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
    }
    headerp->last_data_block = last_block;

    // Update blockmap pages with correct associations
    for(uint32 blockmap_id = 0; blockmap_id < number_of_blockmaps_in_group; ++blockmap_id) {
        // When the blockmap page group was created, header block was updated accordingly in
        // ContinueBlockMapGroupInitialization call above.
        const BlockNumber blockmapno = blockmap_id + headerp->blockmap_groups[ blockmap_groupno ].first_block;
        Buffer            buf = ReadBufferExtended(index, MAIN_FORKNUM, blockmapno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        GenericXLogState *state = GenericXLogStart(index);
        Page              page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        HnswBlockmapPage *blockmap = (HnswBlockmapPage *)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
        memcpy(blockmap->blocknos,
               l_wal_retriever_block_numbers + blockmap_id * HNSW_BLOCKMAP_BLOCKS_PER_PAGE,
               sizeof(BlockNumber) * HNSW_BLOCKMAP_BLOCKS_PER_PAGE);
        MarkBufferDirty(buf);
        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
    }
}

void StoreExternalIndex(Relation                index,
                        usearch_index_t         external_index,
                        ForkNumber              forkNum,
                        char                   *data,
                        usearch_init_options_t *opts,
                        size_t                  num_added_vectors)
{
    Buffer header_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    // even when we are creating a new page, it must always be the first page we create
    // and should therefore have BLockNumber 0
    assert(BufferGetBlockNumber(header_buf) == 0);
    LockBuffer(header_buf, BUFFER_LOCK_EXCLUSIVE);

    GenericXLogState *state = GenericXLogStart(index);
    Page              header_page = GenericXLogRegisterBuffer(state, header_buf, GENERIC_XLOG_FULL_IMAGE);

    PageInit(header_page, BufferGetPageSize(header_buf), 0);
    HnswIndexHeaderPage *headerp = (HnswIndexHeaderPage *)PageGetContents(header_page);

    headerp->magicNumber = LDB_WAL_MAGIC_NUMBER;
    headerp->version = LDB_WAL_VERSION_NUMBER;
    headerp->vector_dim = opts->dimensions;
    headerp->m = opts->connectivity;
    headerp->ef_construction = opts->expansion_add;
    headerp->ef = opts->expansion_search;
    headerp->metric_kind = opts->metric_kind;

    headerp->num_vectors = num_added_vectors;
    headerp->blockmap_groups_nr = 0;

    for(uint32 i = 0; i < lengthof(headerp->blockmap_groups); ++i) {
        headerp->blockmap_groups[ i ] = (HnswBlockMapGroupDesc){
            .first_block = InvalidBlockNumber,
            .blockmaps_initialized = 0,
        };
    }
    // headerp->blockmap_groups and blockmap_groups_nr are
    // updated in a separate wal entry
    headerp->last_data_block = InvalidBlockNumber;

    memcpy(headerp->usearch_header, data, USEARCH_HEADER_SIZE);
    ((PageHeader)header_page)->pd_lower = ((char *)headerp + sizeof(HnswIndexHeaderPage)) - (char *)header_page;

    uint64   progress = USEARCH_HEADER_SIZE;  // usearch header size
    unsigned blockmap_groupno = 0;
    uint32   group_node_first_index = 0;
    uint32   num_added_vectors_remaining = num_added_vectors;
    uint32   batch_size = HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
    while(num_added_vectors_remaining > 0) {
        StoreExternalIndexBlockMapGroup(index,
                                        external_index,
                                        headerp,
                                        forkNum,
                                        data,
                                        &progress,
                                        opts->dimensions,
                                        group_node_first_index,
                                        Min(num_added_vectors_remaining, batch_size),
                                        blockmap_groupno);
        num_added_vectors_remaining -= Min(num_added_vectors_remaining, batch_size);
        group_node_first_index += batch_size;
        batch_size = batch_size * 2;
        blockmap_groupno++;
    }

    MarkBufferDirty(header_buf);
    GenericXLogFinish(state);
    UnlockReleaseBuffer(header_buf);
}

// adds a an item to hnsw index relation page. assumes the page has enough space for the item
// the function also takes care of setting the special block
static OffsetNumber HnswIndexPageAddVector(Page page, HnswIndexTuple *new_vector_data, int new_vector_size)
{
    HnswIndexPageSpecialBlock *special_block;
    OffsetNumber               inserted_at;

    inserted_at = PageAddItem(
        page, (Item)new_vector_data, sizeof(HnswIndexTuple) + new_vector_size, InvalidOffsetNumber, false, false);
    ldb_invariant(inserted_at != InvalidOffsetNumber, "unexpectedly could not add item to the last existing page");
    special_block = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);

    if(PageGetMaxOffsetNumber(page) == 1) {
        // we added the first element to the index page!
        // update firstId
        ldb_dlog("InsertBranching: we added first element to index page");
        special_block->firstId = new_vector_data->id;
        special_block->lastId = new_vector_data->id;
        special_block->nextblockno = InvalidBlockNumber;
    } else {
        ldb_dlog("InsertBranching: we added (NOT FIRST) element to index page");
        assert(special_block->lastId == new_vector_data->id - 1);
        special_block->lastId += 1;
        // we always add to the last page so nextblockno
        // of the page we add to is always InvalidBlockNumber
        assert(special_block->nextblockno == InvalidBlockNumber);
    }
    return inserted_at;
}

// the function assumes that its modifications to hdr will
// saved durably on the index relation by the caller
// the function does all the necessary preperation of an index page inside postgres
// hnsw index for the external indexer to start using the tuple (or node-entry) via
// appropriate mutable external retriever
HnswIndexTuple *PrepareIndexTuple(Relation             index_rel,
                                  GenericXLogState    *state,
                                  HnswIndexHeaderPage *hdr,
                                  usearch_metadata_t  *metadata,
                                  uint32               new_tuple_id,
                                  uint32               new_tuple_level,
                                  HnswInsertState     *insertstate)
{
    if(hdr->blockmap_groups_nr > 0) {
        unsigned last_blockmap_group = hdr->blockmap_groups_nr - 1;

        if(!BlockMapGroupIsFullyInitialized(hdr, last_blockmap_group)) {
            ContinueBlockMapGroupInitialization(
                hdr, index_rel, MAIN_FORKNUM, BlockMapGroupFirstNodeIndex(last_blockmap_group), last_blockmap_group);
        }
    }
    // if any data blocks exist, the last one's buffer will be read into this
    Buffer last_dblock = InvalidBuffer;
    // if a new data buffer is created for the inserted vector, it will be stored here
    Buffer new_dblock = InvalidBuffer;
    // blockmap block that points to the blockno of newly inserted node
    Buffer blockmap_block = InvalidBuffer;

    Page                       page;
    OffsetNumber               new_tup_at;
    HnswIndexPageSpecialBlock *special_block;
    BlockNumber                new_vector_blockno;
    uint32          new_tuple_size = UsearchNodeBytes(metadata, hdr->vector_dim * sizeof(float), new_tuple_level);
    HnswIndexTuple *alloced_tuple = NULL;
    HnswIndexTuple *new_tup_ref = NULL;
    // create the new node
    // allocate buffer to construct the new node
    // note that we allocate more than sizeof(HnswIndexTuple) since the struct has a flexible array member
    // which depends on parameters passed into UsearchNodeBytes above
    alloced_tuple = (HnswIndexTuple *)palloc0(sizeof(HnswIndexTuple) + new_tuple_size);
    alloced_tuple->id = new_tuple_id;
    alloced_tuple->level = new_tuple_level;
    alloced_tuple->size = new_tuple_size;

    /*** Add a new tuple corresponding to the added vector to the list of tuples in the index
     *  (create new page if necessary) ***/

    if(hdr->last_data_block == InvalidBlockNumber) {
        ContinueBlockMapGroupInitialization(hdr, index_rel, MAIN_FORKNUM, 0, 0);
        new_dblock = ReadBufferExtended(index_rel, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(new_dblock, BUFFER_LOCK_EXCLUSIVE);
        new_vector_blockno = BufferGetBlockNumber(new_dblock);

        // todo:: add a failure point in here for tests and make sure new_dblock is not leaked
        hdr->last_data_block = new_vector_blockno;

        // 4.
        page = GenericXLogRegisterBuffer(state, new_dblock, LDB_GENERIC_XLOG_DELTA_IMAGE);
        PageInit(page, BufferGetPageSize(new_dblock), sizeof(HnswIndexPageSpecialBlock));
        extra_dirtied_add(insertstate->retriever_ctx->extra_dirted, new_vector_blockno, new_dblock, page);

        new_tup_at = HnswIndexPageAddVector(page, alloced_tuple, alloced_tuple->size);

        MarkBufferDirty(new_dblock);
    } else {
        page = extra_dirtied_get(insertstate->retriever_ctx->extra_dirted, hdr->last_data_block, &last_dblock);

        if(page == NULL) {
            last_dblock = ReadBufferExtended(index_rel, MAIN_FORKNUM, hdr->last_data_block, RBM_NORMAL, NULL);
            LockBuffer(last_dblock, BUFFER_LOCK_EXCLUSIVE);
            page = GenericXLogRegisterBuffer(state, last_dblock, LDB_GENERIC_XLOG_DELTA_IMAGE);
            extra_dirtied_add(insertstate->retriever_ctx->extra_dirted, hdr->last_data_block, last_dblock, page);
        }

        assert(last_dblock != InvalidBuffer);

        if(PageGetFreeSpace(page) > sizeof(HnswIndexTuple) + alloced_tuple->size
           && AreEnoughBlockMapsForTupleId(hdr->blockmap_groups_nr, new_tuple_id)) {
            // there is enough space in the last page to fit the new vector
            // so we just append it to the page
            ldb_dlog("InsertBranching: we adding element to existing page");
            new_tup_at = HnswIndexPageAddVector(page, alloced_tuple, alloced_tuple->size);
            new_vector_blockno = BufferGetBlockNumber(last_dblock);
            assert(new_vector_blockno == hdr->last_data_block);

            MarkBufferDirty(last_dblock);
        } else {
            ldb_dlog("InsertBranching: creating new data bage to add an element to");
            // 1. create and read a new block
            // 2. store the new block blockno in the last block special
            // 2.5 update index header to point to the new last page
            // 3. mark dirty the old block (PIN must be held until after the xlog transaction is committed)
            // 4. add the new vector to the newly created page

            // 1.
            // todo:: I think if for some reason insertion fails after this point, the new block will stick
            // around. So, perhaps we should do what we did with blockmap and have a separate wal record for new
            // page creation.

            // check the count of blockmaps, see if there's place to add the block id, if yes add, if no create a
            // new group check if already existing blockmaps are not enough
            if(!AreEnoughBlockMapsForTupleId(hdr->blockmap_groups_nr, new_tuple_id)) {
                ContinueBlockMapGroupInitialization(
                    hdr, index_rel, MAIN_FORKNUM, new_tuple_id, hdr->blockmap_groups_nr);
            }

            new_dblock = ReadBufferExtended(index_rel, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
            LockBuffer(new_dblock, BUFFER_LOCK_EXCLUSIVE);
            new_vector_blockno = BufferGetBlockNumber(new_dblock);
            // todo:: add a failure point in here for tests and make sure new_dblock is not leaked

            // 2.
            special_block = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
            special_block->nextblockno = new_vector_blockno;

            // 2.5
            hdr->last_data_block = new_vector_blockno;

            // 3.
            MarkBufferDirty(last_dblock);
            page = NULL;

            // 4.
            page = GenericXLogRegisterBuffer(state, new_dblock, LDB_GENERIC_XLOG_DELTA_IMAGE);
            PageInit(page, BufferGetPageSize(new_dblock), sizeof(HnswIndexPageSpecialBlock));
            extra_dirtied_add(insertstate->retriever_ctx->extra_dirted, new_vector_blockno, new_dblock, page);

            new_tup_at = HnswIndexPageAddVector(page, alloced_tuple, alloced_tuple->size);

            MarkBufferDirty(new_dblock);
        }
    }

    /*** extract the inserted tuple ref to return so usearch can do further work on it ***/
    new_tup_ref = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, new_tup_at));
    assert(new_tup_ref->id == new_tuple_id);
    assert(new_tup_ref->level == new_tuple_level);
    assert(new_tup_ref->size == new_tuple_size);
    page = NULL;  // to avoid its accidental use
    /*** Update pagemap with the information of the added page ***/
    {
        BlockNumber       blockmapno = getBlockMapPageBlockNumber(&hdr->blockmap_groups[ 0 ], new_tuple_id);
        Page              blockmap_page;
        HnswBlockmapPage *blockmap;
        int               max_offset;

        // todo:: figure out how/from where /usr/include/strings.h is included at this point
        // (noticed that index is a function defined there)

        blockmap_block = ReadBufferExtended(index_rel, MAIN_FORKNUM, blockmapno, RBM_NORMAL, NULL);
        LockBuffer(blockmap_block, BUFFER_LOCK_EXCLUSIVE);
        blockmap_page = GenericXLogRegisterBuffer(state, blockmap_block, LDB_GENERIC_XLOG_DELTA_IMAGE);
        extra_dirtied_add(insertstate->retriever_ctx->extra_dirted, blockmapno, blockmap_block, blockmap_page);

        /* sanity-check blockmap block offset number */
        max_offset = PageGetMaxOffsetNumber(blockmap_page);
        ldb_invariant(max_offset == FirstOffsetNumber,
                      "ERROR: Blockmap max_offset is %d but was supposed to be %d",
                      max_offset,
                      FirstOffsetNumber);

        // todo:: elsewhere this blockmap var is called blockmap_page
        // be consistent with naming here and elsewhere
        blockmap = (HnswBlockmapPage *)PageGetItem(blockmap_page, PageGetItemId(blockmap_page, FirstOffsetNumber));
        // set the pointer to the newly added node block in the blockmap
        blockmap->blocknos[ new_tuple_id % HNSW_BLOCKMAP_BLOCKS_PER_PAGE ] = new_vector_blockno;
    }
    /*** Update header ***/
    hdr->num_vectors++;

    pfree(alloced_tuple);
    return new_tup_ref;
}

static BlockNumber getBlockMapPageBlockNumber(const HnswBlockMapGroupDesc *blockmap_groups, int id)
{
    assert(id >= 0);
    // Trust me, I'm an engineer!
    id = id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1;
    int k;
    for(k = 0; id >= (1 << k); ++k) {
    }
    assert(blockmap_groups[ k - 1 ].first_block != InvalidBlockNumber);
    return blockmap_groups[ k - 1 ].first_block + (id - (1 << (k - 1)));
}

BlockNumber getDataBlockNumber(RetrieverCtx *ctx, int id, bool add_to_extra_dirtied)
{
    HTABCache             *cache = &ctx->block_numbers_cache;
    HnswBlockMapGroupDesc *blockmap_groups
        = ctx->header_page_under_wal != NULL ? ctx->header_page_under_wal->blockmap_groups : ctx->blockmap_groups_cache;
    BlockNumber       blockmapno = getBlockMapPageBlockNumber(blockmap_groups, id);
    BlockNumber       blockno;
    HnswBlockmapPage *blockmap_page;
    Page              page;
    Buffer            buf;
    OffsetNumber      offset;
    bool              idx_pagemap_prelocked = false;

#if LANTERNDB_USEARCH_LEVEL_DISTRIBUTION
    static levels[ 20 ] = {0};
    static cnt = 0;

    // clang-format off
    if (cnt % 100 == 0) {
        elog(INFO, "levels0 %d %d %d %d %d %d %d %d %d %d", levels[0], levels[1], levels[2], levels[3], levels[4],
        levels[5], levels[6], levels[7], levels[8], levels[9]);
        elog(INFO, "levels1 %d %d %d %d %d %d %d %d %d %d",
        levels[10], levels[11], levels[12], levels[13], levels[14], levels[15], levels[16], levels[17], levels[18],
        levels[19]);
    }
    // clang-format on
#endif

    void *blockno_from_cache_p = cache_get_item(cache, &id);
    if(blockno_from_cache_p != NULL) {
        return *((BlockNumber *)blockno_from_cache_p);
    }

    // it is necessary to first check the extra dirtied pages for the blockmap page, in case we are in the
    // middle of an insert and the insert operation has the block we need under a lock
    page = extra_dirtied_get(ctx->extra_dirted, blockmapno, NULL);
    if(page == NULL) {
        buf = ReadBufferExtended(ctx->index_rel, MAIN_FORKNUM, blockmapno, RBM_NORMAL, NULL);
        const int mode = add_to_extra_dirtied ? BUFFER_LOCK_EXCLUSIVE : BUFFER_LOCK_SHARE;
        LockBuffer(buf, mode);
        page = BufferGetPage(buf);
        if(add_to_extra_dirtied) {
            extra_dirtied_add(ctx->extra_dirted, blockmapno, buf, page);
        }
    } else {
        idx_pagemap_prelocked = true;
    }

    // Blockmap page is stored as a single large blob per page
    assert(PageGetMaxOffsetNumber(page) == FirstOffsetNumber);

    blockmap_page = (HnswBlockmapPage *)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));

    offset = id % HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
    blockno = blockmap_page->blocknos[ offset ];
    cache_set_item(cache, &id, &blockmap_page->blocknos[ offset ]);
    if(!idx_pagemap_prelocked) {
        UnlockReleaseBuffer(buf);
    }

    return blockno;
}

void *ldb_wal_index_node_retriever(void *ctxp, int id)
{
    RetrieverCtx   *ctx = (RetrieverCtx *)ctxp;
    BlockNumber     data_block_no;
    HnswIndexTuple *nodepage;
    Page            page;
    OffsetNumber    offset, max_offset;
    Buffer          buf = InvalidBuffer;
    bool            idx_page_prelocked = false;
    void           *cached_node = fa_cache_get(&ctx->fa_cache, id);
    if(cached_node != NULL) {
        return cached_node;
    }

    data_block_no = getDataBlockNumber(ctx, id, false);

    page = extra_dirtied_get(ctx->extra_dirted, data_block_no, NULL);
    if(page == NULL) {
        buf = ReadBufferExtended(ctx->index_rel, MAIN_FORKNUM, data_block_no, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
    } else {
        idx_page_prelocked = true;
    }

    max_offset = PageGetMaxOffsetNumber(page);
    for(offset = FirstOffsetNumber; offset <= max_offset; offset = OffsetNumberNext(offset)) {
        nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
        if(nodepage->id == (uint32)id) {
#if LANTERNDB_USEARCH_LEVEL_DISTRIBUTION
            levels[ nodepage->level ]++;
#endif
#if LANTERNDB_COPYNODES
            BufferNode *buffNode;
            buffNode = (BufferNode *)palloc(sizeof(BufferNode));
            buffNode->buf = (char *)palloc(nodepage->size);
            memcpy(buffNode->buf, nodepage->node, nodepage->size);
            if(!idx_page_prelocked) {
                UnlockReleaseBuffer(buf);
            }
            dlist_push_tail(&ctx->takenbuffers, &buffNode->node);
            return buffNode->buf;
#else
            if(!idx_page_prelocked) {
                // Wrap buf in a linked list node
                BufferNode *buffNode;
                buffNode = (BufferNode *)palloc(sizeof(BufferNode));
                buffNode->buf = buf;

                // Add buffNode to list of pinned buffers
                dlist_push_tail(&ctx->takenbuffers, &buffNode->node);
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }

#if PG_VERSION_NUM >= 130000
            CheckMem(work_mem,
                     NULL,
                     NULL,
                     0,
                     "pinned more tuples during node retrieval than will fit in work_mem, cosider increasing work_mem");
#endif
            fa_cache_insert(&ctx->fa_cache, id, nodepage->node);

            return nodepage->node;
#endif
        }
    }
    if(!idx_page_prelocked) {
        assert(BufferIsValid(buf));
        UnlockReleaseBuffer(buf);
    }
    ldb_invariant(false, "node with id %d not found", id);
    pg_unreachable();
}

void *ldb_wal_index_node_retriever_mut(void *ctxp, int id)
{
    RetrieverCtx   *ctx = (RetrieverCtx *)ctxp;
    BlockNumber     data_block_no = getDataBlockNumber(ctx, id, true);
    HnswIndexTuple *nodepage;
    Page            page;
    OffsetNumber    offset, max_offset;
    Buffer          buf = InvalidBuffer;
    bool            idx_page_prelocked = false;

    // here, we don't bother looking up the fully associative cache because
    // given the current usage of _mut, it is never going to be in the chache

    page = extra_dirtied_get(ctx->extra_dirted, data_block_no, NULL);
    if(page == NULL) {
        buf = ReadBufferExtended(ctx->index_rel, MAIN_FORKNUM, data_block_no, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        // todo:: has to be under WAL!!
        page = BufferGetPage(buf);
        extra_dirtied_add(ctx->extra_dirted, data_block_no, buf, page);
    } else {
        idx_page_prelocked = true;
    }

    max_offset = PageGetMaxOffsetNumber(page);
    for(offset = FirstOffsetNumber; offset <= max_offset; offset = OffsetNumberNext(offset)) {
        nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
        if(nodepage->id == (uint32)id) {
            fa_cache_insert(&ctx->fa_cache, id, nodepage->node);

            return nodepage->node;
        }
    }

    if(!idx_page_prelocked) {
        assert(BufferIsValid(buf));
        UnlockReleaseBuffer(buf);
    }
    ldb_invariant(false, "node with id %d not found", id);
    pg_unreachable();
}
