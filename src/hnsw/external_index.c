
#include <postgres.h>

#include "external_index.h"

#include <access/generic_xlog.h>  // GenericXLog
#include <access/heapam.h>        // relation_open
#include <assert.h>
#include <common/relpath.h>
#include <math.h>
#include <miscadmin.h>  // START_CRIT_SECTION, END_CRIT_SECTION
#include <pg_config.h>  // BLCKSZ
#include <storage/block.h>
#include <storage/bufmgr.h>  // Buffer
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "extra_dirtied.h"
#include "failure_point.h"
#include "hnsw.h"
#include "hnsw/fa_cache.h"
#include "htab_cache.h"
#include "insert.h"
#include "options.h"
#include "retriever.h"
#include "usearch.h"
#include "usearch_storage.hpp"
#include "utils.h"

#if PG_VERSION_NUM >= 130000
#include <miscadmin.h>
#endif

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

void StoreExternalIndexBlockMapGroup(Relation             index,
                                     const metadata_t    *metadata,
                                     HnswIndexHeaderPage *headerp,
                                     ForkNumber           forkNum,
                                     char                *data,
                                     uint64              *progress,
                                     int                  dimension,
                                     uint32               first_node_index,
                                     uint32               num_added_vectors,
                                     ItemPointerData     *item_pointers)
{
    // Now add nodes to data pages
    char  *node = 0;
    int    node_size = 0;
    int    node_level = 0;
    uint32 predicted_next_block = InvalidBlockNumber;
    uint32 last_block = -1;

    HnswIndexTuple *bufferpage = palloc(BLCKSZ);

    /* Add all the vectors to the WAL */
    for(uint32 node_id = first_node_index; node_id < first_node_index + num_added_vectors;) {
        // 1. create HnswIndexTuple

        // 2. fill header and special

        // 3. while available space is larger than node_size
        //     3a. add node to page

        // 4. commit buffer
        Buffer       buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
        BlockNumber  blockno = BufferGetBlockNumber(buf);
        OffsetNumber offsetno;
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
                                metadata,
                                /*->>output*/ &node_size,
                                &node_level);
            bufferpage->seqid = node_id;
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
            offsetno = PageAddItem(
                page, (Item)bufferpage, sizeof(HnswIndexTuple) + node_size, InvalidOffsetNumber, false, false);

            if(offsetno == InvalidOffsetNumber) {
                // break to get a fresh page
                break;
            }

            // we successfully recorded the node. move to the next one
            BlockIdSet(&item_pointers[ node_id ].ip_blkid, blockno);
            item_pointers[ node_id ].ip_posid = offsetno;
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

    LDB_FAILURE_POINT_CRASH_IF_ENABLED("just_before_updating_blockmaps_after_inserting_nodes");
}

void StoreExternalEmptyIndex(Relation index, ForkNumber forkNum, char *data, usearch_init_options_t *opts)
{
    // this method is intended to store empty indexes for unlogged tables (ambuildempty method) and should hence be
    // called with forkNum = INIT_FORKNUM

    Buffer header_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    // even when we are creating a new page, it must always be the first page we create
    // and should therefore have BlockNumber 0
    assert(BufferGetBlockNumber(header_buf) == 0);

    LockBuffer(header_buf, BUFFER_LOCK_EXCLUSIVE);

    START_CRIT_SECTION();

    Page header_page = BufferGetPage(header_buf);

    PageInit(header_page, BufferGetPageSize(header_buf), 0);

    HnswIndexHeaderPage *headerp = (HnswIndexHeaderPage *)PageGetContents(header_page);

    headerp->magicNumber = LDB_WAL_MAGIC_NUMBER;
    headerp->version = LDB_WAL_VERSION_NUMBER;
    headerp->vector_dim = opts->dimensions;
    headerp->m = opts->connectivity;
    headerp->ef_construction = opts->expansion_add;
    headerp->ef = opts->expansion_search;
    headerp->metric_kind = opts->metric_kind;
    headerp->pq = opts->pq;
    headerp->num_centroids = opts->num_centroids;
    headerp->num_subvectors = opts->num_subvectors;

    headerp->num_vectors = 0;
    headerp->blockmap_groups_nr_unused = 0;

    headerp->last_data_block = InvalidBlockNumber;

    memcpy(headerp->usearch_header, data, USEARCH_HEADER_SIZE);
    ((PageHeader)header_page)->pd_lower = ((char *)headerp + sizeof(HnswIndexHeaderPage)) - (char *)header_page;

    MarkBufferDirty(header_buf);

    // Write a WAL record containing a full image of the page. Even though this is an unlogged table that doesn't use
    // WAL, this line appears to flush changes to disc immediately (and not waiting after the first checkpoint). This is
    // important because this empty index will live in the init fork, where it will be used to reset the unlogged index
    // after a crash, and so we need this written to disc in order to have proper crash recovery functionality available
    // immediately. Otherwise, if a crash occurs before the first postgres checkpoint, postgres can't read the init fork
    // from disc and we will have a corrupted index when postgres attempts recovery. This is also what nbtree access
    // method's implementation does for empty unlogged indexes (ambuildempty implementation).
    // NOTE: we MUST have this be inside a crit section, or else an assertion inside this method will fail and crash the
    // db
    log_newpage_buffer(header_buf, false);

    END_CRIT_SECTION();

    UnlockReleaseBuffer(header_buf);
}

void StoreExternalIndex(Relation                index,
                        const metadata_t       *external_index_metadata,
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
    headerp->pq = opts->pq;
    headerp->num_centroids = opts->num_centroids;
    headerp->num_subvectors = opts->num_subvectors;

    headerp->last_data_block = InvalidBlockNumber;

    memcpy(headerp->usearch_header, data, USEARCH_HEADER_SIZE);
    ((PageHeader)header_page)->pd_lower = ((char *)headerp + sizeof(HnswIndexHeaderPage)) - (char *)header_page;

    // Flush header page to WAL, because StoreExternalIndexBlockMapGroup references header page
    // In wal logs when adding block map groups, and WAL redo crashes in replica as header page record
    // Would not exist yet

    MarkBufferDirty(header_buf);
    GenericXLogFinish(state);

    state = GenericXLogStart(index);
    header_page = GenericXLogRegisterBuffer(state, header_buf, GENERIC_XLOG_FULL_IMAGE);
    headerp = (HnswIndexHeaderPage *)PageGetContents(header_page);

    // allocate some pages for pq codebook
    // for now, always add this blocks to make sure all tests run with these and nothing fails
    if(opts->pq) {
        const int num_clusters = 256;
        // total bytes for num_clusters clusters = num_clusters * vector_size_in_bytes
        // total pages for codebook = bytes / page_size
        for(int i = 0; i < ceil((float)(num_clusters)*opts->dimensions * sizeof(float) / BLCKSZ); i++) {
            Buffer cluster_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
            LockBuffer(cluster_buf, BUFFER_LOCK_EXCLUSIVE);
            GenericXLogState *cluster_state = GenericXLogStart(index);

            GenericXLogRegisterBuffer(cluster_state, cluster_buf, GENERIC_XLOG_FULL_IMAGE);
            // todo:: actually, write the codebook here
            GenericXLogFinish(cluster_state);
            UnlockReleaseBuffer(cluster_buf);
        }
    }

    uint64 progress = USEARCH_HEADER_SIZE;
    uint32 group_node_first_index = 0;
    uint32 num_added_vectors_remaining = num_added_vectors;
    uint32 batch_size = HNSW_BLOCKMAP_BLOCKS_PER_PAGE;

    ItemPointerData *item_pointers = palloc(num_added_vectors * sizeof(ItemPointerData));

    while(num_added_vectors_remaining > 0) {
        StoreExternalIndexBlockMapGroup(index,
                                        external_index_metadata,
                                        headerp,
                                        forkNum,
                                        data,
                                        &progress,
                                        opts->dimensions,
                                        group_node_first_index,
                                        Min(num_added_vectors_remaining, batch_size),
                                        item_pointers);
        num_added_vectors_remaining -= Min(num_added_vectors_remaining, batch_size);
        group_node_first_index += batch_size;
        batch_size = batch_size * 2;
    }

    // this is where I rewrite all neighbors to use BlockNumber
    Buffer              buf;
    HnswIndexHeaderPage header_copy;
    Page                page;
    OffsetNumber        offset, maxoffset;
    GenericXLogState   *gxlogState;
    header_copy = *(HnswIndexHeaderPage *)PageGetContents(header_page);
    // rewrite neighbor lists in terms of block numbers
    for(BlockNumber blockno = 1;
        BlockNumberIsValid(header_copy.last_data_block) && blockno <= header_copy.last_data_block;
        blockno++) {
        bool block_modified = false;
        buf = ReadBufferExtended(index, MAIN_FORKNUM, blockno, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        gxlogState = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(gxlogState, buf, LDB_GENERIC_XLOG_DELTA_IMAGE);
        maxoffset = PageGetMaxOffsetNumber(page);

        if(false /*pq header page*/) {
        } else {
            block_modified = true;
            // todo:: this could also be a pq page(see external_index.c, opts->pq handling)
            for(offset = FirstOffsetNumber; offset <= maxoffset; offset = OffsetNumberNext(offset)) {
                HnswIndexTuple *nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
                uint32          level = level_from_node(nodepage->node);
                for(uint32 i = 0; i <= level; i++) {
                    uint32                    slot_count;
                    ldb_lantern_slot_union_t *slots
                        = get_node_neighbors_mut(external_index_metadata, nodepage->node, i, &slot_count);
                    for(uint32 j = 0; j < slot_count; j++) {
                        uint32 nid = slots[ j ].seqid;
                        slots[ j ].itemPointerData = item_pointers[ nid ];
                    }
                }
            }
        }

        if(block_modified) {
            GenericXLogFinish(gxlogState);
        } else {
            GenericXLogAbort(gxlogState);
        }

        UnlockReleaseBuffer(buf);
    }
    // rewrote all neighbor list. Rewrite graph entry point as well
    uint64                   entry_slot = usearch_header_get_entry_slot(headerp->usearch_header);
    ldb_lantern_slot_union_t updated_slot;
    uint64                   ret_slot;
    updated_slot.itemPointerData = item_pointers[ entry_slot ];
    memcpy(&ret_slot, &updated_slot, sizeof(updated_slot));
    usearch_header_set_entry_slot(headerp->usearch_header, ret_slot);
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
        special_block->firstId = new_vector_data->seqid;
        special_block->lastId = new_vector_data->seqid;
        special_block->nextblockno = InvalidBlockNumber;
    } else {
        ldb_dlog("InsertBranching: we added (NOT FIRST) element to index page");
        assert(special_block->lastId == new_vector_data->seqid - 1);
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
                                  metadata_t          *metadata,
                                  uint32               new_tuple_id,
                                  uint32               new_tuple_level,

                                  ldb_lantern_slot_union_t *slot,
                                  HnswInsertState          *insertstate)
{
    // if any data blocks exist, the last one's buffer will be read into this
    Buffer last_dblock = InvalidBuffer;
    // if a new data buffer is created for the inserted vector, it will be stored here
    Buffer new_dblock = InvalidBuffer;

    Page                       page;
    OffsetNumber               new_tup_at;
    HnswIndexPageSpecialBlock *special_block;
    BlockNumber                new_vector_blockno = InvalidBlockNumber;
    uint32          new_tuple_size = UsearchNodeBytes(metadata, hdr->vector_dim * sizeof(float), new_tuple_level);
    HnswIndexTuple *alloced_tuple = NULL;
    HnswIndexTuple *new_tup_ref = NULL;
    // create the new node
    // allocate buffer to construct the new node
    // note that we allocate more than sizeof(HnswIndexTuple) since the struct has a flexible array member
    // which depends on parameters passed into UsearchNodeBytes above
    alloced_tuple = (HnswIndexTuple *)palloc0(sizeof(HnswIndexTuple) + new_tuple_size);
    alloced_tuple->seqid = new_tuple_id;
    alloced_tuple->level = new_tuple_level;
    alloced_tuple->size = new_tuple_size;

    /*** Add a new tuple corresponding to the added vector to the list of tuples in the index
     *  (create new page if necessary) ***/

    if(hdr->last_data_block == InvalidBlockNumber) {
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

        /**
         * The sizeof(ItemIdData) in the if condition below is necessary to make sure there is enough
         * space for the next line pointer in the page. We do not allocate line pointers in advance and add them
         * as the need arrises. So, we have to always check that there is enough space.
         * You can put the following check in the "else" branch to verify that sizeof(ItemIdData) addition
         * actually makes a difference:
         *
         *   if(PageGetFreeSpace(page) > sizeof(HnswIndexTuple) + alloced_tuple->size) {
         *       ldb_dlog(
         *           "LANTERN: note: there is enough space for the tuple, but not enough for the"
         *           "new ItemIdData line pointer");
         *   }
         *
         **/
        if(PageGetFreeSpace(page) > sizeof(ItemIdData) + sizeof(HnswIndexTuple) + alloced_tuple->size) {
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
    assert(new_vector_blockno != InvalidBlockNumber);
    ItemPointerData tmp;
    BlockIdSet(&tmp.ip_blkid, new_vector_blockno);
    tmp.ip_posid = new_tup_at;
    memcpy(&slot->itemPointerData, &tmp, sizeof(ItemPointerData));
    new_tup_ref = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, new_tup_at));
    assert(new_tup_ref->seqid == new_tuple_id);
    assert(new_tup_ref->level == new_tuple_level);
    assert(new_tup_ref->size == new_tuple_size);
    page = NULL;  // to avoid its accidental use
    /*** Update header ***/
    hdr->num_vectors++;

    pfree(alloced_tuple);
    return new_tup_ref;
}

bool isBlockMapBlock(const HnswBlockMapGroupDesc *blockmap_groups, const int blockmap_group_nr, BlockNumber blockno)
{
    if(!BlockNumberIsValid(blockno)) {
        return false;
    }
    for(int i = 0; i < blockmap_group_nr; i++) {
        if(blockmap_groups[ i ].first_block <= blockno && blockno < blockmap_groups[ i ].first_block + (1 << i)) {
            return true;
        }
    }
    return false;
}

void *ldb_wal_index_node_retriever(void *ctxp, uint64 id)
{
    RetrieverCtx   *ctx = (RetrieverCtx *)ctxp;
    HnswIndexTuple *nodepage;
    Page            page;
    Buffer          buf = InvalidBuffer;
    bool            idx_page_prelocked = false;
    ItemPointerData tid_data;
    BlockNumber     data_block_no;

    memcpy(&tid_data, &id, sizeof(ItemPointerData));
    data_block_no = BlockIdGetBlockNumber(&tid_data.ip_blkid);

    page = extra_dirtied_get(ctx->extra_dirted, data_block_no, NULL);
    if(page == NULL) {
        buf = ReadBufferExtended(ctx->index_rel, MAIN_FORKNUM, data_block_no, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
    } else {
        idx_page_prelocked = true;
    }

    nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, tid_data.ip_posid));
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
#endif

    // if we locked the page, unlock it and only leave a pin on it.
    // otherwise, it must must have been locked because we are in the middle of an update and that node
    // was affected, so we must leave it locked
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
    // fa_cache_insert(&ctx->fa_cache, (uint32)id, nodepage->node);
    return nodepage->node;
}

void *ldb_wal_index_node_retriever_mut(void *ctxp, uint64 id)
{
    RetrieverCtx   *ctx = (RetrieverCtx *)ctxp;
    HnswIndexTuple *nodepage;
    Page            page;
    Buffer          buf = InvalidBuffer;
    ItemPointerData tid_data;
    BlockNumber     data_block_no;
    assert(ctx->header_page_under_wal->version == LDB_WAL_VERSION_NUMBER);

    memcpy(&tid_data, &id, sizeof(ItemPointerData));
    data_block_no = BlockIdGetBlockNumber(&tid_data.ip_blkid);

    // here, we don't bother looking up the fully associative cache because
    // given the current usage of _mut, it is never going to be in the chache

    page = extra_dirtied_get(ctx->extra_dirted, data_block_no, NULL);
    if(page == NULL) {
        extra_dirtied_add_wal_read_buffer(ctx->extra_dirted, ctx->index_rel, MAIN_FORKNUM, data_block_no, &buf, &page);
    }

    nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, tid_data.ip_posid));
    // fa_cache_insert(&ctx->fa_cache, (uint32)id, nodepage->node);
    return nodepage->node;
}
