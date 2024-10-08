
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
 * Stores hnsw nodes onto postgres index pages
 * Assumes individual writes are not WAL tracked and instead a final
 * pass brings everything under WAL.
 *
 * It will write from the specified buffer (*data) to pages until it reaches the buffer_size
 * In case of external indexing via socket the bytes_written may exceed buffer_size, but it will not overflow
 * as we subtract BLCKSZ from the actual buffer size when calling this function, because the last node may not
 * be fully received from the socket.
 *
 * This function will try to reuse the last page from header pointer if available
 */
void StoreExternalIndexNodes(Relation             index,
                             const metadata_t    *metadata,
                             HnswIndexHeaderPage *headerp,
                             ForkNumber           forkNum,
                             char                *data,
                             uint64               buffer_size,
                             uint64              *progress_bytes,
                             uint64              *num_added_vectors,
                             uint32               pg_dimension,
                             uint32               usearch_dimension,
                             uint32               first_node_index,
                             ItemPointerData     *item_pointers)
{
    assert(sizeof(HnswIndexTuple) + pg_dimension * sizeof(float) <= BLCKSZ);

    // Now add nodes to data pages
    char  *node = 0;
    bool   force_create_page = false;
    int    node_size = 0;
    int    node_level = 0;
    uint32 predicted_next_block = InvalidBlockNumber;
    uint32 last_block = InvalidBlockNumber;
    uint32 node_id = first_node_index;
    uint64 bytes_written = 0;

    HnswIndexTuple *bufferpage = palloc(BLCKSZ);

    Buffer                     buf = 0;
    Buffer                     newbuf;
    BlockNumber                blockno;
    OffsetNumber               offsetno;
    Page                       page;
    HnswIndexPageSpecialBlock *special = NULL;

    if(headerp->last_data_block != InvalidBlockNumber) {
        // reuse last page
        buf = ReadBufferExtended(index, forkNum, headerp->last_data_block, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
        blockno = headerp->last_data_block;
        last_block = blockno;
        page = BufferGetPage(buf);
        special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    } else {
        force_create_page = true;
    }

    /* Add all the vectors to the index pages */
    while(bytes_written < buffer_size) {
        /************* extract node from usearch index************/
        node = data + bytes_written;
        node_level = level_from_node(node);
        node_size = node_tuple_size(node, usearch_dimension, metadata);

        // note: even if the condition is true, nodepage may be too large
        // as the condition does not take into account the flexible array component
        // force_create_page is set to true in 2 conditions:
        //   1. The index pages are empty and we should create the first page on start.
        //      This is set based on condition above when the last_data_block is equal to InvalidBlockNumber
        //   2. There's not enough space in current page to store the node, so we should create a new page.
        //      This is set if PageAddItem will return InvalidOffsetNumber
        if(force_create_page || (PageGetFreeSpace(page) < sizeof(HnswIndexTuple) + node_size)) {
            // create new page
            newbuf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
            blockno = BufferGetBlockNumber(newbuf);

            if(special) {
                // fill the last page special info if buffer was modified
                special->nextblockno = blockno;
                special->lastId = node_id - 1;
                MarkBufferDirty(buf);
            }

            if(buf) {
                UnlockReleaseBuffer(buf);
            }

            // initialize new page
            buf = newbuf;
            page = BufferGetPage(buf);
            PageInit(page, BufferGetPageSize(buf), sizeof(HnswIndexPageSpecialBlock));
            special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
            special->firstId = node_id;
            special->nextblockno = InvalidBlockNumber;
            last_block = BufferGetBlockNumber(buf);
            force_create_page = false;

            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        }

        memset(bufferpage, 0, BLCKSZ);

        bufferpage->seqid = node_id;
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
            // write failed, continue to get a fresh page
            force_create_page = true;
            continue;
        }

        // we successfully recorded the node. move to the next one
        BlockIdSet(&item_pointers[ node_id ].ip_blkid, blockno);
        item_pointers[ node_id ].ip_posid = offsetno;
        bytes_written += node_size;
        node_id++;
    }

    special->lastId = node_id - 1;

    UnlockReleaseBuffer(buf);

    headerp->last_data_block = last_block;

    *progress_bytes += bytes_written;
    *num_added_vectors += (node_id - first_node_index);

    LDB_FAILURE_POINT_CRASH_IF_ENABLED("just_before_updating_blockmaps_after_inserting_nodes");
}

void StoreExternalEmptyIndex(
    Relation index, ForkNumber forkNum, char *data, int dimensions, usearch_init_options_t *opts)
{
    // this method is intended to store empty indexes for unlogged tables (ambuildempty method) and should hence be
    // called with forkNum = INIT_FORKNUM

    Buffer header_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    // even when we are creating a new page, it must always be the first page we create
    // and should therefore have BlockNumber 0
    assert(BufferGetBlockNumber(header_buf) == 0);

    LockBuffer(header_buf, BUFFER_LOCK_EXCLUSIVE);

    Page header_page = BufferGetPage(header_buf);

    PageInit(header_page, BufferGetPageSize(header_buf), 0);

    HnswIndexHeaderPage *headerp = (HnswIndexHeaderPage *)PageGetContents(header_page);

    headerp->magicNumber = LDB_WAL_MAGIC_NUMBER;
    headerp->version = LDB_WAL_VERSION_NUMBER;
    headerp->vector_dim = dimensions;
    headerp->m = opts->connectivity;
    headerp->ef_construction = opts->expansion_add;
    headerp->ef = opts->expansion_search;
    headerp->metric_kind = opts->metric_kind;
    headerp->quantization = opts->quantization;
    headerp->pq = opts->pq;
    headerp->num_centroids = opts->num_centroids;
    headerp->num_subvectors = opts->num_subvectors;

    headerp->num_vectors = 0;

    headerp->last_data_block = InvalidBlockNumber;

    memcpy(headerp->usearch_header, data, USEARCH_HEADER_SIZE);
    ((PageHeader)header_page)->pd_lower = ((char *)headerp + sizeof(HnswIndexHeaderPage)) - (char *)header_page;

    MarkBufferDirty(header_buf);
    UnlockReleaseBuffer(header_buf);

    // Write a WAL record containing a full image of the page. Even though this is an unlogged table that doesn't use
    // WAL, this line appears to flush changes to disc immediately (and not waiting after the first checkpoint). This is
    // important because this empty index will live in the init fork, where it will be used to reset the unlogged index
    // after a crash, and so we need this written to disc in order to have proper crash recovery functionality available
    // immediately. Otherwise, if a crash occurs before the first postgres checkpoint, postgres can't read the init fork
    // from disc and we will have a corrupted index when postgres attempts recovery. This is also what nbtree access
    // method's implementation does for empty unlogged indexes (ambuildempty implementation).
    log_newpage_range(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), false);
}

/*
 * This function will write usearch index into postgres index pages
 * It has 2 paths:
 *  - The first path is when we already have usearch index file mmaped into memory
 *    and should just iterate over it and write into pages
 *  - The second path is when we are reading the index file from remote socket
 *    in this case we will read the file in 10MB chunks and try to write that chunk into pages.
 * */

void StoreExternalIndex(Relation                 index,
                        const metadata_t        *external_index_metadata,
                        ForkNumber               forkNum,
                        char                    *data,
                        usearch_init_options_t  *opts,
                        uint32                   pg_dimensions,
                        size_t                   num_added_vectors,
                        external_index_socket_t *external_index_socket,
                        uint64                   index_file_size)
{
    Buffer header_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    // even when we are creating a new page, it must always be the first page we create
    // and should therefore have BLockNumber 0
    assert(BufferGetBlockNumber(header_buf) == 0);
    LockBuffer(header_buf, BUFFER_LOCK_EXCLUSIVE);

    Page header_page = BufferGetPage(header_buf);

    PageInit(header_page, BufferGetPageSize(header_buf), 0);
    HnswIndexHeaderPage *headerp = (HnswIndexHeaderPage *)PageGetContents(header_page);

    headerp->magicNumber = LDB_WAL_MAGIC_NUMBER;
    headerp->version = LDB_WAL_VERSION_NUMBER;
    headerp->vector_dim = opts->dimensions;
    if(opts->metric_kind == usearch_metric_hamming_k) {
        headerp->vector_dim /= sizeof(int32) * CHAR_BIT;
    }
    headerp->m = opts->connectivity;
    headerp->ef_construction = opts->expansion_add;
    headerp->ef = opts->expansion_search;
    headerp->metric_kind = opts->metric_kind;
    headerp->quantization = opts->quantization;

    headerp->num_vectors = num_added_vectors;
    headerp->pq = opts->pq;
    headerp->num_centroids = opts->num_centroids;
    headerp->num_subvectors = opts->num_subvectors;

    headerp->last_data_block = InvalidBlockNumber;

    // allocate some pages for pq codebook
    // for now, always add this blocks to make sure all tests run with these and nothing fails
    if(opts->pq) {
        const int num_clusters = 256;
        // total bytes for num_clusters clusters = num_clusters * vector_size_in_bytes
        // total pages for codebook = bytes / page_size
        for(int i = 0; i < ceil((float)(num_clusters)*pg_dimensions * sizeof(float) / BLCKSZ); i++) {
            Buffer cluster_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
            LockBuffer(cluster_buf, BUFFER_LOCK_EXCLUSIVE);
            Page page = BufferGetPage(cluster_buf);
            PageInit(page, BufferGetPageSize(cluster_buf), 0);
            // todo:: actually, write the codebook here
            MarkBufferDirty(cluster_buf);
            UnlockReleaseBuffer(cluster_buf);
        }
    }

    uint64 progress = USEARCH_HEADER_SIZE;
    uint64 tuples_indexed = 0;

    ItemPointerData *item_pointers = palloc(num_added_vectors * sizeof(ItemPointerData));

    ((PageHeader)header_page)->pd_lower = ((char *)headerp + sizeof(HnswIndexHeaderPage)) - (char *)header_page;

    memcpy(headerp->usearch_header, data, USEARCH_HEADER_SIZE);

    if(external_index_socket != NULL) {
        uint64 bytes_read = 0;
        uint64 total_bytes_read = USEARCH_HEADER_SIZE;
        char  *external_index_data = palloc0(EXTERNAL_INDEX_FILE_BUFFER_SIZE);
        uint32 buffer_position = 0;
        uint64 local_progress = 0;
        uint64 data_size = 0;

        while(tuples_indexed < num_added_vectors) {
            local_progress = 0;

            bytes_read = external_index_read_all(external_index_socket,
                                                 external_index_data + buffer_position,
                                                 EXTERNAL_INDEX_FILE_BUFFER_SIZE - buffer_position);
            total_bytes_read += bytes_read;

            if(total_bytes_read == index_file_size) {
                // index file streaming finished
                data_size = buffer_position + bytes_read;
            } else {
                data_size = EXTERNAL_INDEX_FILE_BUFFER_SIZE - BLCKSZ;
            }

            // store nodes into index pages
            StoreExternalIndexNodes(index,
                                    external_index_metadata,
                                    headerp,
                                    forkNum,
                                    external_index_data,
                                    data_size,
                                    &local_progress,
                                    &tuples_indexed,
                                    pg_dimensions,
                                    opts->dimensions,
                                    tuples_indexed,
                                    item_pointers);

            // rotate buffer
            buffer_position = EXTERNAL_INDEX_FILE_BUFFER_SIZE - local_progress;

            if(total_bytes_read != index_file_size) {
                assert(buffer_position <= BLCKSZ);
                memcpy(external_index_data, external_index_data + local_progress, buffer_position);
            } else {
                pfree(external_index_data);
            }

            progress += local_progress;
            CHECK_FOR_INTERRUPTS();
        }
    } else {
        uint64 added_vectors = 0;
        StoreExternalIndexNodes(index,
                                external_index_metadata,
                                headerp,
                                forkNum,
                                data + USEARCH_HEADER_SIZE,
                                index_file_size - USEARCH_HEADER_SIZE,
                                &progress,
                                &added_vectors,
                                pg_dimensions,
                                opts->dimensions,
                                0,
                                item_pointers);
        assert(added_vectors == num_added_vectors);
    }
    // this is where I rewrite all neighbors to use BlockNumber
    Buffer              buf;
    HnswIndexHeaderPage header_copy;
    Page                page;
    OffsetNumber        offset, maxoffset;
    header_copy = *(HnswIndexHeaderPage *)PageGetContents(header_page);
    // rewrite neighbor lists in terms of block numbers
    for(BlockNumber blockno = 1;
        BlockNumberIsValid(header_copy.last_data_block) && blockno <= header_copy.last_data_block;
        blockno++) {
        buf = ReadBufferExtended(index, MAIN_FORKNUM, blockno, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        maxoffset = PageGetMaxOffsetNumber(page);

        // when index is a pq-index, there will be pq header pages that are currently empty
        // the loop below will skip those. in the future, when those pages are filled up,
        // we need to add a branch here and skip those pages

        for(offset = FirstOffsetNumber; offset <= maxoffset; offset = OffsetNumberNext(offset)) {
            HnswIndexTuple *nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
            uint32          level = level_from_node(nodepage->node);
            for(uint32 i = 0; i <= level; i++) {
                uint32                      slot_count;
                ldb_unaligned_slot_union_t *slots
                    = get_node_neighbors_mut(external_index_metadata, nodepage->node, i, &slot_count);
                for(uint32 j = 0; j < slot_count; j++) {
                    uint32 nid = 0;
                    memcpy(&nid, &slots[ j ].seqid, sizeof(slots[ j ].seqid));
                    memcpy(&slots[ j ].itemPointerData, &item_pointers[ nid ], sizeof(ItemPointerData));
                }
            }
        }

        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
    // rewrote all neighbor list. Rewrite graph entry point as well
    uint64 entry_slot = usearch_header_get_entry_slot(headerp->usearch_header);
    uint64 ret_slot = 0;
    // rewrite header slot unless we created an empty index
    if(num_added_vectors > 0) {
        assert(entry_slot < UINT32_MAX);
        memcpy(&ret_slot, &item_pointers[ (uint32)entry_slot ], sizeof(ItemPointerData));
        usearch_header_set_entry_slot(headerp->usearch_header, ret_slot);
    }
    MarkBufferDirty(header_buf);
    UnlockReleaseBuffer(header_buf);
    /*
     * We didn't write WAL records as we built the index, so if
     * WAL-logging is required, write all pages to the WAL now.
     * Note: the WAL logging function is going to take an exclusive lock on all pages, so
     * we must call this after unlocking all the pages (header page, in particular, here).
     * Since we are in index building phase, postgres has taken an AccessExclusiveLock on the relation itself,
     * so this is safe
     */
    if(RelationNeedsWAL(index)) {
        log_newpage_range(index, MAIN_FORKNUM, 0, RelationGetNumberOfBlocks(index), true);
    }
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

                                  ldb_unaligned_slot_union_t *slot,
                                  HnswInsertState            *insertstate)
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
    assert(new_tup_ref->size == new_tuple_size);
    page = NULL;  // to avoid its accidental use
    /*** Update header ***/
    hdr->num_vectors++;

    pfree(alloced_tuple);
    return new_tup_ref;
}

void *ldb_wal_index_node_retriever(void *ctxp, unsigned long long id)
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

void *ldb_wal_index_node_retriever_mut(void *ctxp, unsigned long long id)
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
