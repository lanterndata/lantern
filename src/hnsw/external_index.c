
#include <postgres.h>

#include "external_index.h"

#include <access/generic_xlog.h>  // GenericXLog
#include <assert.h>
#include <common/relpath.h>
#include <pg_config.h>       // BLCKSZ
#include <storage/bufmgr.h>  // Buffer
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "cache.h"
#include "extra_dirtied.h"
#include "insert.h"
#include "retriever.h"
#include "usearch.h"

static BlockNumber getBlockMapPageBlockNumber(uint32 *blockmap_page_group_index, int id);

int UsearchNodeBytes(usearch_metadata_t *metadata, int vector_bytes, int level)
{
    const int NODE_HEAD_BYTES = sizeof(usearch_label_t) + 4 /*sizeof dim */ + 4 /*sizeof level*/;
    int       node_bytes = 0;
    node_bytes += NODE_HEAD_BYTES + metadata->neighbors_base_bytes;
    node_bytes += metadata->neighbors_bytes * level;
    node_bytes += vector_bytes;
    return node_bytes;
}

static char *extract_node(char               *data,
                          int                 progress,
                          int                 dim,
                          usearch_metadata_t *metadata,
                          /*->>output*/ int  *node_size,
                          int                *level)
{
    char *tape = data + progress;

    int read_dim_bytes = -1;
    memcpy(&read_dim_bytes, tape + sizeof(usearch_label_t), 4);  //+sizeof(label)
    memcpy(level, tape + sizeof(usearch_label_t) + 4, 4);        //+sizeof(label)+sizeof(dim)
    const int NODE_HEAD_BYTES = sizeof(usearch_label_t) + 4 /*sizeof dim */ + 4 /*sizeof level*/;
    const int VECTOR_BYTES = dim * sizeof(float);
    assert(VECTOR_BYTES == read_dim_bytes);
    *node_size = UsearchNodeBytes(metadata, VECTOR_BYTES, *level);
    return tape;
}

int CreateBlockMapGroup(
    HnswIndexHeaderPage *hdr, Relation index, ForkNumber forkNum, int first_node_index, int blockmap_groupno)
{
    // Create empty blockmap pages for the group
    const int number_of_blockmaps_in_group = 1 << blockmap_groupno;
    assert(hdr != NULL);

    for(int blockmap_id = 0; blockmap_id < number_of_blockmaps_in_group; ++blockmap_id) {
        GenericXLogState *state = GenericXLogStart(index);
        Buffer            buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        if(blockmap_id == 0) {
            hdr->blockmap_page_groups = blockmap_groupno;
            hdr->blockmap_page_group_index[ blockmap_groupno ] = BufferGetBlockNumber(buf);
        }

        Page page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        PageInit(page, BufferGetPageSize(buf), sizeof(HnswIndexPageSpecialBlock));

        HnswIndexPageSpecialBlock *special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
        special->firstId = first_node_index + blockmap_id * HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        special->nextblockno = InvalidBlockNumber;

        HnswBlockmapPage *blockmap = palloc0(BLCKSZ);
        blockmap->first_id = first_node_index + blockmap_id * HNSW_BLOCKMAP_BLOCKS_PER_PAGE;

        if(PageAddItem(page, (Item)blockmap, sizeof(HnswBlockmapPage), InvalidOffsetNumber, false, false)
           == InvalidOffsetNumber) {
            // we always add a single Blockmap page per Index page which has a fixed size that
            // always fits in postgres wal page. So this should never happen
            // (Assumes 8k BLKSZ. we can make HnswBlockmapPage size configurable by BLCKSZ)
            elog(ERROR, "could not add blockmap to page");
        }

        special->lastId = first_node_index + (blockmap_id + 1) * HNSW_BLOCKMAP_BLOCKS_PER_PAGE - 1;
        special->nextblockno = BufferGetBlockNumber(buf) + 1;

        MarkBufferDirty(buf);
        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
        // GenericXLog allows registering up to 4 buffers at a time. So, we cannot set up large BlockMapGroups
        // in a single WAL entry. If we could, we would start the generic Xlog record before the for loop and commit
        // all changes as a whole in the end of this function.
        // Because now all changes do not happen atomically, we probably need to use some other mechanism to make
        // sure we do not corrupt the index in case of a crash in the middle of a BlockMapGroup creation.
    }

    // it is possible that usearch asks for a newly added node from this blockmap range
    // we need to make sure the global header has this information

    return number_of_blockmaps_in_group;
}

void StoreExternalIndexBlockMapGroup(Relation             index,
                                     usearch_index_t      external_index,
                                     HnswIndexHeaderPage *headerp,
                                     ForkNumber           forkNum,
                                     char                *data,
                                     int                 *progress,
                                     int                  dimension,
                                     int                  first_node_index,
                                     size_t               num_added_vectors,
                                     int                  blockmap_groupno)
{
    const int number_of_blockmaps_in_group
        = CreateBlockMapGroup(headerp, index, forkNum, first_node_index, blockmap_groupno);

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
    for(int node_id = first_node_index; node_id < first_node_index + num_added_vectors;) {
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
            if(predicted_next_block != BufferGetBlockNumber(buf)) {
                elog(ERROR,
                     "my block number hypothesis failed. "
                     "predicted block number %d does not match "
                     "actual %d",
                     predicted_next_block,
                     BufferGetBlockNumber(buf));
            }
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
            if(node_level > 100) {
                elog(ERROR,
                     "node level is too large. at id %d"
                     "this is likely a bug in usearch. "
                     "level: %d",
                     node_id,
                     node_level);
            }

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
    for(int blockmap_id = 0; blockmap_id < number_of_blockmaps_in_group; ++blockmap_id) {
        // When the blockmap page group was created, header block was updated accordingly in CreateBlockMapGroup
        // call above.
        const BlockNumber blockmapno = blockmap_id + headerp->blockmap_page_group_index[ blockmap_groupno ];
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

void StoreExternalIndex(Relation        index,
                        usearch_index_t external_index,
                        ForkNumber      forkNum,
                        char           *data,
                        int             dimension,
                        size_t          num_added_vectors)
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
    headerp->vector_dim = dimension;

    headerp->num_vectors = num_added_vectors;
    headerp->blockmap_page_groups = 0;
    MemSet(headerp->blockmap_page_group_index, 0, HNSW_MAX_BLOCKMAP_GROUPS);
    // headerp->blockmap_page_group_index and blockmap_page_groups are
    // updated in a separate wal entry
    headerp->last_data_block = -1;

    memcpy(headerp->usearch_header, data, 64);
    ((PageHeader)header_page)->pd_lower = ((char *)headerp + sizeof(HnswIndexHeaderPage)) - (char *)header_page;

    uint32 number_of_index_pages = num_added_vectors / HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1;
    int    progress = 64;  // usearch header size
    int    blockmap_groupno = 0;
    int    group_node_first_index = 0;
    int    num_added_vectors_remaining = (int)num_added_vectors;
    int    batch_size = HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
    while(num_added_vectors_remaining > 0) {
        StoreExternalIndexBlockMapGroup(index,
                                        external_index,
                                        headerp,
                                        forkNum,
                                        data,
                                        &progress,
                                        dimension,
                                        group_node_first_index,
                                        Min(num_added_vectors_remaining, batch_size),
                                        blockmap_groupno);
        num_added_vectors_remaining -= batch_size;
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
    if(inserted_at == InvalidOffsetNumber) {
        elog(ERROR, "unexpectedly could not add item to the last existing page");
    }
    special_block = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);

    if(PageGetMaxOffsetNumber(page) == 1) {
        // we added the first element to the index page!
        // update firstId
        elog(DEBUG5, "InsertBranching: we added first element to index page");
        special_block->firstId = new_vector_data->id;
        special_block->lastId = new_vector_data->id;
        special_block->nextblockno = InvalidBlockNumber;
    } else {
        elog(DEBUG5, "InsertBranching: we added (NOT FIRST) element to index page");
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
                                  int                  new_tuple_level,
                                  HnswInsertState     *insertstate)
{
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
    bool                       last_dblock_is_dirty = false;
    int             new_tuple_size = UsearchNodeBytes(metadata, hdr->vector_dim * sizeof(float), new_tuple_level);
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
        CreateBlockMapGroup(hdr, index_rel, MAIN_FORKNUM, 0, 0);
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

        const int blockmaps_are_enough
            = new_tuple_id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1 < (1 << (hdr->blockmap_page_groups + 1));
        if(PageGetFreeSpace(page) > sizeof(HnswIndexTuple) + alloced_tuple->size && blockmaps_are_enough) {
            // there is enough space in the last page to fit the new vector
            // so we just append it to the page
            elog(DEBUG5, "InsertBranching: we adding element to existing page");
            new_tup_at = HnswIndexPageAddVector(page, alloced_tuple, alloced_tuple->size);
            new_vector_blockno = BufferGetBlockNumber(last_dblock);
            assert(new_vector_blockno == hdr->last_data_block);

            MarkBufferDirty(last_dblock);
        } else {
            elog(DEBUG5, "InsertBranching: creating new data bage to add an element to");
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
            // new group check if already existing blockmaps are not enough new_tuple_id /
            // HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1 is kth blockmap we check if k is more than already created 2^groups
            if(new_tuple_id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1 >= (1 << (hdr->blockmap_page_groups + 1))) {
                CreateBlockMapGroup(hdr, index_rel, MAIN_FORKNUM, new_tuple_id, hdr->blockmap_page_groups + 1);
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
        int               id_offset = (new_tuple_id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE) * HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        BlockNumber       blockmapno = getBlockMapPageBlockNumber(hdr->blockmap_page_group_index, new_tuple_id);
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
        if(max_offset != FirstOffsetNumber) {
            elog(ERROR, "ERROR: Blockmap max_offset is %d but was supposed to be %d", max_offset, FirstOffsetNumber);
        }

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

static BlockNumber getBlockMapPageBlockNumber(uint32 *blockmap_page_group_index, int id)
{
    assert(id >= 0);
    // Trust me, I'm an engineer!
    id = id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE + 1;
    int k;
    for(k = 0; id >= (1 << k); ++k) {
    }
    return blockmap_page_group_index[ k - 1 ] + (id - (1 << (k - 1)));
}

BlockNumber getDataBlockNumber(RetrieverCtx *ctx, int id, bool add_to_extra_dirtied)
{
    Cache            *cache = &ctx->block_numbers_cache;
    uint32           *blockmap_group_index = ctx->header_page_under_wal != NULL
                                                 ? ctx->header_page_under_wal->blockmap_page_group_index
                                                 : ctx->blockmap_page_group_index_cache;
    BlockNumber       blockmapno = getBlockMapPageBlockNumber(blockmap_group_index, id);
    BlockNumber       blockno, blockno_from_cache;
    HnswBlockmapPage *blockmap_page;
    Page              page;
    Buffer            buf;
    OffsetNumber      offset, max_offset;
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

    blockno_from_cache = cache_get_item(cache, &id);
    if(blockno_from_cache != InvalidBlockNumber) {
        return blockno_from_cache;
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
    max_offset = PageGetMaxOffsetNumber(page);
    assert(max_offset == FirstOffsetNumber);

    blockmap_page = (HnswBlockmapPage *)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));

    offset = id % HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
    blockno = blockmap_page->blocknos[ offset ];
    cache_set_item(cache, &id, blockmap_page->blocknos[ offset ]);
    if(!idx_pagemap_prelocked) {
        UnlockReleaseBuffer(buf);
    }

    return blockno;
}

void *ldb_wal_index_node_retriever(void *ctxp, int id)
{
    RetrieverCtx   *ctx = (RetrieverCtx *)ctxp;
    BlockNumber     data_block_no = getDataBlockNumber(ctx, id, false);
    HnswIndexTuple *nodepage;
    Page            page;
    Buffer          buf;
    OffsetNumber    offset, max_offset;
    bool            idx_page_prelocked = false;

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
        if(nodepage->id == id) {
#if LANTERNDB_USEARCH_LEVEL_DISTRIBUTION
            levels[ nodepage->level ]++;
#endif
#if LANTERNDB_COPYNODES
            if(wal_retriever_area == NULL || wal_retriever_area_offset + nodepage->size > wal_retriever_area_size) {
                elog(ERROR,
                     "ERROR: wal_retriever_area "
                     "is NULL or full");
            }
            memcpy(wal_retriever_area + wal_retriever_area_offset, nodepage->node, nodepage->size);
            wal_retriever_area_offset += nodepage->size;
            if(!idx_page_prelocked) {
                UnlockReleaseBuffer(buf);
            }
            return wal_retriever_area + wal_retriever_area_offset - nodepage->size;
#else
            if(!idx_page_prelocked) {
                if(ctx->takenbuffers[ ctx->takenbuffers_next ] != InvalidBuffer) {
                    ReleaseBuffer(ctx->takenbuffers[ ctx->takenbuffers_next ]);
                    ctx->takenbuffers[ ctx->takenbuffers_next ] = InvalidBuffer;
                }
                ctx->takenbuffers[ ctx->takenbuffers_next ] = buf;
                ctx->takenbuffers_next++;

                if(ctx->takenbuffers_next == TAKENBUFFERS_MAX) {
                    // todo:: use a postgres linked list here (pairing heap) to avoid the limit
                    // and bulk allocation
                    ctx->takenbuffers_next = 0;
                }
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }
            return nodepage->node;
#endif
        }
    }
    if(!idx_page_prelocked) {
        UnlockReleaseBuffer(buf);
    }
    pg_unreachable();
}

void *ldb_wal_index_node_retriever_mut(void *ctxp, int id)
{
    RetrieverCtx   *ctx = (RetrieverCtx *)ctxp;
    BlockNumber     data_block_no = getDataBlockNumber(ctx, id, true);
    HnswIndexTuple *nodepage;
    Page            page;
    Buffer          buf;
    OffsetNumber    offset, max_offset;
    bool            idx_page_prelocked = false;

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
        if(nodepage->id == id) {
            return nodepage->node;
        }
    }

    if(!idx_page_prelocked) {
        UnlockReleaseBuffer(buf);
    }
    pg_unreachable();
}
