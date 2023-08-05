
#include "postgres.h"

#include "external_index.h"

#include <access/generic_xlog.h>  // GenericXLog
#include <assert.h>
#include <utils/hsearch.h>

#include "cache.h"
#include "common/relpath.h"
#include "insert.h"
#include "pg_config.h"       // BLCKSZ
#include "storage/bufmgr.h"  // Buffer
#include "usearch.h"
#include "utils/relcache.h"

static inline void fill_blockno_mapping();
static HTAB       *wal_retriever_block_numbers_cache = NULL;

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

void StoreExternalIndex(Relation        index,
                        usearch_index_t external_index,
                        ForkNumber      forkNum,
                        char           *data,
                        size_t          external_index_size,
                        int             dimension,
                        size_t          num_added_vectors)
{
    char  *node = 0;
    int    node_size = 0;
    int    node_level = 0;
    int    progress = 64;  // usearch header size
    uint32 predicted_next_block = InvalidBlockNumber;
    uint32 last_block = -1;
    uint32 block = 1;
    uint32 blockno_index_start = -1;

    if(num_added_vectors >= HNSW_MAX_INDEXED_VECTORS) {
        elog(ERROR, "too many vectors to store in hnsw index. Current limit: %d", HNSW_MAX_INDEXED_VECTORS);
    }

    BlockNumber    *wal_retriever_block_numbers = NULL;
    HnswIndexTuple *bufferpage = palloc(BLCKSZ);

    usearch_metadata_t metadata = usearch_metadata(external_index, NULL);

    // header page is created twice. it is always at block=0 so the second time just overrides it
    // it is added here to make sure a data block does not get to block=0.
    // after somem sleep I will prob find a better way to do this
    CreateHeaderPage(index, data, forkNum, dimension, num_added_vectors, -1, -1, false);

    /* Add all the vectors to the WAL */
    for(int node_id = 0; node_id < num_added_vectors;) {
        // 1. create HnswIndexTuple

        // 2. fill header and special

        // 3. while available space is larger than node_size
        //     3a. add node to page

        // 4. commit buffer

        Buffer                     buf;
        Page                       page;
        GenericXLogState          *state;
        HnswIndexPageSpecialBlock *special;

        buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
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

        special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
        special->firstId = node_id;
        special->nextblockno = InvalidBlockNumber;

        // note: even if the condition is true, nodepage may be too large
        // as the condition does not take into account the flexible array component
        while(PageGetFreeSpace(page) > sizeof(HnswIndexTuple) + dimension * sizeof(float)) {
            if(node_id >= num_added_vectors) break;
            memset(bufferpage, 0, BLCKSZ);
            /************* extract node from usearch index************/

            node = extract_node(data,
                                progress,
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
            progress += node_size;
            node_id++;
        }
        if(node_id < num_added_vectors) {
            predicted_next_block = BufferGetBlockNumber(buf) + 1;
        } else {
            last_block = predicted_next_block;
            predicted_next_block = InvalidBlockNumber;
        }
        special->lastId = node_id - 1;
        special->nextblockno = predicted_next_block;
        MarkBufferDirty(buf);
        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
    }

    HnswBlockmapPage *blockmap = palloc0(BLCKSZ);
    // I do update header page yet another time here to update num_vectors and the global
    // HEADER_FOR_EXTERNAL_RETRIEVER which is used by fill_blockno_mapping
    CreateHeaderPage(index, data, forkNum, dimension, num_added_vectors, blockno_index_start, last_block, true);
    wal_retriever_block_numbers = palloc0(sizeof(BlockNumber) * num_added_vectors);
    if(wal_retriever_block_numbers == NULL) elog(ERROR, "could not allocate wal_retriever_block_numbers");
    INDEX_RELATION_FOR_RETRIEVER = index;
    fill_blockno_mapping(wal_retriever_block_numbers);

    /* Create blockmap for <=2 memory access retreival of any node*/
    for(int node_id = 0; node_id < HNSW_MAX_INDEXED_VECTORS; node_id += HNSW_BLOCKMAP_BLOCKS_PER_PAGE) {
        Buffer                     buf;
        Page                       page;
        GenericXLogState          *state;
        HnswIndexPageSpecialBlock *special;

        buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
        if(blockno_index_start == -1) {
            blockno_index_start = BufferGetBlockNumber(buf);
        }
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        PageInit(page, BufferGetPageSize(buf), sizeof(HnswIndexPageSpecialBlock));

#if 0
        /* I was trying to figure out how postgres allocates blocks
         * It seems it always gives n+1 block after block n and have not been
         * able to find a counterexample
         */
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
#endif

        special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
        special->firstId = node_id;
        special->nextblockno = InvalidBlockNumber;

        memset(blockmap, 0, BLCKSZ);
        blockmap->first_id = node_id;

        // for already inserted nodes, we populate corresponding to blockmap pages
        // Otherwise, we only populate expected first node_id and leave the rest for INSERTS
        if(node_id < num_added_vectors) {
            memcpy(blockmap->blocknos,
                   wal_retriever_block_numbers + node_id,
                   sizeof(BlockNumber) * HNSW_BLOCKMAP_BLOCKS_PER_PAGE);
        }
        if(PageAddItem(page, (Item)blockmap, sizeof(HnswBlockmapPage), InvalidOffsetNumber, false, false)
           == InvalidOffsetNumber) {
            // we always add a single Blockmap page per Index page which has a fixed size that
            // always fits in postgres wal page. So this should never happen
            // (Assumes 8k BLKSZ. we can make HnswBlockmapPage size configurable by BLCKSZ)
            elog(ERROR, "could not add blockmap to page");
        }

        if(node_id < num_added_vectors) {
            predicted_next_block = BufferGetBlockNumber(buf) + 1;
        } else {
            last_block = predicted_next_block;
            predicted_next_block = InvalidBlockNumber;
        }
        special->lastId = node_id - 1;
        special->nextblockno = predicted_next_block;
        MarkBufferDirty(buf);
        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
    }

    pfree(wal_retriever_block_numbers);
    wal_retriever_block_numbers = NULL;
    pfree(blockmap);
    CreateHeaderPage(index, data, forkNum, dimension, num_added_vectors, blockno_index_start, last_block, true);

    pfree(bufferpage);
}

void CreateHeaderPage(Relation   index,
                      char      *usearchHeader64,
                      ForkNumber forkNum,
                      int        vector_dim,
                      int        num_vectors,
                      uint32     blockno_index_start,
                      uint32     num_blocks,
                      bool       update)
{
    Buffer               buf;
    Page                 page;
    GenericXLogState    *state;
    HnswIndexHeaderPage *headerp;
    BlockNumber          headerblockno = 0;

    if(update) {
        headerblockno = 0;
    } else {
        headerblockno = P_NEW;
    }
    buf = ReadBufferExtended(index, forkNum, headerblockno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(page, BufferGetPageSize(buf), 0);

    headerp = (HnswIndexHeaderPage *)PageGetContents(page);
    headerp->magicNumber = LDB_WAL_MAGIC_NUMBER;
    headerp->version = LDB_WAL_VERSION_NUMBER;
    headerp->vector_dim = vector_dim;
    headerp->num_vectors = num_vectors;
    headerp->first_data_block = InvalidBlockNumber;
    headerp->last_data_block = InvalidBlockNumber;
    if(blockno_index_start >= headerblockno + 2) {
        // headerblockno+1 is a data page
        headerp->first_data_block = headerblockno + 1;
        headerp->last_data_block = blockno_index_start - 1;
    } else {
        elog(WARNING, "creating index on empty table");
    }
    headerp->last_data_block = blockno_index_start - 1;
    headerp->blockno_index_start = blockno_index_start;
    headerp->num_blocks = num_blocks;
    memcpy(headerp->usearch_header, usearchHeader64, 64);
    ((PageHeader)page)->pd_lower = ((char *)headerp + sizeof(HnswIndexHeaderPage)) - (char *)page;

    HEADER_FOR_EXTERNAL_RETRIEVER = *headerp;
    assert(BufferGetBlockNumber(buf) == 0);

    MarkBufferDirty(buf);
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

Relation            INDEX_RELATION_FOR_RETRIEVER;
HnswIndexHeaderPage HEADER_FOR_EXTERNAL_RETRIEVER;
Buffer             *EXTRA_DIRTIED;
Page               *EXTRA_DIRTIED_PAGE;
int                 EXTRA_DIRTIED_SIZE = 0;

#if LANTERNDB_COPYNODES
static char *wal_retriever_area = NULL;
static int   wal_retriever_area_size = 0;
static int   wal_retriever_area_offset = 0;
#else

#define TAKENBUFFERS_MAX 1000
static Buffer *takenbuffers;
static int     takenbuffers_next = 0;
#endif

void ldb_wal_retriever_area_init(int size)
{
#if LANTERNDB_COPYNODES
    wal_retriever_area = palloc(size);
    if(wal_retriever_area == NULL) elog(ERROR, "could not allocate wal_retriever_area");
    wal_retriever_area_size = size;
    wal_retriever_area_offset = 0;
#else
    takenbuffers = palloc0(sizeof(Buffer) * TAKENBUFFERS_MAX);
    if(takenbuffers_next > 0) {
            elog(ERROR, "takenbuffers_next > 0 %d", takenbuffers_next);
    }
#endif

    if(HEADER_FOR_EXTERNAL_RETRIEVER.num_vectors <= 0) {
        elog(ERROR, "ldb_wal_retriever_area_init called with num_vectors <= 0");
    }
    /* fill in a buffer with blockno index information, before spilling it to disk */
    wal_retriever_block_numbers_cache = cache_create();

    if(wal_retriever_block_numbers_cache == NULL) elog(ERROR, "could not allocate wal_retriever_block_numbers_cache");

    if(EXTRA_DIRTIED_SIZE > 0) {
        elog(INFO, "EXTRA_DIRTIED_SIZE > 0 %d", EXTRA_DIRTIED_SIZE);
        for(int i = 0; i < EXTRA_DIRTIED_SIZE; i++) {
            elog(INFO, "buf %d in extra_dirtied : %d", i, EXTRA_DIRTIED[ i ]);
        }
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
                                  Buffer (*extra_dirtied)[ LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS ],
                                  Page (*extra_dirtied_page)[ LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS ],
                                  int *extra_dirtied_size)
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
        assert(hdr->first_data_block == InvalidBlockNumber);
        elog(ERROR, "inserting into an empty table not supported");
        // index is created on the empty table.
        // allocate the first page here
    } else {
        last_dblock = ReadBufferExtended(index_rel, MAIN_FORKNUM, hdr->last_data_block, RBM_NORMAL, NULL);
        for(int i = 0; i < *extra_dirtied_size; i++) {
            if(last_dblock == (*extra_dirtied)[ i ]) {
                page = (*extra_dirtied_page)[ i ];
                last_dblock_is_dirty = true;
                ReleaseBuffer(last_dblock);
                // todo:: get rid of all uses of last_dblock after this point
                //  using it is safe since we hold a pin via whoever added an exclusevly locked
                //  page to extra_dirtied but the code becomes even more confusing
                break;
            }
        }

        if(!last_dblock_is_dirty) {
            LockBuffer(last_dblock, BUFFER_LOCK_EXCLUSIVE);
            page = GenericXLogRegisterBuffer(state, last_dblock, LDB_GENERIC_XLOG_DELTA_IMAGE);

            // we only add last_dblock to extra_dirtied if we were the first to touch it
            // otherwise, we skip, since last_dblock is already in extra_dirtied
            // (we found out from extra_dirtied that we were not first to touch it, after all!)
            (*extra_dirtied)[ (*extra_dirtied_size)++ ] = last_dblock;
            (*extra_dirtied_page)[ (*extra_dirtied_size) - 1 ] = page;
        }

        if(PageGetFreeSpace(page) > sizeof(HnswIndexTuple) + alloced_tuple->size) {
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

            (*extra_dirtied)[ (*extra_dirtied_size)++ ] = new_dblock;
            (*extra_dirtied_page)[ (*extra_dirtied_size) - 1 ] = page;

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
        BlockNumber       blockmapno = hdr->blockno_index_start + id_offset / HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        Page              blockmap_page;
        HnswBlockmapPage *blockmap;
        int               max_offset;

        // todo:: figure out how/from where /usr/include/strings.h is included at this point
        // (noticed that index is a function defined there)

        blockmap_block = ReadBufferExtended(index_rel, MAIN_FORKNUM, blockmapno, RBM_NORMAL, NULL);
        LockBuffer(blockmap_block, BUFFER_LOCK_EXCLUSIVE);
        blockmap_page = GenericXLogRegisterBuffer(state, blockmap_block, LDB_GENERIC_XLOG_DELTA_IMAGE);

        (*extra_dirtied)[ (*extra_dirtied_size)++ ] = blockmap_block;
        (*extra_dirtied_page)[ (*extra_dirtied_size) - 1 ] = blockmap_page;
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

void ldb_wal_retriever_area_reset()
{
#if LANTERNDB_COPYNODES
    wal_retriever_area_offset = 0;
#else
    for(int i = 0; i < TAKENBUFFERS_MAX; i++) {
            if(takenbuffers[ i ] == InvalidBuffer) {
                continue;
        }
            ReleaseBuffer(takenbuffers[ i ]);
            takenbuffers[ i ] = InvalidBuffer;
    }
    takenbuffers_next = 0;
#endif
}

void ldb_wal_retriever_area_free()
{
    cache_destroy(wal_retriever_block_numbers_cache);
    wal_retriever_block_numbers_cache = NULL;
#if LANTERNDB_COPYNODES
    pfree(wal_retriever_area);
    wal_retriever_area = NULL;
    wal_retriever_area_size = 0;
    wal_retriever_area_offset = 0;
#else
    for(int i = 0; i < TAKENBUFFERS_MAX; i++) {
            if(takenbuffers[ i ] == InvalidBuffer) {
                continue;
        }
            ReleaseBuffer(takenbuffers[ i ]);
            takenbuffers[ i ] = InvalidBuffer;
    }
    pfree(takenbuffers);
    takenbuffers_next = 0;
#endif
}

static inline void *wal_index_node_retriever_exact(int id)
{
    HnswBlockmapPage *blockmap_page;
    // fix blockmap size to X pages -> X * 8k overhead -> can have max table size of 2000 * X
    // fix blockmap size to 20000 pages -> 160 MB overhead -> can have max table size of 40M rows
    // 40M vectors of 128 dims -> 40 * 128 * 4 = 163840 Mbytes -> 163 GB... will not fly
    BlockNumber     blockmapno = HEADER_FOR_EXTERNAL_RETRIEVER.blockno_index_start + id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
    BlockNumber     blockno;
    Buffer          buf;
    Page            page;
    HnswIndexTuple *nodepage;
    OffsetNumber    offset, max_offset;
    bool            idx_page_prelocked = false;
    bool            idx_pagemap_prelocked = false;
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

    BlockNumber blockno_from_cache = cache_get_item(wal_retriever_block_numbers_cache, &id);
    if(blockno_from_cache != InvalidBlockNumber) {
        blockno = blockno_from_cache;
    } else {
        int id_offset = (id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE) * HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        buf = ReadBufferExtended(INDEX_RELATION_FOR_RETRIEVER, MAIN_FORKNUM, blockmapno, RBM_NORMAL, NULL);
        // you might expect that this is unnecessary, since we always unlock the pagemap page right after reading the
        // necessary information into wal_retriever_block_numbers
        // BUT it is necessary because _mut() retriever might have blocked the current page in order to add a value to
        // it!
        for(int i = 0; i < EXTRA_DIRTIED_SIZE; i++) {
            if(EXTRA_DIRTIED[ i ] == buf) {
                idx_pagemap_prelocked = true;
                page = EXTRA_DIRTIED_PAGE[ i ];
                ReleaseBuffer(buf);
                buf = InvalidBuffer;
            }
        }

        if(!idx_pagemap_prelocked) {
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
        }
        max_offset = PageGetMaxOffsetNumber(page);

        if(max_offset != FirstOffsetNumber) {
            elog(ERROR, "ERROR: Blockmap max_offset is %d but was supposed to be %d", max_offset, FirstOffsetNumber);
        }
        blockmap_page = (HnswBlockmapPage *)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
        int key = id % HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        blockno = blockmap_page->blocknos[ key ];
        cache_set_item(wal_retriever_block_numbers_cache, &key, blockmap_page->blocknos[ key ]);
        if(!idx_pagemap_prelocked) {
            UnlockReleaseBuffer(buf);
        }
    }

    // now I know the block from the blockmap. now find the tuple in the block
    buf = ReadBufferExtended(INDEX_RELATION_FOR_RETRIEVER, MAIN_FORKNUM, blockno, RBM_NORMAL, NULL);
    for(int i = 0; i < EXTRA_DIRTIED_SIZE; i++) {
        if(EXTRA_DIRTIED[ i ] == buf) {
            idx_page_prelocked = true;
            page = EXTRA_DIRTIED_PAGE[ i ];

            ReleaseBuffer(buf);
            buf = InvalidBuffer;
        }
    }

    if(!idx_page_prelocked) {
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
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
                    if(takenbuffers[ takenbuffers_next ] != InvalidBuffer) {
                        ReleaseBuffer(takenbuffers[ takenbuffers_next ]);
                        takenbuffers[ takenbuffers_next ] = InvalidBuffer;
                }
                    takenbuffers[ takenbuffers_next ] = buf;
                    takenbuffers_next++;

                    if(takenbuffers_next == TAKENBUFFERS_MAX) {
                        // if(takenbuffers[ 0 ] != InvalidBuffer) {
                    //     ReleaseBuffer(takenbuffers[ 0 ]);
                    //     takenbuffers[ 0 ] = InvalidBuffer;
                    // }
                    takenbuffers_next = 0;
                }
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }
            return nodepage->node;
#endif
        }
    }
    UnlockReleaseBuffer(buf);
    elog(ERROR, "reached end of retriever without finding node %d", id);
}

void *ldb_wal_index_node_retriever_mut(int id)
{
    HnswBlockmapPage *blockmap_page;
    BlockNumber     blockmapno = HEADER_FOR_EXTERNAL_RETRIEVER.blockno_index_start + id / HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
    BlockNumber     blockno;
    Buffer          buf;
    Page            page;
    HnswIndexTuple *nodepage;
    OffsetNumber    offset, max_offset;
    bool            idx_page_prelocked = false;
    bool            idx_pagemap_prelocked = false;

    BlockNumber blockno_from_cache = cache_get_item(wal_retriever_block_numbers_cache, &id);
    if(blockno_from_cache != InvalidBlockNumber) {
        blockno = blockno_from_cache;
    } else {
        // if this is th elast page and blocknos is not filled up, only read the part that is filled up
        // todo:: when we are adding vectors, this does not take into account that the blockmap now has soft-length
        // of num_vectors + 1
        buf = ReadBufferExtended(INDEX_RELATION_FOR_RETRIEVER, MAIN_FORKNUM, blockmapno, RBM_NORMAL, NULL);
        for(int i = 0; i < EXTRA_DIRTIED_SIZE; i++) {
            if(EXTRA_DIRTIED[ i ] == buf) {
                idx_pagemap_prelocked = true;
                page = EXTRA_DIRTIED_PAGE[ i ];

                ReleaseBuffer(buf);
                buf = InvalidBuffer;
            }
        }

        if(!idx_pagemap_prelocked) {
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
        }
        max_offset = PageGetMaxOffsetNumber(page);

        if(max_offset != FirstOffsetNumber) {
            elog(ERROR, "ERROR: Blockmap max_offset is %d but was supposed to be %d", max_offset, FirstOffsetNumber);
        }
        blockmap_page = (HnswBlockmapPage *)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));

        CacheKey key = id % HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
        blockno = blockmap_page->blocknos[ key ];
        cache_set_item(wal_retriever_block_numbers_cache, &key, blockno);
        if(!idx_pagemap_prelocked) {
            UnlockReleaseBuffer(buf);
        }
    }

    // now I know the block from the blockmap. now find the tuple in the block
    buf = ReadBufferExtended(INDEX_RELATION_FOR_RETRIEVER, MAIN_FORKNUM, blockno, RBM_NORMAL, NULL);
    for(int i = 0; i < EXTRA_DIRTIED_SIZE; i++) {
        if(EXTRA_DIRTIED[ i ] == buf) {
            idx_page_prelocked = true;
            page = EXTRA_DIRTIED_PAGE[ i ];

            ReleaseBuffer(buf);
            buf = InvalidBuffer;
        }
    }

    if(!idx_page_prelocked) {
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        assert(EXTRA_DIRTIED_SIZE + 1 < LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
        EXTRA_DIRTIED[ EXTRA_DIRTIED_SIZE++ ] = buf;
        page = BufferGetPage(buf);
        EXTRA_DIRTIED_PAGE[ EXTRA_DIRTIED_SIZE - 1 ] = page;
        // todo:: q::
        //  does it matter whether we mark a buffer dirty before or after writing to it?
        MarkBufferDirty(buf);
    }

    max_offset = PageGetMaxOffsetNumber(page);

    for(offset = FirstOffsetNumber; offset <= max_offset; offset = OffsetNumberNext(offset)) {
        nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
        if(nodepage->id == id) {
            return nodepage->node;
        }
    }
    // **** UNREACHABLE ****
    if(!idx_page_prelocked) {
        UnlockReleaseBuffer(buf);
    }
    elog(ERROR, "reached end of retriever without finding node %d", id);
}

void *ldb_wal_index_node_retriever(int id)
{
    return wal_index_node_retriever_exact(id);
    // return wal_index_node_retriever_sequential(id);
    // return wal_index_node_retriever_binary(id);
}

static inline void fill_blockno_mapping(BlockNumber *wal_retriever_block_numbers)
{
    BlockNumber                blockno = 1;
    Buffer                     buf;
    Page                       page;
    HnswIndexPageSpecialBlock *special_block;
    HnswIndexTuple            *nodepage;
    OffsetNumber               offset, max_offset;

    while(BlockNumberIsValid(blockno)) {
        buf = ReadBufferExtended(INDEX_RELATION_FOR_RETRIEVER, MAIN_FORKNUM, blockno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        max_offset = PageGetMaxOffsetNumber(page);
        // todo:: do I set special_block->nextblockno of the last block correctly?

        for(offset = FirstOffsetNumber; offset <= max_offset; offset = OffsetNumberNext(offset)) {
            nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
            if(wal_retriever_block_numbers[ nodepage->id ] != 0) {
                elog(ERROR,
                     "ldb_wal_retriever_area_init: duplicate id %d at offset %d. prev. blockno: %d, new blockno: %d",
                     nodepage->id,
                     offset,
                     wal_retriever_block_numbers[ nodepage->id ],
                     blockno);
            }
            wal_retriever_block_numbers[ nodepage->id ] = blockno;
        }
        special_block = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
        blockno = special_block->nextblockno;
        UnlockReleaseBuffer(buf);
    }
}
