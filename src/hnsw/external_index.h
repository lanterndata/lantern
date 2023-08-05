#ifndef LDB_HNSW_EXTERNAL_INDEX_H
#define LDB_HNSW_EXTERNAL_INDEX_H

#include "postgres.h"

#include <access/generic_xlog.h>
#include <common/relpath.h>  // ForkNumber
#include <storage/bufmgr.h>  // Buffer
#include <utils/relcache.h>  // Relation

#include "insert.h"
#include "usearch.h"

#define LDB_WAL_MAGIC_NUMBER   0xa47e20db
#define LDB_WAL_VERSION_NUMBER 0x00000001

// used for code clarity when modifying WAL entries
#define LDB_GENERIC_XLOG_DELTA_IMAGE 0

// with so many blockmap groups we can store up to 2^32-1 vectors
#define HNSW_MAX_BLOCKMAP_GROUPS 32

// this should be as large as possible to cache more stuff
// while still fitting on the first page
#define NODE_STORAGE_NUM_COARSE 10
typedef struct HnswIndexHeaderPage
{
    uint32 magicNumber;
    uint32 version;
    uint32 vector_dim;
    uint32 num_vectors;
    // todo:: switch these to BlockNumber for documentation
    // first data block is needed because in case of creating an index on empty table it no longer
    // is headeblockno + 1
    uint32 last_data_block;
    uint32 blockno_index_start;
    //todo:: get rid of this
    uint32 num_blocks;
    char   usearch_header[ 64 ];
    uint32 coarse_ids[ NODE_STORAGE_NUM_COARSE ];
    uint32 coarse_block[ NODE_STORAGE_NUM_COARSE ];

    uint32 blockmap_page_groups;
    uint32 blockmap_page_group_index[ HNSW_MAX_BLOCKMAP_GROUPS ];
} HnswIndexHeaderPage;

typedef struct HnswIndexPageSpecialBlock
{
    uint32 firstId;
    uint32 lastId;
    uint32 nextblockno;

} HnswIndexPageSpecialBlock;

typedef struct HnswIndexTuple
{
    uint32 id;
    uint32 level;
    // stores size of the flexible array member
    uint32 size;
    char   node[ FLEXIBLE_ARRAY_MEMBER ];
} HnswIndexTuple;

#define HNSW_BLOCKMAP_BLOCKS_PER_PAGE 2000
// limit max indexed vectors to 40M to simplify blockmap pages into
// contiguous range
// This is wasteful (160MB) but for moderately sized tables (5M vecs = ~2.5 GB)
// it is not that bad
// Once other basic operations are supported, will change this to be a small number
// of exponentially increasing contiguous ranges so both the limit and the waste
// will go away without adding search overhead
#define HNSW_MAX_INDEXED_VECTORS 40000

typedef struct
{
    // for debugging, each block will store the ground truth index for the first
    // block id it is holding
    // this is calculated externally, however
    uint32      first_id;
    BlockNumber blocknos[ HNSW_BLOCKMAP_BLOCKS_PER_PAGE ];
} HnswBlockmapPage;

// todo:: get rid of these. maybe pass it to usearch_set_retriever and have it pass it back?
extern Relation            INDEX_RELATION_FOR_RETRIEVER;
extern HnswIndexHeaderPage HEADER_FOR_EXTERNAL_RETRIEVER;
extern Buffer             *EXTRA_DIRTIED;
extern Page               *EXTRA_DIRTIED_PAGE;
extern int                 EXTRA_DIRTIED_SIZE;
// this area is used to return pointers back to usearch

void ldb_wal_retriever_area_init(int size);

// can be used after each usearch_search to tell the retriever that the pointers given out
// will no longer be used
void ldb_wal_retriever_area_reset();
void ldb_wal_retriever_area_free();

int  UsearchNodeBytes(usearch_metadata_t *metadata, int vector_bytes, int level);
void CreateHeaderPage(Relation   index,
                      char      *usearchHeader64,
                      ForkNumber forkNum,
                      int        vector_dim,
                      int        num_vectors,
                      uint32     blockno_index_start,
                      uint32     num_blocks,
                      bool       update);

void StoreExternalIndex(Relation        index,
                        usearch_index_t external_index,
                        ForkNumber      forkNum,
                        char           *data,
                        size_t          external_index_size,
                        int             dimension,
                        size_t          num_added_vectors);

// add the fully constructed index tuple to the index via wal
// hdr is passed in so num_vectors, first_block_no, last_block_no can be updated
HnswIndexTuple *PrepareIndexTuple(Relation             index_rel,
                                  GenericXLogState    *state,
                                  HnswIndexHeaderPage *hdr,
                                  usearch_metadata_t  *metadata,
                                  uint32               new_tuple_id,
                                  int                  new_tuple_level,
                                  Buffer (*extra_dirtied)[ LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS ],
                                  Page (*extra_dirtied_page)[ LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS ],
                                  int *extra_dirtied_size);

void *ldb_wal_index_node_retriever(int id);
void *ldb_wal_index_node_retriever_mut(int id);

BlockNumber getBlockMapPageBlockNumber(HnswIndexHeaderPage* hdr, int id);

#endif  // LDB_HNSW_EXTERNAL_INDEX_H