#ifndef LDB_HNSW_EXTERNAL_INDEX_H
#define LDB_HNSW_EXTERNAL_INDEX_H

#include "postgres.h"

#include <common/relpath.h>  // ForkNumber
#include <storage/bufmgr.h>  // Buffer
#include <utils/relcache.h>  // Relation

#include "usearch.h"

#define LDB_WAL_MAGIC_NUMBER   0xa47e20db
#define LDB_WAL_VERSION_NUMBER 0x00000001

// this should be as large as possible to cache more stuff
// while still fitting on the first page
#define NODE_STORAGE_NUM_COARSE 10
typedef struct HnswIndexHeaderPage
{
    uint32 magicNumber;
    uint32 version;
    uint32 vector_dim;
    uint32 num_vectors;
    uint32 blockno_index_start;
    uint32 num_blocks;
    char   usearch_header[ 64 ];
    uint32 coarse_ids[ NODE_STORAGE_NUM_COARSE ];
    uint32 coarse_block[ NODE_STORAGE_NUM_COARSE ];
} HnswIndexHeaderPage;

typedef struct HnswIndexPageSpecialBlock
{
    uint32 firstId;
    uint32 lastId;
    uint32 nextblockno;

} HnswIndexPageSpecialBlock;

typedef struct HnswIndexPage
{
    uint32 id;
    uint32 level;
    uint32 size;
    char   node[ FLEXIBLE_ARRAY_MEMBER ];
} HnswIndexPage;

#define HNSW_BLOCKMAP_BLOCKS_PER_PAGE 2000

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
// this area is used to return pointers back to usearch

void ldb_wal_retriever_area_init(int size);

// can be used after each usearch_search to tell the retriever that the pointers given out
// will no longer be used
void ldb_wal_retriever_area_reset();
void ldb_wal_retriever_area_free();

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

void *ldb_wal_index_node_retriever(int id);

#endif // LDB_HNSW_EXTERNAL_INDEX_H