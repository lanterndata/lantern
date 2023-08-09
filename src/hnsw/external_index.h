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

// This is enough store up to 2^32-1 vectors
// N.B. other factors make this a hard upper limit (e.g. num_vectors is uint32)
// So one cannot increase this number and expect to handle larger indexes
#define HNSW_MAX_BLOCKMAP_GROUPS 32

typedef struct HnswIndexHeaderPage
{
    uint32 magicNumber;
    uint32 version;
    uint32 vector_dim;
    uint32 num_vectors;
    uint32 last_data_block;
    uint32 num_blocks;
    char   usearch_header[ 64 ];

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
void CreateHeaderPage(Relation    index,
                      char       *usearchHeader64,
                      ForkNumber  forkNum,
                      uint32      vector_dim,
                      uint32      num_vectors,
                      BlockNumber last_data_block,
                      uint32      num_blocks,
                      bool        update);

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

BlockNumber getBlockMapPageBlockNumber(HnswIndexHeaderPage *hdr, int id);

#endif  // LDB_HNSW_EXTERNAL_INDEX_H