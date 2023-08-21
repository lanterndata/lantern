#ifndef LDB_HNSW_EXTERNAL_INDEX_H
#define LDB_HNSW_EXTERNAL_INDEX_H

#include <postgres.h>

#include <access/generic_xlog.h>
#include <common/relpath.h>  // ForkNumber
#include <storage/bufmgr.h>  // Buffer
#include <utils/relcache.h>  // Relation

#include "cache.h"
#include "extra_dirtied.h"
#include "usearch.h"

#define LDB_WAL_MAGIC_NUMBER   0xa47e20db
#define LDB_WAL_VERSION_NUMBER 0x00000001

// used for code clarity when modifying WAL entries
#define LDB_GENERIC_XLOG_DELTA_IMAGE 0

// This is enough store up to 2^32-1 vectors
// N.B. other factors make this a hard upper limit (e.g. num_vectors is uint32)
// So one cannot increase this number and expect to handle larger indexes
#define HNSW_MAX_BLOCKMAP_GROUPS 32

// each blockmap entry is a 4 byte ID. 2000 fits in BLCKSZ=8192 page.
// Note: If you make complex changes at the code of the database, you can change this number to a smaller value
// to be able to test more of the algorithm corner cases with a small table dataset
#define HNSW_BLOCKMAP_BLOCKS_PER_PAGE 2000

typedef struct HnswIndexHeaderPage
{
    uint32      magicNumber;
    uint32      version;
    uint32      vector_dim;
    uint32      num_vectors;
    BlockNumber last_data_block;
    char        usearch_header[ 64 ];

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

typedef struct
{
    // for debugging, each block will store the ground truth index for the first
    // block id it is holding
    // this is calculated externally, however
    uint32      first_id;
    BlockNumber blocknos[ HNSW_BLOCKMAP_BLOCKS_PER_PAGE ];
} HnswBlockmapPage;

typedef struct
{
    Cache block_numbers_cache;

    Relation index_rel;

    // used for scans
    uint32 blockmap_page_group_index_cache[ HNSW_MAX_BLOCKMAP_GROUPS ];  // todo::
    // used for inserts
    HnswIndexHeaderPage *header_page_under_wal;

    ExtraDirtiedBufs *extra_dirted;

#if LANTERNDB_COPYNODES
    char *wal_retriever_area = NULL;
    int   wal_retriever_area_size = 0;
    int   wal_retriever_area_offset = 0;
#else

    Buffer *takenbuffers;
    int     takenbuffers_next;
#endif
} RetrieverCtx;

typedef struct
{
    usearch_index_t uidx;
    RetrieverCtx   *retriever_ctx;
} HnswInsertState;

int UsearchNodeBytes(usearch_metadata_t *metadata, int vector_bytes, int level);

void StoreExternalIndex(Relation        index,
                        usearch_index_t external_index,
                        ForkNumber      forkNum,
                        char           *data,
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
                                  HnswInsertState     *insertstate);

#endif  // LDB_HNSW_EXTERNAL_INDEX_H
