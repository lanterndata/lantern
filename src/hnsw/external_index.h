#ifndef LDB_HNSW_EXTERNAL_INDEX_H
#define LDB_HNSW_EXTERNAL_INDEX_H

#include <postgres.h>

#include <access/generic_xlog.h>
#include <common/relpath.h>  // ForkNumber
#include <lib/ilist.h>       // Dlist
#include <storage/bufmgr.h>  // Buffer
#include <utils/relcache.h>  // Relation

#include "extra_dirtied.h"
#include "fa_cache.h"
#include "hnsw.h"
#include "htab_cache.h"
#include "options.h"
#include "usearch.h"

#define LDB_WAL_MAGIC_NUMBER   0xa47e60db
#define LDB_WAL_VERSION_NUMBER 0x00000003

// old version numbers. Suffix is the last version that used this format
// #define LDB_WAL_VERSION_NUMBER_0_2_7 0x00000002

// used for code clarity when modifying WAL entries
#define LDB_GENERIC_XLOG_DELTA_IMAGE 0

// usearch header size (80) + graph header size (40 + 16)
#define USEARCH_HEADER_SIZE 136

typedef struct HnswBlockMapGroupDesc
{
    BlockNumber first_block;
    uint32      blockmaps_initialized;
} HnswBlockMapGroupDesc;

typedef struct HnswIndexHeaderPage
{
    uint32                magicNumber;
    uint32                version;
    uint32                vector_dim;
    uint32                m;
    uint32                ef_construction;
    uint32                ef;
    usearch_metric_kind_t metric_kind;
    uint32                num_vectors;
    BlockNumber           last_data_block;
    char                  usearch_header[ USEARCH_HEADER_SIZE ];

    bool   pq;
    size_t num_centroids;
    size_t num_subvectors;

    usearch_scalar_kind_t quantization;
} HnswIndexHeaderPage;

// the added 40 byte graph header (currently unused)
struct index_serialized_header_t
{
    uint64 size;
    uint64 connectivity;
    uint64 connectivity_base;
    uint64 max_level;
    uint64 entry_slot;
};

typedef struct HnswIndexPageSpecialBlock
{
    uint32 firstId;
    uint32 lastId;
    uint32 nextblockno;

} HnswIndexPageSpecialBlock;

typedef struct HnswIndexTuple
{
    uint32 seqid;
    // stores size of the flexible array member
    uint32 size;
    char   node[ FLEXIBLE_ARRAY_MEMBER ];
} HnswIndexTuple;

typedef struct
{
    HTABCache block_numbers_cache;

    Relation index_rel;

    // used for inserts
    HnswIndexHeaderPage *header_page_under_wal;

    ExtraDirtiedBufs *extra_dirted;

    FullyAssociativeCache fa_cache;

    dlist_head takenbuffers;
} RetrieverCtx;

typedef struct
{
#if LANTERNDB_COPYNODES
    char *buf;
#else
    Buffer buf;
#endif
    dlist_node node;
} BufferNode;

typedef struct
{
    usearch_index_t uidx;
    RetrieverCtx   *retriever_ctx;
    HnswColumnType  columnType;
    float          *pq_codebook;
} HnswInsertState;

void StoreExternalEmptyIndex(
    Relation index, ForkNumber forkNum, char *data, int dimensions, usearch_init_options_t *opts);
void StoreExternalIndex(Relation                index,
                        const metadata_t       *external_index_metadata,
                        ForkNumber              forkNum,
                        char                   *data,
                        usearch_init_options_t *opts,
                        size_t                  num_added_vectors);

// add the fully constructed index tuple to the index via wal
// hdr is passed in so num_vectors, first_block_no, last_block_no can be updated
HnswIndexTuple *PrepareIndexTuple(Relation             index_rel,
                                  GenericXLogState    *state,
                                  HnswIndexHeaderPage *hdr,
                                  metadata_t          *metadata,
                                  uint32               new_tuple_id,
                                  uint32               new_tuple_level,

                                  ldb_unaligned_slot_union_t *slot,
                                  HnswInsertState            *insertstate);

bool isBlockMapBlock(const HnswBlockMapGroupDesc *blockmap_groups, const int blockmap_group_nr, BlockNumber blockno);

#endif  // LDB_HNSW_EXTERNAL_INDEX_H
