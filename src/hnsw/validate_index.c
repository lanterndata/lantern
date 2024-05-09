#include <postgres.h>

#include "hnsw/validate_index.h"

#include <access/heapam.h> /* relation_open */
#include <catalog/index.h> /* IndexGetRelation */
#include <inttypes.h>      /* PRIu32 */
#include <math.h>
#include <stdint.h>         /* UINT32_MAX */
#include <string.h>         /* bzero */
#include <utils/memutils.h> /* AllocSetContextCreate */

#include "hnsw.h"
#include "hnsw/external_index.h" /* HnswIndexHeaderPage */
#include "hnsw/options.h"        /* ldb_HnswGetM */
#include "hnsw/utils.h"          /* ldb_invariant */
#include "usearch.h"

/* vi infix here is for Validate Index */

enum ldb_vi_block_type
{
    LDB_VI_BLOCK_UNKNOWN,
    LDB_VI_BLOCK_HEADER,
    LDB_VI_BLOCK_BLOCKMAP,
    LDB_VI_BLOCK_NODES,
    LDB_VI_BLOCK_CODEBOOK,
    LDB_VI_BLOCK_NR,
};

/* represents PostgreSQL block in the index */
struct ldb_vi_block
{
    enum ldb_vi_block_type vp_type;
    uint32_t               vp_nodes_nr;
};

static_assert(sizeof(ldb_lantern_slot_union_t) == 6, "index validation assumes neighbor id is 6 bytes");
static_assert(sizeof(ldb_lantern_slot_union_t) == LANTERN_SLOT_SIZE, "index validation assumes neighbor id is 6 bytes");

/*
 * Represents a stored usearch node.
 * Assumes that usearch node has label, dim (size in bytes of the vector
 * at the end) and neighbors on different levels.
 *
 * Please adjust ldb_vi_read_node_carefully() in case if on-storage format changes.
 */
struct ldb_vi_node
{
    BlockNumber     vn_block;  /* in the index */
    OffsetNumber    vn_offset; /* within vn_block */
    uint32          vn_id;     /* HnswIndexTuple.id */
    usearch_label_t vn_label;
    uint32          vn_level;         /* HnswIndexTuple.level, usearch index_gt::level_t */
    uint32         *vn_neighbors_nr;  /* number of neighbors for each level */
    uint32        **vn_neighbors_old; /* array of arrays of *4byte* neighbor IDs for each level */

    ldb_lantern_slot_union_t **vn_neighbors; /* array of arrays of neighbors for each level */
};

/*
 * TODO add const to parameters wherever needed
 * TODO validate groups after max group no
 * TODO export entire index into json (for Python)
 * TODO test for non-default M
 * TODO add execution times for each stage
 * TODO check that the vectors are the same as in the table relation
 */

static void ldb_vi_analyze_blockmap(HnswBlockmapPage    *blockmap,
                                    struct ldb_vi_block *vi_blocks,
                                    BlockNumber          blocks_nr,
                                    struct ldb_vi_node  *vi_nodes,
                                    uint32               nodes_nr)
{
    for(uint32 node_id_in_blockmap = 0; node_id_in_blockmap < HNSW_BLOCKMAP_BLOCKS_PER_PAGE; ++node_id_in_blockmap) {
        uint32      node_id = blockmap->first_id + node_id_in_blockmap;
        BlockNumber blockno = blockmap->blocknos[ node_id_in_blockmap ];
        if(node_id < nodes_nr) {
            if(blockno == 0) {
                elog(ERROR,
                     "blockmap.blocknos[%" PRIu32
                     "] == 0 for "
                     "node_id=%" PRIu32 " nodes_nr=%" PRIu32,
                     node_id_in_blockmap,
                     node_id,
                     nodes_nr);
            }
            if(blockno >= blocks_nr) {
                elog(ERROR,
                     "blockmap.blocknos[%" PRIu32 "]=%" PRIu32 " >= blocks_nr=%" PRIu32
                     " for "
                     "node_id=%" PRIu32 " nodes_nr=%" PRIu32,
                     node_id_in_blockmap,
                     blockno,
                     blocks_nr,
                     node_id,
                     nodes_nr);
            }
            if(vi_blocks[ blockno ].vp_type == LDB_VI_BLOCK_UNKNOWN) vi_blocks[ blockno ].vp_type = LDB_VI_BLOCK_NODES;
            if(vi_blocks[ blockno ].vp_type != LDB_VI_BLOCK_NODES) {
                elog(ERROR,
                     "vi_blocks[%" PRIu32
                     "].vp_type=%d != %d for "
                     "blocks_nr=%" PRIu32 " node_id_in_blockmap=%" PRIu32 " node_id=%" PRIu32 " nodes_nr=%" PRIu32,
                     blockno,
                     vi_blocks[ blockno ].vp_type,
                     LDB_VI_BLOCK_NODES,
                     blocks_nr,
                     node_id_in_blockmap,
                     node_id,
                     nodes_nr);
            }
            vi_nodes[ node_id ].vn_block = blockno;
        } else if(blockno != 0) {
            elog(ERROR,
                 "blockmap.blocknos[%" PRIu32 "]=%" PRIu32
                 " != 0 for "
                 "node_id=%" PRIu32 " nodes_nr=%" PRIu32,
                 node_id_in_blockmap,
                 blockno,
                 node_id,
                 nodes_nr);
        }
    }
}
static void ldb_vi_read_pq_codebook(Relation             index,
                                    HnswIndexHeaderPage *index_header,

                                    struct ldb_vi_block *vi_blocks,
                                    BlockNumber          blocks_nr)
{
    LDB_UNUSED(index);
    if(blocks_nr < 1) return;
    int num_clusters = 256;
    for(int i = 0; i < ceil((float)((num_clusters)*index_header->vector_dim * sizeof(float)) / BLCKSZ); i++) {
        vi_blocks[ i + 1 ].vp_type = LDB_VI_BLOCK_CODEBOOK;
    }
}

static void ldb_vi_read_blockmaps(Relation             index,
                                  HnswIndexHeaderPage *index_header,
                                  struct ldb_vi_block *vi_blocks,
                                  BlockNumber          blocks_nr,
                                  struct ldb_vi_node  *vi_nodes,
                                  uint32               nodes_nr)
{
    /* TODO the outer loop math is mostly copy-pasted from StoreExternalIndex() */
    uint32 blockmap_groupno = 0;
    uint32 group_node_first_index = 0;
    uint32 nodes_remaining = nodes_nr;
    uint32 batch_size = HNSW_BLOCKMAP_BLOCKS_PER_PAGE;
    bool   last_group_node_is_used = true;

    if(blocks_nr == 0) return;
    vi_blocks[ 0 ].vp_type = LDB_VI_BLOCK_HEADER;
    while(nodes_remaining != 0 || (last_group_node_is_used && blockmap_groupno < index_header->blockmap_groups_nr)) {
        BlockNumber number_of_blockmaps_in_group = NumberOfBlockMapsInGroup(blockmap_groupno);

        if(blockmap_groupno >= index_header->blockmap_groups_nr) {
            elog(ERROR,
                 "blockmap_groupno=%" PRIu32 " >= index_header->blockmap_groups_nr=%" PRIu32,
                 blockmap_groupno,
                 index_header->blockmap_groups_nr);
        }
        if(index_header->blockmap_groups[ blockmap_groupno ].blockmaps_initialized != number_of_blockmaps_in_group) {
            elog(ERROR,
                 "HnswBlockMapGroupDesc.blockmaps_initialized=%" PRIu32 " != NumberOfBlockMapsInGroup()=%" PRIu32
                 " for blockmap_groupno=%" PRIu32,
                 index_header->blockmap_groups[ blockmap_groupno ].blockmaps_initialized,
                 number_of_blockmaps_in_group,
                 blockmap_groupno);
        }
        /* TODO see the loop in CreateBlockMapGroup() */
        BlockNumber group_start = index_header->blockmap_groups[ blockmap_groupno ].first_block;
        for(unsigned blockmap_id = 0; blockmap_id < number_of_blockmaps_in_group; ++blockmap_id) {
            BlockNumber blockmap_block = group_start + blockmap_id;
            BlockNumber expected_special_nextblockno;

            if(blockmap_block >= blocks_nr) {
                elog(ERROR,
                     "blockmap_block=%" PRIu32 " >= blocks_nr=%" PRIu32 " (blockmap_groupno=%d blockmap_id=%d)",
                     blockmap_block,
                     blocks_nr,
                     blockmap_groupno,
                     blockmap_id);
            }
            if(vi_blocks[ blockmap_block ].vp_type != LDB_VI_BLOCK_UNKNOWN) {
                elog(ERROR,
                     "vi_blocks[%" PRIu32 "].vp_type=%d (should be %d)",
                     blockmap_block,
                     vi_blocks[ blockmap_block ].vp_type,
                     LDB_VI_BLOCK_UNKNOWN);
            }
            vi_blocks[ blockmap_block ].vp_type = LDB_VI_BLOCK_BLOCKMAP;
            Buffer buf = ReadBuffer(index, blockmap_block);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            Page page = BufferGetPage(buf);

            /* see StoreExternalIndexBlockMapGroup() */
            if(PageGetMaxOffsetNumber(page) < FirstOffsetNumber) {
                elog(ERROR,
                     "blockmap_block=%" PRIu32
                     " for blockmap_groupno=%d blockmap_id=%d "
                     "doesn't have HnswBlockmapPage inside",
                     blockmap_groupno,
                     blockmap_id,
                     blockmap_block);
            }
            HnswBlockmapPage *blockmap = (HnswBlockmapPage *)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
            if(blockmap->first_id != group_node_first_index + blockmap_id * HNSW_BLOCKMAP_BLOCKS_PER_PAGE) {
                elog(ERROR,
                     "blockmap->first_id=%" PRIu32
                     " != "
                     "group_node_first_index=%d + blockmap_id=%u * HNSW_BLOCKMAP_BLOCKS_PER_PAGE=%d for "
                     "blockmap_groupno=%" PRIu32,
                     blockmap->first_id,
                     group_node_first_index,
                     blockmap_id,
                     HNSW_BLOCKMAP_BLOCKS_PER_PAGE,
                     blockmap_groupno);
            }
            HnswIndexPageSpecialBlock *special = (HnswIndexPageSpecialBlock *)PageGetSpecialPointer(page);
            if(special->firstId != blockmap->first_id) {
                elog(ERROR,
                     "special->firstId=%" PRIu32 " != blockmap->first_id=%" PRIu32
                     " for "
                     "blockmap_block=%" PRIu32 " blockmap_groupno=%d blockmap_id=%d",
                     special->firstId,
                     blockmap->first_id,
                     blockmap_block,
                     blockmap_groupno,
                     blockmap_id);
            }
            if(special->lastId != special->firstId + HNSW_BLOCKMAP_BLOCKS_PER_PAGE - 1) {
                elog(ERROR,
                     "special->lastId=%" PRIu32 " != (special->first_id=%" PRIu32
                     " + HNSW_BLOCKMAP_BLOCKS_PER_PAGE=%d - 1) for "
                     "blockmap_block=%" PRIu32 " blockmap_groupno=%d blockmap_id=%d",
                     special->lastId,
                     special->firstId,
                     HNSW_BLOCKMAP_BLOCKS_PER_PAGE,
                     blockmap_block,
                     blockmap_groupno,
                     blockmap_id);
            }
            /* TODO confirm this */
            /*
            expected_special_nextblockno = blockmap_id == number_of_blockmaps_in_group - 1 ?
                                           InvalidBlockNumber : blockmap_block + 1;
            */
            expected_special_nextblockno = blockmap_block + 1;
            if(special->nextblockno != expected_special_nextblockno) {
                elog(ERROR,
                     "special->nextblockno=%" PRIu32 " != expected_special_nextblockno=%" PRIu32
                     " for "
                     "blockmap_block=%" PRIu32 " blockmap_groupno=%d blockmap_id=%d",
                     special->nextblockno,
                     expected_special_nextblockno,
                     blockmap_block,
                     blockmap_groupno,
                     blockmap_id);
            }
            ldb_vi_analyze_blockmap(blockmap, vi_blocks, blocks_nr, vi_nodes, nodes_nr);

            UnlockReleaseBuffer(buf);
        }
        /*
         * This is for the case when the last blockmap group is initialized,
         * but PostgreSQL process crashed before something was added to it.
         */
        last_group_node_is_used = batch_size == nodes_remaining;
        nodes_remaining -= Min(batch_size, nodes_remaining);
        group_node_first_index += batch_size;
        batch_size = batch_size * 2;
        blockmap_groupno++;
    }
}

/* Read a part of the node. Also advance tape_pos by chunk_size. */
static void ldb_vi_read_node_chunk(const struct ldb_vi_node *vi_node,
                                   void                     *chunk,
                                   size_t                    chunk_size,
                                   const char               *chunk_name,
                                   void                     *tape,
                                   unsigned                 *tape_pos,
                                   unsigned                  tape_size)
{
    if(*tape_pos + chunk_size > tape_size) {
        elog(ERROR,
             "Error reading %s: tape_pos=%u + _chunk_size=%zu > tape_size=%u for "
             "block=%" PRIu32 " offset=%" PRIu16 " node_id=%" PRIu32,
             chunk_name,
             *tape_pos,
             chunk_size,
             tape_size,
             vi_node->vn_block,
             vi_node->vn_offset,
             vi_node->vn_id);
    }
    memcpy(chunk, (char *)tape + *tape_pos, chunk_size);
    *tape_pos += chunk_size;
}

#define LDB_VI_READ_NODE_CHUNK(_vi_node, _chunk, _tape, _tape_pos, _tape_size) \
    ldb_vi_read_node_chunk((_vi_node), &(_chunk), sizeof(_chunk), #_chunk, (_tape), (_tape_pos), (_tape_size))

/* See "Load nodes one by one" loop in usearch index_gt::load() */
static void ldb_vi_read_node_carefully(void               *node_tape,
                                       unsigned            node_tape_size,
                                       uint32              vector_size_bytes,
                                       const uint32        M,
                                       const uint32        index_storage_version,
                                       struct ldb_vi_node *vi_node,
                                       uint32              nodes_nr)
{
    unsigned tape_pos = 0;
    uint16   level_on_tape;
    uint32   neighbors_nr;
    uint32   neighbors_max;
    uint32  *neighbors_old;
    uint32   unused_neighbor_slot_old;

    ldb_lantern_slot_union_t *neighbors;
    ldb_lantern_slot_union_t  unused_neighbor_slot;

    LDB_VI_READ_NODE_CHUNK(vi_node, vi_node->vn_label, node_tape, &tape_pos, node_tape_size);
    LDB_VI_READ_NODE_CHUNK(vi_node, level_on_tape, node_tape, &tape_pos, node_tape_size);

    if(level_on_tape != vi_node->vn_level) {
        elog(ERROR,
             "level_on_tape=%" PRIu32 " != vi_node->vn_level=%" PRIu32
             " for "
             "node_id=%" PRIu32 " block=%" PRIu32 " offset=%" PRIu16,
             level_on_tape,
             vi_node->vn_level,
             vi_node->vn_id,
             vi_node->vn_block,
             vi_node->vn_offset);
    }
    /*
     * Now read lists of neighbors for each level.
     * See the comment for usearch neighbors_ref_t for the description of neighbors for one level.
     * See usearch precompute_ for the max numbers of neighbors for each level.
     * See usearch neighbors_ for the layour of neighbors for different levels on the tape.
     *
     * connectivity is M
     * connectivity_max_base is M * base_level_multiple()
     *
     * base_level_multiple() in usearch is 2.
     */
    vi_node->vn_neighbors_nr = palloc_array(typeof(*(vi_node->vn_neighbors_nr)), vi_node->vn_level + 1);
    vi_node->vn_neighbors = palloc_array(typeof(*(vi_node->vn_neighbors)), vi_node->vn_level + 1);
    vi_node->vn_neighbors_old = palloc_array(typeof(*(vi_node->vn_neighbors_old)), vi_node->vn_level + 1);
    for(uint32 level = 0; level <= vi_node->vn_level; ++level) {
        neighbors_max = level == 0 ? M * 2 : M;
        LDB_VI_READ_NODE_CHUNK(vi_node, neighbors_nr, node_tape, &tape_pos, node_tape_size);

        if(neighbors_nr > neighbors_max) {
            elog(ERROR,
                 "neighbors_nr=%" PRIu32 " > neighbors_max=%" PRIu32
                 " for "
                 "level=%" PRIu32
                 " tape_pos=%u node_tape_size=%u "
                 "node_id=%" PRIu32 " block=%" PRIu32 " offset=%" PRIu16,
                 neighbors_nr,
                 neighbors_max,
                 level,
                 tape_pos,
                 node_tape_size,
                 vi_node->vn_id,
                 vi_node->vn_block,
                 vi_node->vn_offset);
        }
        neighbors_old = palloc_array(typeof(*neighbors_old), neighbors_nr);
        neighbors = palloc_array(typeof(*neighbors), neighbors_nr);
        for(uint32 i = 0; i < neighbors_nr; ++i) {
            bool is_error = true;
            if(index_storage_version == LDB_WAL_VERSION_NUMBER) {
                LDB_VI_READ_NODE_CHUNK(vi_node, neighbors[ i ], node_tape, &tape_pos, node_tape_size);
                is_error = *(uint32 *)&neighbors[ i ] >= nodes_nr;
            } else if(index_storage_version == LDB_WAL_VERSION_NUMBER_0_2_7) {
                LDB_VI_READ_NODE_CHUNK(vi_node, neighbors_old[ i ], node_tape, &tape_pos, node_tape_size);
                is_error = neighbors_old[ i ] >= nodes_nr;
            } else {
                elog(ERROR, "Unknown index_storage_version=%x", index_storage_version);
            }

            if(is_error) {
                elog(ERROR,
                     "neighbors[%" PRIu32 "]=%" PRIu32 " >= nodes_nr=%" PRIu32
                     " for "
                     "neighbors_nr=%" PRIu32 " neighbors_max=%" PRIu32 " level=%" PRIu32
                     " tape_pos=%u node_tape_size=%u "
                     "node_id=%" PRIu32 " block=%" PRIu32 " offset=%" PRIu16,
                     i,
                     *(uint32 *)&neighbors[ i ],
                     nodes_nr,
                     neighbors_nr,
                     neighbors_max,
                     level,
                     tape_pos,
                     node_tape_size,
                     vi_node->vn_id,
                     vi_node->vn_block,
                     vi_node->vn_offset);
            }
        }

        if(index_storage_version == LDB_WAL_VERSION_NUMBER) {
            for(uint32 i = neighbors_nr; i < neighbors_max; ++i) {
                LDB_VI_READ_NODE_CHUNK(vi_node, unused_neighbor_slot, node_tape, &tape_pos, node_tape_size);
            }
            vi_node->vn_neighbors[ level ] = neighbors;
        } else if(index_storage_version == LDB_WAL_VERSION_NUMBER_0_2_7) {
            for(uint32 i = neighbors_nr; i < neighbors_max; ++i) {
                LDB_VI_READ_NODE_CHUNK(vi_node, unused_neighbor_slot_old, node_tape, &tape_pos, node_tape_size);
            }
            vi_node->vn_neighbors_old[ level ] = neighbors_old;
        } else {
            // condition has been chacked above already, sowe would have thrown by now
            assert(false);
        }
        vi_node->vn_neighbors_nr[ level ] = neighbors_nr;
    }
    /* the vector of floats is at the end */
    tape_pos += vector_size_bytes;
    if(tape_pos != node_tape_size) {
        elog(ERROR,
             "tape_pos=%u != node_tape_size=%u for "
             "node_id=%" PRIu32 " block=%" PRIu32 " offset=%" PRIu16,
             tape_pos,
             node_tape_size,
             vi_node->vn_id,
             vi_node->vn_block,
             vi_node->vn_offset);
    }
}

#undef LDB_VI_READ_NODE_CHUNK

static void ldb_vi_read_nodes(Relation                   index,
                              const HnswIndexHeaderPage *index_header,
                              struct ldb_vi_block       *vi_blocks,
                              BlockNumber                blocks_nr,
                              struct ldb_vi_node        *vi_nodes,
                              uint32                     nodes_nr)
{
    /* see usearch_init_options_t.quantization in PopulateUsearchOpts() */
    const size_t scalar_size = sizeof(float);

    for(uint32_t i = 0; i < nodes_nr; ++i) {
        if(vi_nodes[ i ].vn_block == InvalidBlockNumber)
            elog(ERROR, "vi_nodes[%" PRIu32 "].vn_block == InvalidBlockNumber", vi_nodes[ i ].vn_block);
    }
    for(BlockNumber block = 0; block < blocks_nr; ++block) {
        if(vi_blocks[ block ].vp_type != LDB_VI_BLOCK_NODES) continue;
        Buffer buf = ReadBuffer(index, block);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);

        if(PageGetMaxOffsetNumber(page) < FirstOffsetNumber)
            elog(ERROR, "block=%" PRIu32 " is supposed to have nodes but it doesn't have any", block);

        for(OffsetNumber offset = FirstOffsetNumber; offset <= PageGetMaxOffsetNumber(page);
            offset = OffsetNumberNext(offset)) {
            ItemId          item_id = PageGetItemId(page, offset);
            HnswIndexTuple *index_tuple = (HnswIndexTuple *)PageGetItem(page, item_id);
            unsigned        index_tuple_length = ItemIdGetLength(item_id);
            uint32          node_id;
            uint32          vector_size_bytes;

            if(index_header->pq) {
                vector_size_bytes = index_header->num_subvectors * 1;
            } else {
                vector_size_bytes = index_header->vector_dim * scalar_size;
            }

            if(sizeof(*index_tuple) > index_tuple_length) {
                elog(ERROR,
                     "sizeof(*index_tuple)=%zu > index_tuple_length=%u for "
                     "block=%" PRIu32 " offset=%" PRIu16,
                     sizeof(*index_tuple),
                     index_tuple_length,
                     block,
                     offset);
            }
            node_id = index_tuple->seqid;
            if(node_id >= nodes_nr) {
                elog(ERROR,
                     "node_id=%" PRIu32 " >= nodes_nr=%" PRIu32
                     " for "
                     "block=%" PRIu32 " offset=%" PRIu16,
                     node_id,
                     nodes_nr,
                     block,
                     offset);
            }
            if(vi_nodes[ node_id ].vn_block != block) {
                elog(ERROR,
                     "vi_nodes[%" PRIu32 "].vn_block=%" PRIu32 " != block=%" PRIu32
                     " for "
                     "offset=%" PRIu16,
                     node_id,
                     vi_nodes[ node_id ].vn_block,
                     block,
                     offset);
            }
            if(vi_nodes[ node_id ].vn_offset != InvalidOffsetNumber) {
                elog(ERROR,
                     "vi_nodes[%" PRIu32 "].vn_offset=%" PRIu32 " != InvalidOffsetNumber=%" PRIu32
                     " for "
                     "block=%" PRIu32,
                     node_id,
                     vi_nodes[ node_id ].vn_offset,
                     InvalidOffsetNumber,
                     block);
            }
            if(sizeof(*index_tuple) + index_tuple->size != index_tuple_length) {
                elog(ERROR,
                     "sizeof(*index_tuple)=%zu + index_tuple->size=%" PRIu32
                     " != index_tuple_length=%u for "
                     "node_id=%" PRIu32 " nodes_nr=%" PRIu32 " block=%" PRIu32 " offset=%" PRIu16,
                     sizeof(*index_tuple),
                     index_tuple->size,
                     index_tuple_length,
                     node_id,
                     nodes_nr,
                     block,
                     offset);
            }
            vi_nodes[ node_id ].vn_offset = offset;
            vi_nodes[ node_id ].vn_id = node_id;
            vi_nodes[ node_id ].vn_level = index_tuple->level;
            ldb_vi_read_node_carefully(&index_tuple->node,
                                       index_tuple->size,
                                       vector_size_bytes,
                                       index_header->m,
                                       index_header->version,
                                       &vi_nodes[ node_id ],
                                       nodes_nr);
        }
        UnlockReleaseBuffer(buf);
    }
}

static void ldb_vi_print_statistics(struct ldb_vi_block *vi_blocks,
                                    BlockNumber          blocks_nr,
                                    struct ldb_vi_node  *vi_nodes,
                                    uint32               nodes_nr)
{
    BlockNumber last_block = InvalidBlockNumber;
    uint32      blocks_per_blocktype[ LDB_VI_BLOCK_NR ];
    uint32      min_nodes_per_block = UINT32_MAX;
    uint32      max_nodes_per_block = 0;
    uint32      max_level = 0;
    uint32     *nodes_per_level;
    uint64     *edges_per_level;
    uint32     *min_neighbors_per_level;
    uint32     *max_neighbors_per_level;

    bzero(&blocks_per_blocktype, sizeof(blocks_per_blocktype));
    for(BlockNumber block = 0; block < blocks_nr; ++block) ++blocks_per_blocktype[ vi_blocks[ block ].vp_type ];
    elog(INFO,
         "blocks for: header %" PRIu32 " blockmap %" PRIu32 " nodes %" PRIu32,
         blocks_per_blocktype[ LDB_VI_BLOCK_HEADER ],
         blocks_per_blocktype[ LDB_VI_BLOCK_BLOCKMAP ],
         blocks_per_blocktype[ LDB_VI_BLOCK_NODES ]);

    for(uint32 i = 0; i < nodes_nr; ++i) ++vi_blocks[ vi_nodes[ i ].vn_block ].vp_nodes_nr;
    /* because in the next loop the condition is "block > 0" */
    ldb_invariant(vi_blocks[ 0 ].vp_type == LDB_VI_BLOCK_HEADER, "block 0 should be the header");
    for(BlockNumber block = blocks_nr - 1; block > 0; --block) {
        if(vi_blocks[ block ].vp_type == LDB_VI_BLOCK_NODES) {
            last_block = block;
            break;
        }
    }
    for(BlockNumber block = 0; block < blocks_nr; ++block) {
        if(vi_blocks[ block ].vp_type == LDB_VI_BLOCK_NODES && block != last_block) {
            min_nodes_per_block = Min(min_nodes_per_block, vi_blocks[ block ].vp_nodes_nr);
            max_nodes_per_block = Max(max_nodes_per_block, vi_blocks[ block ].vp_nodes_nr);
        }
    }
    if(blocks_per_blocktype[ LDB_VI_BLOCK_NODES ] == 0) {
        elog(INFO, "nodes per block: 0 blocks with nodes");
    } else if(blocks_per_blocktype[ LDB_VI_BLOCK_NODES ] == 1) {
        elog(INFO, "nodes per block: last block %" PRIu32, vi_blocks[ last_block ].vp_nodes_nr);
    } else {
        elog(INFO,
             "nodes per block: min (except last) %" PRIu32 " max (except last) %" PRIu32 " last %" PRIu32,
             min_nodes_per_block,
             max_nodes_per_block,
             vi_blocks[ last_block ].vp_nodes_nr);
    }

    for(uint32 i = 0; i < nodes_nr; ++i) max_level = Max(max_level, vi_nodes[ i ].vn_level);

    nodes_per_level = palloc0_array(typeof(*nodes_per_level), max_level + 1);
    edges_per_level = palloc0_array(typeof(*edges_per_level), max_level + 1);
    min_neighbors_per_level = palloc0_array(typeof(*min_neighbors_per_level), max_level + 1);
    max_neighbors_per_level = palloc0_array(typeof(*max_neighbors_per_level), max_level + 1);
    for(uint32 level = 0; level <= max_level; ++level) {
        min_neighbors_per_level[ level ] = UINT32_MAX;
        max_neighbors_per_level[ level ] = 0;
    }
    for(uint32 i = 0; i < nodes_nr; ++i) {
        struct ldb_vi_node *node = &vi_nodes[ i ];

        ++nodes_per_level[ node->vn_level ];
        for(uint32 level = 0; level <= node->vn_level; ++level) {
            edges_per_level[ level ] += node->vn_neighbors_nr[ level ];
            min_neighbors_per_level[ level ] = Min(min_neighbors_per_level[ level ], node->vn_neighbors_nr[ level ]);
            max_neighbors_per_level[ level ] = Max(max_neighbors_per_level[ level ], node->vn_neighbors_nr[ level ]);
            if(0) {
                /* useful for debugging */
                for(uint32 n = 0; n < node->vn_neighbors_nr[ level ]; ++n) {
                    elog(INFO,
                         "node %" PRIu32 " level %" PRIu32 " neighbor %" PRIu32 ": %" PRIu32,
                         i,
                         level,
                         n,
                         *(uint32 *)&node->vn_neighbors[ level ][ n ]);
                }
            }
        }
    }
    for(uint32 level = 0; level <= max_level; ++level) {
        if(min_neighbors_per_level[ level ] == UINT32_MAX) min_neighbors_per_level[ level ] = 0;
    }
    for(uint32 level = 0; level <= max_level; ++level) {
        elog(INFO,
             "level=%" PRIu32 ": nodes %" PRIu32
             " directed neighbor edges %lu "
             "min neighbors %" PRIu32 " max neighbors %" PRIu32,
             level,
             nodes_per_level[ level ],
             edges_per_level[ level ],
             min_neighbors_per_level[ level ],
             max_neighbors_per_level[ level ]);
    }
    pfree(max_neighbors_per_level);
    pfree(min_neighbors_per_level);
    pfree(edges_per_level);
    pfree(nodes_per_level);
}

void ldb_validate_index(Oid indrelid, bool print_info)
{
    Relation             index;
    BlockNumber          header_blockno = 0;
    Buffer               header_buf;
    Page                 header_page;
    HnswIndexHeaderPage *index_header;
    MemoryContext        memCtx;
    MemoryContext        saveCtx;
    BlockNumber          blocks_nr;
    uint32               nodes_nr;
    struct ldb_vi_block *vi_blocks;
    struct ldb_vi_node  *vi_nodes;

    /* the code here doesn't change the index, so AccessShareLock is enough */
    index = relation_open(indrelid, AccessShareLock);

    if(print_info) {
        elog(INFO, "validate_index() start for %s with Oid=%u", RelationGetRelationName(index), indrelid);
    } else {
        elog(INFO, "validate_index() start for %s", RelationGetRelationName(index));
    }
    memCtx = AllocSetContextCreate(CurrentMemoryContext, "hnsw validate_index context", ALLOCSET_DEFAULT_SIZES);
    saveCtx = MemoryContextSwitchTo(memCtx);

    header_buf = ReadBuffer(index, header_blockno);
    LockBuffer(header_buf, BUFFER_LOCK_EXCLUSIVE);
    header_page = BufferGetPage(header_buf);
    index_header = (HnswIndexHeaderPage *)PageGetContents(header_page);
    if(index_header->magicNumber != LDB_WAL_MAGIC_NUMBER) {
        elog(ERROR,
             "Invalid HnswIndexHeaderPage.magicNumber (page %" PRIu32 ", got %x, expected %x)",
             header_blockno,
             index_header->magicNumber,
             LDB_WAL_MAGIC_NUMBER);
    }
    if(index_header->m != (uint32)ldb_HnswGetM(index)) {
        elog(ERROR, "index_header->m=%" PRIu32 " != ldb_HnswGetM(index)=%d", index_header->m, ldb_HnswGetM(index));
    }
    if(print_info) {
        elog(INFO,
             "index_header = HnswIndexHeaderPage("
             "version=%" PRIu32 " vector_dim=%" PRIu32 " m=%" PRIu32 " ef_construction=%" PRIu32 " ef=%" PRIu32
             " pq=%d metric_kind=%d num_vectors=%" PRIu32 " last_data_block=%" PRIu32 " blockmap_groups_nr=%" PRIu32
             ")",
             index_header->version,
             index_header->vector_dim,
             index_header->m,
             index_header->ef_construction,
             index_header->ef,
             index_header->metric_kind,
             index_header->pq,
             index_header->num_vectors,
             index_header->last_data_block,
             index_header->blockmap_groups_nr);
        for(uint32 i = 0; i < index_header->blockmap_groups_nr; ++i) {
            elog(INFO,
                 "blockmap_groups[%" PRIu32 "]=(first_block=%" PRIu32 ", blockmaps_initialized=%" PRIu32 "),",
                 i,
                 index_header->blockmap_groups[ i ].first_block,
                 index_header->blockmap_groups[ i ].blockmaps_initialized);
        }
    }

    blocks_nr = RelationGetNumberOfBlocksInFork(index, MAIN_FORKNUM);
    nodes_nr = index_header->num_vectors;
    if(print_info) {
        elog(INFO, "blocks_nr=%" PRIu32 " nodes_nr=%" PRIu32, blocks_nr, nodes_nr);
    }
    /* TODO check nodes_nr against index_header->blockmap_groups_nr */

    vi_blocks = palloc0_array(typeof(*vi_blocks), blocks_nr);
    vi_nodes = palloc0_array(typeof(*vi_nodes), nodes_nr);
    for(uint32 i = 0; i < nodes_nr; ++i) {
        vi_nodes[ i ].vn_block = InvalidBlockNumber;
        vi_nodes[ i ].vn_offset = InvalidOffsetNumber;
    }

    if(index_header->pq) {
        ldb_vi_read_pq_codebook(index, index_header, vi_blocks, blocks_nr);
    }
    ldb_vi_read_blockmaps(index, index_header, vi_blocks, blocks_nr, vi_nodes, nodes_nr);
    for(BlockNumber block = 0; block < blocks_nr; ++block) {
        if(vi_blocks[ block ].vp_type == LDB_VI_BLOCK_UNKNOWN) {
            elog(ERROR, "vi_blocks[%" PRIu32 "].vp_type == LDB_VI_BLOCK_UNKNOWN (but it should be known now)", block);
        }
    }
    ldb_vi_read_nodes(index, index_header, vi_blocks, blocks_nr, vi_nodes, nodes_nr);
    if(print_info) ldb_vi_print_statistics(vi_blocks, blocks_nr, vi_nodes, nodes_nr);

    pfree(vi_nodes);
    pfree(vi_blocks);

    UnlockReleaseBuffer(header_buf);
    MemoryContextSwitchTo(saveCtx);
    MemoryContextDelete(memCtx);
    elog(INFO, "validate_index() done, no issues found.");
    relation_close(index, AccessShareLock);
}
