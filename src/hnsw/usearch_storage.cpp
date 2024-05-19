#include <usearch.h>
#include <usearch/index.hpp>
#include <usearch/lantern_storage.hpp>
extern "C" {
#include "hnsw.h"
#include "usearch_storage.hpp"
}

#include <cassert>

namespace
{
using namespace unum::usearch;
using node_t = unum::usearch::node_at<default_key_t, lantern_slot_t>;
}  // namespace

uint32_t UsearchNodeBytes(const metadata_t *metadata, int vector_bytes, int level)
{
    const int NODE_HEAD_BYTES = sizeof(usearch_label_t) + sizeof(unum::usearch::level_t) /*sizeof level*/;
    assert(sizeof(usearch_label_t) == 8);
    uint32_t node_bytes = 0;

    node_bytes += NODE_HEAD_BYTES + metadata->neighbors_base_bytes;
    node_bytes += metadata->neighbors_bytes * level;
    // assuming at most 2 ** 8 centroids (= 1 byte) per subvector
    assert(!metadata->init_options.pq || metadata->init_options.num_subvectors <= vector_bytes / sizeof(float));
    assert(!metadata->init_options.pq || metadata->init_options.num_subvectors > 0);
    node_bytes += metadata->init_options.pq ? metadata->init_options.num_subvectors : vector_bytes;
    return node_bytes;
}

void usearch_init_node(
    metadata_t *meta, char *tape, usearch_key_t key, uint32_t level, uint64_t slot_id, void *vector, size_t vector_len)
{
    int node_size = UsearchNodeBytes(meta, vector_len, level);
    std::memset(tape, 0, node_size);
    node_t node = node_t{tape};
    assert(level == uint16_t(level));
    node.level(level);
    node.key(key);
}

char *extract_node(char              *data,
                   uint64_t           progress,
                   int                dim,
                   const metadata_t  *metadata,
                   /*->>output*/ int *node_size,
                   int               *level)
{
    char  *tape = data + progress;
    node_t node = node_t{tape};

    *level = (int)node.level();
    const int VECTOR_BYTES = dim * sizeof(float);
    *node_size = UsearchNodeBytes(metadata, VECTOR_BYTES, *level);
    return tape;
}

usearch_label_t label_from_node(char *node) 
{
    node_t n = node_t{node};
    return n.key();
}

unsigned long level_from_node(char *node)
{
    node_t n = node_t{node};
    return n.level();
}

void reset_node_label(char *node)
{
    node_t n = node_t{node};
    n.key(INVALID_ELEMENT_LABEL);
}

ldb_unaligned_slot_union_t *get_node_neighbors_mut(const metadata_t *meta,
                                                   char             *node,
                                                   uint32            level,
                                                   uint32           *neighbors_count)
{
    const node_t n = node_t{node};

    precomputed_constants_t pre;
    pre.neighbors_bytes = meta->neighbors_bytes;
    pre.neighbors_base_bytes = meta->neighbors_base_bytes;
    pre.inverse_log_connectivity = meta->inverse_log_connectivity;

    node_t::neighbors_ref_t ns = n.neighbors_(pre, level);
    *neighbors_count = ns.size();
    static_assert(sizeof(ldb_unaligned_slot_union_t) == sizeof(node_t::slot_t));
    static_assert(sizeof(ldb_unaligned_slot_union_t) == sizeof(node_t::neighbors_ref_t::compressed_slot_t));
    return (ldb_unaligned_slot_union_t *)ns.misaligned_tape();
}
