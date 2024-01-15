
#include <usearch/index.hpp>
extern "C" {
#include "usearch_storage.hpp"
}

#include <cassert>

#include "usearch/index_dense.hpp"

uint32_t UsearchNodeBytes(usearch_metadata_t *metadata, int vector_bytes, int level)
{
    const int NODE_HEAD_BYTES = sizeof(usearch_label_t) + sizeof(unum::usearch::level_t) /*sizeof level*/;
    uint32_t  node_bytes = 0;
    node_bytes += NODE_HEAD_BYTES + metadata->neighbors_base_bytes;
    node_bytes += metadata->neighbors_bytes * level;
    node_bytes += vector_bytes;
    return node_bytes;
}

char *extract_node(char               *data,
                   uint64_t            progress,
                   int                 dim,
                   usearch_metadata_t *metadata,
                   /*->>output*/ int  *node_size,
                   int                *level)
{
    using namespace unum::usearch;
    using node_t = unum::usearch::node_at< default_key_t, default_slot_t >;
    char  *tape = data + progress;
    node_t node = node_t{tape};

    // int read_dim_bytes = -1;
    // memcpy(&read_dim_bytes, tape + sizeof(usearch_label_t), 4);  //+sizeof(label)

    *level = node.level();
    // memcpy(level, tape + sizeof(usearch_label_t) + 4, 4);        //+sizeof(label)+sizeof(dim)
    const int VECTOR_BYTES = dim * sizeof(float);
    // assert(VECTOR_BYTES == read_dim_bytes);
    *node_size = UsearchNodeBytes(metadata, VECTOR_BYTES, *level);
    return tape;
}
