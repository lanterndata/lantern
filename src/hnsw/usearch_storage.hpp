#ifndef HNSW_USEARCH_STORAGE_H
#define HNSW_USEARCH_STORAGE_H
#ifdef __cplusplus
extern "C" {
#endif
#include "hnsw.h"
#include "usearch.h"

uint32_t UsearchNodeBytes(const metadata_t *metadata, int vector_bytes, int level);
void     usearch_init_node(
        metadata_t *meta, char *tape, usearch_key_t key, uint32_t level, uint64_t slot_id, void *vector, size_t vector_len);

char *extract_node(char             *data,
                   uint64_t          progress,
                   int               dim,
                   const metadata_t *metadata,
                   /*->>out*/ int   *node_size,
                   int              *level);

usearch_label_t label_from_node(char *node);
unsigned long   level_from_node(char *node);
void            reset_node_label(char *node);

ldb_unaligned_slot_union_t *get_node_neighbors_mut(const metadata_t *meta,
                                                   char             *node,
                                                   uint32            level,
                                                   uint32           *neighbors_count);

#ifdef __cplusplus
}
#endif

#endif  // HNSW_USEARCH_STORAGE_H
