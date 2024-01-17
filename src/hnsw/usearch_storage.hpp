// #include <postgres.h>
#include "usearch.h"

#ifdef __cplusplus
extern "C" {
#endif

uint32_t UsearchNodeBytes(usearch_metadata_t *metadata, int vector_bytes, int level);
void     usearch_init_node(usearch_metadata_t *meta,
                           char               *tape,
                           unsigned long       key,
                           uint32_t            level,
                           uint32_t            slot_id,
                           void               *vector,
                           size_t              vector_len);

char *extract_node(char               *data,
                   uint64_t            progress,
                   int                 dim,
                   usearch_metadata_t *metadata,
                   /*->>output*/ int  *node_size,
                   int                *level);

#ifdef __cplusplus
}
#endif
