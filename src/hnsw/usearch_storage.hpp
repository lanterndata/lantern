// #include <postgres.h>
#include "usearch.h"

#ifdef __cplusplus
extern "C" {
#endif

uint32_t UsearchNodeBytes(usearch_metadata_t *metadata, int vector_bytes, int level);
char    *extract_node(char               *data,
                      uint64_t            progress,
                      int                 dim,
                      usearch_metadata_t *metadata,
                      /*->>output*/ int  *node_size,
                      int                *level);

#ifdef __cplusplus
}
#endif
