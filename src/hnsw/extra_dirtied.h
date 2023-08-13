#ifndef LDB_HNSW_EXTRA_DIRTIED_H
#define LDB_HNSW_EXTRA_DIRTIED_H

#include <common/relpath.h>
#include <storage/block.h>
#include <storage/buf.h>
#include <storage/bufmgr.h>
#include <storage/bufpage.h>

// #define LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS 5
// this now includes buffers dirtied by the usearch
// hnsw updates (= buffers of new neighbors of the inserted vector)
#define LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS 100

typedef struct
{
    Relation     index_rel;
    BlockNumber* EXTRA_DIRTIED_BLOCKNO;
    Buffer*      EXTRA_DIRTIED_BUF;
    Page*        EXTRA_DIRTIED_PAGE;
    int          EXTRA_DIRTIED_SIZE;
} ExtraDirtiedBufs;

ExtraDirtiedBufs* extra_dirtied_new(Relation index_rel);
// Page extra_dirtied_add(ExtraDirtiedBufs *ed, BlockNumber blockno);
void extra_dirtied_add(ExtraDirtiedBufs* ed, BlockNumber blockno, Buffer buf, Page page);
Page extra_dirtied_get(ExtraDirtiedBufs* ed, BlockNumber blockno, Buffer* out_buf);
void extra_dirtied_release_all(ExtraDirtiedBufs* ed);
void extra_dirtied_free(ExtraDirtiedBufs* ed);

#endif  // LDB_HNSW_EXTRA_DIRTIED_H