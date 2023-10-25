#ifndef LDB_HNSW_EXTRA_DIRTIED_H
#define LDB_HNSW_EXTRA_DIRTIED_H

#include <access/generic_xlog.h>
#include <common/relpath.h>
#include <storage/block.h>
#include <storage/buf.h>
#include <storage/bufmgr.h>
#include <storage/bufpage.h>

// #define LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS 5
// this now includes buffers dirtied by the usearch
// hnsw updates (= buffers of new neighbors of the inserted vector)
#define LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS 100
#define LDB_HNSW_EXTRA_DIRTIED_MAX_WAL_RECORDS \
    (((LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS) + (MAX_GENERIC_XLOG_PAGES)-1) / (MAX_GENERIC_XLOG_PAGES))

typedef struct
{
    BlockNumber*       extra_dirtied_blockno;
    Buffer*            extra_dirtied_buf;
    Page*              extra_dirtied_page;
    int                extra_dirtied_size;
    GenericXLogState** extra_dirtied_state;
    int                extra_dirtied_state_size;
    int                extra_dirtied_pages_in_the_last_state;
} ExtraDirtiedBufs;

ExtraDirtiedBufs* extra_dirtied_new();
// Page extra_dirtied_add(ExtraDirtiedBufs *ed, BlockNumber blockno);
void extra_dirtied_add(ExtraDirtiedBufs* ed, BlockNumber blockno, Buffer buf, Page page);
void extra_dirtied_add_wal_read_buffer(
    ExtraDirtiedBufs* ed, Relation index, ForkNumber forkNum, BlockNumber blockno, Buffer* buf, Page* page);
Page extra_dirtied_get(ExtraDirtiedBufs* ed, BlockNumber blockno, Buffer* out_buf);
void extra_dirtied_release_all(ExtraDirtiedBufs* ed);
void extra_dirtied_free(ExtraDirtiedBufs* ed);

#endif  // LDB_HNSW_EXTRA_DIRTIED_H
