#include <postgres.h>

#include "extra_dirtied.h"

#include <assert.h>

ExtraDirtiedBufs *extra_dirtied_new()
{
    ExtraDirtiedBufs *ed = palloc0(sizeof(ExtraDirtiedBufs));
    assert(ed != NULL);
    ed->extra_dirtied_blockno = palloc0(sizeof(BlockNumber) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->extra_dirtied_buf = palloc0(sizeof(Buffer) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->extra_dirtied_page = palloc0(sizeof(Page) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->extra_dirtied_size = 0;
    return ed;
}

void extra_dirtied_add(ExtraDirtiedBufs *ed, BlockNumber blockno, Buffer buf, Page page)
{
    assert(ed != NULL);
    assert(page != NULL);
    assert(BufferIsValid(buf));
    // todo:: check as invariant that the page is locked at and is under WAL
    // currently, it may not always be under WAL which should be fixed

    assert(ed->extra_dirtied_size + 1 < LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->extra_dirtied_page[ ed->extra_dirtied_size++ ] = page;
    ed->extra_dirtied_buf[ ed->extra_dirtied_size - 1 ] = buf;
    ed->extra_dirtied_blockno[ ed->extra_dirtied_size - 1 ] = blockno;
}

Page extra_dirtied_get(ExtraDirtiedBufs *ed, BlockNumber blockno, Buffer *buf)
{
    for(int i = 0; i < ed->extra_dirtied_size; i++) {
        if(ed->extra_dirtied_blockno[ i ] == blockno) {
            if(buf != NULL) {
                *buf = ed->extra_dirtied_buf[ i ];
            }
            return ed->extra_dirtied_page[ i ];
        }
    }
    return NULL;
}

void extra_dirtied_release_all(ExtraDirtiedBufs *ed)
{
    for(int i = 0; i < ed->extra_dirtied_size; i++) {
        assert(BufferIsValid(ed->extra_dirtied_buf[ i ]));
        // header is not considered extra. we know we should not have dirtied it
        // sanity check callees that manimulate extra_dirtied did not violate this
        assert(ed->extra_dirtied_blockno[ i ] != 0);
        MarkBufferDirty(ed->extra_dirtied_buf[ i ]);
        UnlockReleaseBuffer(ed->extra_dirtied_buf[ i ]);
    }
    ed->extra_dirtied_size = 0;
}

void extra_dirtied_free(ExtraDirtiedBufs *ed)
{
    if(ed->extra_dirtied_size != 0) {
        elog(WARNING, "extra dirtied size is not 0. Was something aborted?");
        extra_dirtied_release_all(ed);
    }

    ed->extra_dirtied_size = 0;
    pfree(ed->extra_dirtied_buf);
    pfree(ed->extra_dirtied_page);
    pfree(ed->extra_dirtied_blockno);
    pfree(ed);
}