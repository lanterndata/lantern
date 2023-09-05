#include <postgres.h>

#include "extra_dirtied.h"

#include <assert.h>

ExtraDirtiedBufs *extra_dirtied_new(Relation index_rel)
{
    ExtraDirtiedBufs *ed = palloc0(sizeof(ExtraDirtiedBufs));
    assert(ed != NULL);
    ed->index_rel = index_rel;
    ed->EXTRA_DIRTIED_BLOCKNO = palloc0(sizeof(BlockNumber) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->EXTRA_DIRTIED_BUF = palloc0(sizeof(Buffer) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->EXTRA_DIRTIED_PAGE = palloc0(sizeof(Page) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->EXTRA_DIRTIED_SIZE = 0;
    return ed;
}

void extra_dirtied_add(ExtraDirtiedBufs *ed, BlockNumber blockno, Buffer buf, Page page)
{
    assert(ed != NULL);
    assert(page != NULL);
    assert(BufferIsValid(buf));
    // todo:: check as invariant that the page is locked at and is under WAL
    // currently, it may not always be under WAL which should be fixed

    assert(ed->EXTRA_DIRTIED_SIZE + 1 < LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->EXTRA_DIRTIED_PAGE[ ed->EXTRA_DIRTIED_SIZE++ ] = page;
    ed->EXTRA_DIRTIED_BUF[ ed->EXTRA_DIRTIED_SIZE - 1 ] = buf;
    ed->EXTRA_DIRTIED_BLOCKNO[ ed->EXTRA_DIRTIED_SIZE - 1 ] = blockno;
}

Page extra_dirtied_get(ExtraDirtiedBufs *ed, BlockNumber blockno, Buffer *buf)
{
    for(int i = 0; i < ed->EXTRA_DIRTIED_SIZE; i++) {
        if(ed->EXTRA_DIRTIED_BLOCKNO[ i ] == blockno) {
            if(buf != NULL) {
                *buf = ed->EXTRA_DIRTIED_BUF[ i ];
            }
            return ed->EXTRA_DIRTIED_PAGE[ i ];
        }
    }
    return NULL;
}

void extra_dirtied_release_all(ExtraDirtiedBufs *ed)
{
    for(int i = 0; i < ed->EXTRA_DIRTIED_SIZE; i++) {
        assert(BufferIsValid(ed->EXTRA_DIRTIED_BUF[ i ]));
        // header is not considered extra. we know we should not have dirtied it
        // sanity check callees that manimulate extra_dirtied did not violate this
        assert(ed->EXTRA_DIRTIED_BLOCKNO[ i ] != 0);
        MarkBufferDirty(ed->EXTRA_DIRTIED_BUF[ i ]);
        UnlockReleaseBuffer(ed->EXTRA_DIRTIED_BUF[ i ]);
    }
    ed->EXTRA_DIRTIED_SIZE = 0;
}

void extra_dirtied_free(ExtraDirtiedBufs *ed)
{
    if(ed->EXTRA_DIRTIED_SIZE != 0) {
        elog(WARNING, "extra dirtied size is not 0. Was something aborted?");
        extra_dirtied_release_all(ed);
    }

    ed->EXTRA_DIRTIED_SIZE = 0;
    pfree(ed->EXTRA_DIRTIED_BUF);
    pfree(ed->EXTRA_DIRTIED_PAGE);
    pfree(ed->EXTRA_DIRTIED_BLOCKNO);
    pfree(ed);
}