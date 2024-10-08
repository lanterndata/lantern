#include <postgres.h>

#include "extra_dirtied.h"

#include <assert.h>

#include "external_index.h"
#include "utils.h"

ExtraDirtiedBufs *extra_dirtied_new()
{
    ExtraDirtiedBufs *ed = palloc0(sizeof(ExtraDirtiedBufs));
    assert(ed != NULL);
    ed->extra_dirtied_blockno = palloc0(sizeof(BlockNumber) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->extra_dirtied_buf = palloc0(sizeof(Buffer) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->extra_dirtied_page = palloc0(sizeof(Page) * LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS);
    ed->extra_dirtied_state = palloc0(sizeof(GenericXLogState *) * LDB_HNSW_EXTRA_DIRTIED_MAX_WAL_RECORDS);
    ed->extra_dirtied_size = 0;
    ed->extra_dirtied_state_size = 0;
    ed->extra_dirtied_pages_in_the_last_state = MAX_GENERIC_XLOG_PAGES;
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

void extra_dirtied_add_wal_read_buffer(
    ExtraDirtiedBufs *ed, Relation index, ForkNumber forkNum, BlockNumber blockno, Buffer *buf, Page *page)
{
    if(ed->extra_dirtied_pages_in_the_last_state == MAX_GENERIC_XLOG_PAGES) {
        ldb_invariant((size_t)ed->extra_dirtied_state_size < LDB_HNSW_EXTRA_DIRTIED_MAX_WAL_RECORDS,
                      "too many dirtied bufs to fit in extra_dirtied WAL records");
        ed->extra_dirtied_state[ ed->extra_dirtied_state_size++ ] = GenericXLogStart(index);
        ed->extra_dirtied_pages_in_the_last_state = 0;
    }
    *buf = ReadBufferExtended(index, forkNum, blockno, RBM_NORMAL, NULL);
    LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);
    *page = GenericXLogRegisterBuffer(
        ed->extra_dirtied_state[ ed->extra_dirtied_state_size - 1 ], *buf, LDB_GENERIC_XLOG_DELTA_IMAGE);
    ++ed->extra_dirtied_pages_in_the_last_state;
    extra_dirtied_add(ed, blockno, *buf, *page);
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
    XLogRecPtr ptr;

    for(int i = 0; i < ed->extra_dirtied_state_size; ++i) {
        ptr = GenericXLogFinish(ed->extra_dirtied_state[ i ]);
        ldb_invariant(ptr != InvalidXLogRecPtr, "GenericXLogFinish() has failed.");
    }
    for(int i = 0; i < ed->extra_dirtied_size; i++) {
        assert(BufferIsValid(ed->extra_dirtied_buf[ i ]));
        // header is not considered extra. we know we should not have dirtied it
        // sanity check callees that manimulate extra_dirtied did not violate this
        assert(ed->extra_dirtied_blockno[ i ] != 0);
        // MarkBufferDirty() had been called by by GenericXLogFinish() already
        UnlockReleaseBuffer(ed->extra_dirtied_buf[ i ]);
    }
    ed->extra_dirtied_size = 0;
}

// Like extra_dirtied_release_all but does not perform a InvalidXLogRecPtr check.
// Used for inserts on unlogged tables, which do not write to WAL
void extra_dirtied_release_all_no_xlog_check(ExtraDirtiedBufs *ed)
{
    for(int i = 0; i < ed->extra_dirtied_state_size; ++i) {
        GenericXLogFinish(ed->extra_dirtied_state[ i ]);
    }

    for(int i = 0; i < ed->extra_dirtied_size; i++) {
        assert(BufferIsValid(ed->extra_dirtied_buf[ i ]));
        // header is not considered extra. we know we should not have dirtied it
        // sanity check callees that manimulate extra_dirtied did not violate this
        assert(ed->extra_dirtied_blockno[ i ] != 0);
        // MarkBufferDirty() had been called by by GenericXLogFinish() already
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
    pfree(ed->extra_dirtied_state);
    pfree(ed->extra_dirtied_buf);
    pfree(ed->extra_dirtied_page);
    pfree(ed->extra_dirtied_blockno);
    pfree(ed);
}
