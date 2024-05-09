#include <postgres.h>

#include "delete.h"

#include <access/generic_xlog.h>
#include <commands/vacuum.h>
#include <storage/bufmgr.h>
#include <storage/off.h>

#include "hnsw.h"
#include "hnsw/external_index.h"
#include "hnsw/utils.h"
#include "usearch_storage.hpp"

IndexBulkDeleteResult *ldb_ambulkdelete(IndexVacuumInfo        *info,
                                        IndexBulkDeleteResult  *stats,
                                        IndexBulkDeleteCallback callback,
                                        void                   *callback_state)
{
    if(stats == NULL) {
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));
    }

    elog(WARNING,
         "LanternDB: hnsw index deletes are currently not implemented. This is a no-op. No memory will be reclaimed");
    // traverse through the index and call the callback for all elements
    Buffer              buf;
    HnswIndexHeaderPage header;
    Page                page;
    OffsetNumber        offset, maxoffset;
    ItemPointerData     tid_data;
    GenericXLogState   *gxlogState;
    buf = ReadBufferExtended(info->index, MAIN_FORKNUM, 0, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
    // todo:: consider making this a shared lock if it would matter
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    header = *(HnswIndexHeaderPage *)PageGetContents(page);
    UnlockReleaseBuffer(buf);

    for(BlockNumber blockno = 1; blockno <= header.last_data_block; blockno++) {
        bool block_modified = false;
        vacuum_delay_point();
        buf = ReadBufferExtended(info->index, MAIN_FORKNUM, blockno, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        gxlogState = GenericXLogStart(info->index);
        page = GenericXLogRegisterBuffer(gxlogState, buf, LDB_GENERIC_XLOG_DELTA_IMAGE);
        maxoffset = PageGetMaxOffsetNumber(page);

        if(false /*pq header page*/) {
            // todo:: this could also be a pq page(see external_index.c, opts->pq handling)
        } else {
            for(offset = FirstOffsetNumber; offset <= maxoffset; offset = OffsetNumberNext(offset)) {
                HnswIndexTuple *nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
                unsigned long   label = label_from_node(nodepage->node);
                label2ItemPointer(label, &tid_data);
                if(callback(&tid_data, callback_state)) {
                    block_modified = true;
                    reset_node_label(nodepage->node);
                    stats->tuples_removed += 1;
                }
            }
        }

        if(block_modified) {
            GenericXLogFinish(gxlogState);
        } else {
            GenericXLogAbort(gxlogState);
        }

        UnlockReleaseBuffer(buf);
    }
    return stats;
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *ldb_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation rel = info->index;

    if(info->analyze_only) return stats;

    /* stats is NULL if ambulkdelete not called */
    /* OK to return NULL if index not changed */
    if(stats == NULL) return NULL;

    stats->num_pages = RelationGetNumberOfBlocks(rel);

    return stats;
}
