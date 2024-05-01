#include <postgres.h>

#include "delete.h"

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
    LDB_UNUSED(stats);
    elog(WARNING,
         "LanternDB: hnsw index deletes are currently not implemented. This is a no-op. No memory will be reclaimed");
    // traverse through the index and call the callback for all elements
    BlockNumber         blockno;
    Buffer              buf;
    HnswIndexHeaderPage header;
    Page                page;
    OffsetNumber        offset, maxoffset;
    ItemPointerData     tid_data;

    for(BlockNumber blockno = 0;; blockno++) {
        if(blockno > 0 && blockno > header.last_data_block) {
            break;
        }
        vacuum_delay_point();
        buf = ReadBufferExtended(info->index, MAIN_FORKNUM, blockno, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        if(0 == blockno) {
            header = *(HnswIndexHeaderPage *)PageGetContents(page);
        } else {
            maxoffset = PageGetMaxOffsetNumber(page);

            if(isBlockMapBlock(header.blockmap_groups, header.blockmap_groups_nr, blockno)) {
                ldb_invariant(1 == maxoffset, "expected blockmap page with single item");
                HnswBlockmapPage *blockmap_page
                    = (HnswBlockmapPage *)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
            } else {
                for(offset = FirstOffsetNumber; offset <= maxoffset; offset = OffsetNumberNext(offset)) {
                    HnswIndexTuple *nodepage = (HnswIndexTuple *)PageGetItem(page, PageGetItemId(page, offset));
                    unsigned long   label = label_from_node(nodepage->node);
                    label2ItemPointer(label, &tid_data);
                    if(callback(&tid_data, callback_state)) {
                        // todo:: mark item as deleted
                    }
                }
            }
        }

        UnlockReleaseBuffer(buf);
    }

    // the NULL is passed to vacuumcleanup which handles being passed a NULL
    return NULL;
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
