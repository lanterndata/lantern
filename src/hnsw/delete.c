#include <postgres.h>

#include "delete.h"

#include <commands/vacuum.h>
#include <storage/bufmgr.h>

#include "hnsw.h"

IndexBulkDeleteResult *ldb_ambulkdelete(IndexVacuumInfo        *info,
                                        IndexBulkDeleteResult  *stats,
                                        IndexBulkDeleteCallback callback,
                                        void                   *callback_state)
{
    LDB_UNUSED(info);
    LDB_UNUSED(stats);
    LDB_UNUSED(callback);
    LDB_UNUSED(callback_state);
    elog(WARNING,
         "LanternDB: hnsw index deletes are currently not implemented. This is a no-op. No memory will be reclaimed");
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
