#include "postgres.h"

#include "delete.h"

#include <commands/vacuum.h>
#include <storage/bufmgr.h>

IndexBulkDeleteResult *ldb_ambulkdelete(IndexVacuumInfo        *info,
                                        IndexBulkDeleteResult  *stats,
                                        IndexBulkDeleteCallback callback,
                                        void                   *callback_state)
{
    elog(ERROR, "Deleting from hnsw index not implemented yet");
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *ldb_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation rel = info->index;
    elog(ERROR, "VACUUM CLEANUP not implemented yet");

    if(info->analyze_only) return stats;

    /* stats is NULL if ambulkdelete not called */
    /* OK to return NULL if index not changed */
    if(stats == NULL) return NULL;

    stats->num_pages = RelationGetNumberOfBlocks(rel);

    return stats;
}
