#ifndef LDB_HNSW_DELETE_H
#define LDB_HNSW_DELETE_H
#include <access/genam.h>
#include <commands/vacuum.h>
#include <storage/bufmgr.h>

IndexBulkDeleteResult *ldb_ambulkdelete(IndexVacuumInfo        *info,
                                        IndexBulkDeleteResult  *stats,
                                        IndexBulkDeleteCallback callback,
                                        void                   *callback_state);
IndexBulkDeleteResult *ldb_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

#endif  // LDB_HNSW_DELETE_H