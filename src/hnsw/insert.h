#ifndef LDB_HNSW_INSERT_H
#define INSERT_H
#include <access/genam.h>     // IndexUniqueCheck
#include <nodes/execnodes.h>  // IndexInfo
#include <storage/bufmgr.h>
#include <utils/memutils.h>

#define LDB_HNSW_INSERT_MAX_EXTRA_DIRTIED_BUFS 5

bool ldb_aminsert(Relation         index,
                Datum           *values,
                bool            *isnull,
                ItemPointer      heap_tid,
                Relation         heap,
                IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                ,
                bool indexUnchanged
#endif
                ,
                IndexInfo *indexInfo);

#endif  // INSERT_H
