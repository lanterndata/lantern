#ifndef LDB_HNSW_INSERT_H
#define INSERT_H
#include <access/genam.h>     // IndexUniqueCheck
#include <nodes/execnodes.h>  // IndexInfo
#include <storage/bufmgr.h>
#include <utils/memutils.h>

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
