#include <postgres.h>

#include "insert.h"

#include <float.h>

#include "hnsw.h"

/*
 * Insert a tuple into the index
 */
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
                  IndexInfo *indexInfo)
{
    /* Skip nulls */
    if(isnull[ 0 ]) return false;

    elog(INFO, "hnsw insert");
    elog(ERROR, "not implemented");

    return false;
}
