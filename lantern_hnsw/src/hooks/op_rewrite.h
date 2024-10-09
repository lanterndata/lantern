#ifndef LDB_HOOKS_OP_REWRITE_H
#define LDB_HOOKS_OP_REWRITE_H

#include <nodes/pg_list.h>
#include <nodes/plannodes.h>

typedef struct OpRewriterContext
{
    List *ldb_ops;
    List *indices;
    List *rtable;
} OpRewriterContext;

bool ldb_rewrite_ops(Plan *plan, List *oidList, List *rtable);
#endif
