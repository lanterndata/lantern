#ifndef _OP_REWRITE_H_
#define _OP_REWRITE_H_

#include <nodes/pg_list.h>

typedef struct OpRewriterContext {
    List *ldb_ops;
    List *indices;
} OpRewriterContext;

bool ldb_rewrite_ops(Plan *plan, List *oidList);
#endif
