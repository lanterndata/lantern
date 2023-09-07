#include <postgres.h>

#include "executor_start.h"

#include <executor/executor.h>

ExecutorStart_hook_type original_ExecutorStart_hook = NULL;
void                    ExecutorStart_hook_with_operator_check(QueryDesc *queryDesc, int eflags)
{
    // elog(WARNING, "querydesc->plannedstmt: %s", nodeToString(queryDesc->plannedstmt));

    if(original_ExecutorStart_hook) {
        original_ExecutorStart_hook(queryDesc, eflags);
    }
    standard_ExecutorStart(queryDesc, eflags);
}