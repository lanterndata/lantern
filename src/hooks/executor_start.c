#include <postgres.h>

#include "executor_start.h"

#include <executor/executor.h>
#include <nodes/pg_list.h>
#include <nodes/plannodes.h>

#include "utils.h"

static bool validate_operator_usage(Plan *plan, List *oidList)
{
    elog(WARNING, "plan state: %s", nodeToString(plan));
    if(IsA(plan, Limit)) {
        elog(WARNING, "I'm a limit");
    } else {
        elog(WARNING, "I'm not a limit");
    }
    return true;
}

ExecutorStart_hook_type original_ExecutorStart_hook = NULL;
void                    ExecutorStart_hook_with_operator_check(QueryDesc *queryDesc, int eflags)
{
    if(original_ExecutorStart_hook) {
        original_ExecutorStart_hook(queryDesc, eflags);
    }

    List *oidList = get_operator_oids();
    if(oidList != NIL) {
        if(!validate_operator_usage(queryDesc->plannedstmt->planTree, oidList)) {
            elog(ERROR, "Operator <-> has no standalone meaning and is reserved for use in vector index lookups only");
        }
        list_free(oidList);
    }

    standard_ExecutorStart(queryDesc, eflags);
}