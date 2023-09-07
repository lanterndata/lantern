#include <postgres.h>

#include "executor_start.h"

#include <executor/executor.h>
#include <nodes/pg_list.h>
#include <nodes/plannodes.h>

#include "plan_tree_walker.h"
#include "utils.h"

typedef struct
{
    List *oidList;
    bool  isIndexScan;
} OperatorUsedCorrectlyContext;

static bool operator_used_correctly_walker(Node *node, OperatorUsedCorrectlyContext *context)
{
    if(node == NULL) return false;
    if(IsA(node, IndexScan)) {
        context->isIndexScan = true;
    }
    if(IsA(node, OpExpr)) {
        if(!context->isIndexScan) {
            return true;
        }
    }

    bool status = plan_tree_walker(node, operator_used_correctly_walker, (void *)context);

    if(IsA(node, IndexScan)) {
        context->isIndexScan = false;
    }
    return status;
}

static bool validate_operator_usage(Plan *plan, List *oidList)
{
    OperatorUsedCorrectlyContext context;
    context.oidList = oidList;
    context.isIndexScan = false;
    if(operator_used_correctly_walker(plan, &context)) {
        elog(ERROR, "Operator <-> has no standalone meaning and is reserved for use in vector index lookups only");
    }
}

ExecutorStart_hook_type original_ExecutorStart_hook = NULL;
void                    ExecutorStart_hook_with_operator_check(QueryDesc *queryDesc, int eflags)
{
    if(original_ExecutorStart_hook) {
        original_ExecutorStart_hook(queryDesc, eflags);
    }

    List *oidList = get_operator_oids();
    if(oidList != NIL) {
        validate_operator_usage(queryDesc->plannedstmt->planTree, oidList);
        ListCell *lc;
        foreach(lc, queryDesc->plannedstmt->subplans) {
            SubPlan *subplan = (SubPlan *)lfirst(lc);
            validate_operator_usage(subplan, oidList);
        }
        list_free(oidList);
    }

    standard_ExecutorStart(queryDesc, eflags);
}