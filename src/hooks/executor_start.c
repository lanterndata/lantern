#include <postgres.h>

#include "executor_start.h"

#include <executor/executor.h>
#include <nodes/pg_list.h>
#include <nodes/plannodes.h>

#include "plan_tree_walker.h"
#include "utils.h"

ExecutorStart_hook_type original_ExecutorStart_hook = NULL;

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
        bool status = plan_tree_walker(node, operator_used_correctly_walker, (void *)context);
        context->isIndexScan = false;
        return status;
    }
    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(context->oidList, opExpr->opno) && !context->isIndexScan) {
            return true;
        }
    }
    if(IsA(node, List)) {
        List     *list = (List *)node;
        ListCell *lc;
        foreach(lc, list) {
            if(operator_used_correctly_walker(lfirst(lc), context)) return true;
        }
        return false;
    }

    if(nodeTag(node) < T_PlanState) {
        return plan_tree_walker(node, operator_used_correctly_walker, (void *)context);
    } else {
        return expression_tree_walker(node, operator_used_correctly_walker, (void *)context);
    }
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

void ExecutorStart_hook_with_operator_check(QueryDesc *queryDesc, int eflags)
{
    if(original_ExecutorStart_hook) {
        original_ExecutorStart_hook(queryDesc, eflags);
    }

    List *oidList = ldb_get_operator_oids();
    validate_operator_usage(queryDesc->plannedstmt->planTree, oidList);
    ListCell *lc;
    foreach(lc, queryDesc->plannedstmt->subplans) {
        SubPlan *subplan = (SubPlan *)lfirst(lc);
        validate_operator_usage(subplan, oidList);
    }
    list_free(oidList);

    standard_ExecutorStart(queryDesc, eflags);
}