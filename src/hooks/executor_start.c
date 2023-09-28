#include <postgres.h>

#include "executor_start.h"

#include <executor/executor.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/plannodes.h>

#include "../hnsw/utils.h"
#include "op_rewrite.h"
#include "plan_tree_walker.h"
#include "utils.h"

ExecutorStart_hook_type original_ExecutorStart_hook = NULL;

typedef struct
{
    List *oidList;
    bool  isIndexScan;
} OperatorUsedCorrectlyContext;

static bool operator_used_incorrectly_walker(Node *node, void *context)
{
    OperatorUsedCorrectlyContext *context_typed = (OperatorUsedCorrectlyContext *)context;
    if(node == NULL) return false;
    if(IsA(node, IndexScan)) {
        context_typed->isIndexScan = true;
        bool status = plan_tree_walker((Plan *)node, operator_used_incorrectly_walker, context);
        context_typed->isIndexScan = false;
        return status;
    }
    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(context_typed->oidList, opExpr->opno) && !context_typed->isIndexScan) {
            return true;
        }
    }
    if(IsA(node, List)) {
        List     *list = (List *)node;
        ListCell *lc;
        foreach(lc, list) {
            if(operator_used_incorrectly_walker(lfirst(lc), context)) return true;
        }
        return false;
    }

    if(is_plan_node(node)) {
        return plan_tree_walker((Plan *)node, operator_used_incorrectly_walker, (void *)context);
    } else {
        return expression_tree_walker(node, operator_used_incorrectly_walker, (void *)context);
    }
    return false;
}

static void validate_operator_usage(Plan *plan, List *oidList)
{
    OperatorUsedCorrectlyContext context;
    context.oidList = oidList;
    context.isIndexScan = false;
    if(operator_used_incorrectly_walker((Node *)plan, (void *)&context)) {
        elog(ERROR, "Operator <-> can only be used inside of an index");
    }
}

void ExecutorStart_hook_with_operator_check(QueryDesc *queryDesc, int eflags)
{
    if(original_ExecutorStart_hook) {
        original_ExecutorStart_hook(queryDesc, eflags);
    }

    List *oidList = ldb_get_operator_oids();
    if(oidList != NULL) {
        // oidList will be NULL if LanternDB extension is not fully initialized
        // e.g. in statements executed as a result of CREATE EXTENSION ... statement
        ldb_rewrite_ops(queryDesc->plannedstmt->planTree, oidList, queryDesc->plannedstmt->rtable);
        validate_operator_usage(queryDesc->plannedstmt->planTree, oidList);
        ListCell *lc;
        foreach(lc, queryDesc->plannedstmt->subplans) {
            Plan *subplan = (Plan *)lfirst(lc);
        ldb_rewrite_ops(subplan, oidList, queryDesc->plannedstmt->rtable);
            validate_operator_usage(subplan, oidList);
        }
        list_free(oidList);
    }

    standard_ExecutorStart(queryDesc, eflags);
}
