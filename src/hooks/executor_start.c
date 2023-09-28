#include <postgres.h>

#include "executor_start.h"

#include <executor/executor.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/plannodes.h>
#include <utils/builtins.h>
#include <catalog/namespace.h>

#include "../hnsw/utils.h"
#include "plan_tree_walker.h"
#include "utils.h"

ExecutorStart_hook_type original_ExecutorStart_hook = NULL;

typedef struct
{
    List *oidList;
    List *distanceFunctionOidList;
    Oid   defaultDistanceFunction;
    bool  isIndexScan;
    bool  isSequentialScan;
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
    if (IsA(node, SeqScan)) {
        context_typed->isSequentialScan = true;
        bool status = plan_tree_walker((Plan *)node, operator_used_incorrectly_walker, context);
        context_typed->isSequentialScan = false;
        return status;       
    }

    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(context_typed->distanceFunctionOidList, opExpr->opno)) {
            context_typed->defaultDistanceFunction = opExpr->opfuncid;
        }
        if(list_member_oid(context_typed->oidList, opExpr->opno) && !context_typed->isIndexScan && !context_typed->isSequentialScan) {
            return true;
        }
        if(list_member_oid(context_typed->oidList, opExpr->opno) && context_typed->isSequentialScan) {
            elog(WARNING, "A sequential scan is being used. This might be because queried column doesn't have an index, or the distance function of the column is a different one.");
            opExpr->opfuncid = context_typed->defaultDistanceFunction;
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

static void validate_operator_usage(Plan *plan, List *oidList, void *distanceFunctionOidList)
{
    OperatorUsedCorrectlyContext context;
    context.oidList = oidList;
    context.distanceFunctionOidList = distanceFunctionOidList;
    FuncCandidateList funcList1 = FuncnameGetCandidates(list_make1(makeString("l2sq_dist")), -1, NIL, false, false, false, true);
    if (funcList1 != NULL) {
        context.defaultDistanceFunction = funcList1->oid;
    } else {
        elog(WARNING, "Default distance function was not found.");
    }
    context.isIndexScan = false;
    context.isSequentialScan = false;
    if(operator_used_incorrectly_walker((Node *)plan, (void *)&context)) {
        elog(ERROR, "Operator <-> has no standalone meaning and is reserved for use in vector index lookups only");
    }
}

void ExecutorStart_hook_with_operator_check(QueryDesc *queryDesc, int eflags)
{
    if(original_ExecutorStart_hook) {
        original_ExecutorStart_hook(queryDesc, eflags);
    }

    List *oidList = ldb_get_operator_oids();
    List *distanceFunctionOidList = ldb_get_distance_function_oids();
    if(oidList != NULL) {
        // oidList will be NULL if LanternDB extension is not fully initialized
        // e.g. in statements executed as a result of CREATE EXTENSION ... statement
        validate_operator_usage(queryDesc->plannedstmt->planTree, oidList, distanceFunctionOidList);
        ListCell *lc;
        foreach(lc, queryDesc->plannedstmt->subplans) {
            Plan *subplan = (Plan *)lfirst(lc);
            validate_operator_usage(subplan, oidList, distanceFunctionOidList);
        }
        list_free(oidList);
        list_free(distanceFunctionOidList);
    }

    standard_ExecutorStart(queryDesc, eflags);
}