#include <postgres.h>
#include <nodes/primnodes.h>
#include <nodes/nodeFuncs.h>

#include <stdbool.h>

#include "utils.h"
#include "plan_tree_walker.h"
#include "op_rewrite.h"

static Node *operator_rewriting_mutator(Node *node, void *ctx) {
    OpRewriterContext *context = (OpRewriterContext *)ctx;

    if (node == NULL) return node;

    if (IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(context->ldb_ops, opExpr->opno)) {
            FuncExpr *fnExpr = makeNode(FuncExpr);
            fnExpr->funcid = opExpr->opfuncid;
            fnExpr->funcresulttype = opExpr->opresulttype;
            fnExpr->funcretset = opExpr->opretset;
            fnExpr->funccollid = opExpr->opcollid;
            fnExpr->inputcollid = opExpr->inputcollid;
            fnExpr->args = opExpr->args;
            fnExpr->location = opExpr->location;

            // operators can't take variadic arguments
            fnExpr->funcvariadic = false;
            // print it as a function
            fnExpr->funcformat = COERCE_EXPLICIT_CALL;

            return (Node *)fnExpr;
        }
    }

    if (IsA(node, IndexScan) || IsA(node, IndexOnlyScan)) {
        return node;
    }

    if (is_plan_node(node)) {
        return (Node *) plan_tree_mutator((Plan *)node, operator_rewriting_mutator, ctx);
    } else {
        return expression_tree_mutator(node, operator_rewriting_mutator, ctx);
    }
}

bool ldb_rewrite_ops(Plan *plan, List *oidList) {
    Node *node = (Node *)plan;

    OpRewriterContext context;
    context.ldb_ops = oidList;
    context.indices = NULL;

    if (IsA(node, IndexScan) || IsA(node, IndexOnlyScan)) {
        return false;
    }

    operator_rewriting_mutator(node, (void *)&context);
    return true;
}
