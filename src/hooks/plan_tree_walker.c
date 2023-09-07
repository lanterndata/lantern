#include <postgres.h>

#include <nodes/nodeFuncs.h>  /* For any other node-related utilities you might use. */
#include <nodes/parsenodes.h> /* If you're handling any parse nodes. */
#include <nodes/plannodes.h>  /* For plan node structures. */

bool plan_tree_walker(Plan *plan, bool (*walker_func)(Node *node, void *context), void *context)
{
    bool result;

    if(plan == NULL) return false;

    // Apply the callback function to this node.
    if(walker_func) {
        result = walker_func((Node *)plan, context);
        if(result) return true;  // If the callback returns true, stop walking.
    }

    switch(nodeTag(plan)) {
        case T_SeqScan: {
            SeqScan *scan = (SeqScan *)plan;

            // Walk the expression trees in the node
            if(expression_tree_walker((Node *)scan->qual, walker_func, context)) return true;

            // ... any other expression fields ...

            // Walk the left and right children
            if(plan_tree_walker(plan->lefttree, walker_func, context)) return true;
            if(plan_tree_walker(plan->righttree, walker_func, context)) return true;
        } break;

            // ... handle other plan node types ...

        default:
            elog(ERROR, "unrecognized node type: %d", (int)nodeTag(plan));
            break;
    }

    return false;
}
