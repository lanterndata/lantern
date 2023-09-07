#include <postgres.h>

#include <nodes/nodeFuncs.h>  /* For any other node-related utilities you might use. */
#include <nodes/parsenodes.h> /* If you're handling any parse nodes. */
#include <nodes/plannodes.h>  /* For plan node structures. */

bool plan_tree_walker(Plan *plan, bool (*walker_func)(Node *node, void *context), void *context)
{
    bool result;

    if(plan == NULL) return false;

    // // Apply the callback function to this node.
    // if(walker_func) {
    //     result = walker_func((Node *)plan, context);
    //     if(result) return true;  // If the callback returns true, stop walking.
    // }

    // switch(nodeTag(plan)) {
    //     default:
    //         elog(WARNING, "unrecognized node type: %d", (int)nodeTag(plan));
    // }

    return false;
}
