#ifndef LDB_HOOKS_PLAN_TREE_WALKER_H
#define LDB_HOOKS_PLAN_TREE_WALKER_H

#include <postgres.h>

#include <nodes/nodes.h>
#include <nodes/plannodes.h>

static inline bool is_plan_node(Node *node)
{
#if PG_VERSION_NUM >= 160000
    return nodeTag(node) >= T_Result && nodeTag(node) <= T_PlanInvalItem;
#else
    return nodeTag(node) >= T_Plan && nodeTag(node) < T_PlanState;
#endif
}

bool  plan_tree_walker(Plan *plan, bool (*walker_func)(Node *node, void *context), void *context);
Node *plan_tree_mutator(Plan *plan, Node *(*mutator_func)(Node *plan, void *context), void *context);
void  base_plan_mutator(Plan *plan, Node *(*mutator_func)(Node *plan, void *context), void *context);

#endif  // LDB_HOOKS_PLAN_TREE_WALKER_H
