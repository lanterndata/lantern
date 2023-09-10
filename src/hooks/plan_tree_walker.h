#ifndef LDB_HOOKS_PLAN_TREE_WALKER_H
#define LDB_HOOKS_PLAN_TREE_WALKER_H

#include <postgres.h>

#include <nodes/plannodes.h>

bool plan_tree_walker(Plan *plan, bool (*walker_func)(Node *node, void *context), void *context);

#endif  // LDB_HOOKS_PLAN_TREE_WALKER_H