#ifndef LDB_HOOKS_EXECUTOR_START_H
#define LDB_HOOKS_EXECUTOR_START_H

#include <postgres.h>

#include <executor/executor.h>

extern ExecutorStart_hook_type original_ExecutorStart_hook;
void                           ExecutorStart_hook_with_operator_check(QueryDesc *queryDesc, int eflags);

#endif  // LDB_HOOKS_EXECUTOR_START_H