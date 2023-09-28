#ifndef LDB_HOOKS_UTILS_H
#define LDB_HOOKS_UTILS_H

#include <postgres.h>

#include <nodes/pg_list.h>

List *ldb_get_operator_oids();
List *ldb_get_distance_function_oids();

#endif  // LDB_HOOKS_UTILS_H