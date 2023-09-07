#ifndef LDB_HOOKS_UTILS_H
#define LDB_HOOKS_UTILS_H

#include <postgres.h>

#include <nodes/pg_list.h>

List *get_operator_oids();

#endif  // LDB_HOOKS_UTILS_H