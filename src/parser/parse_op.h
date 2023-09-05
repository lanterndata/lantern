#ifndef LDB_HNSW_PARSE_OP_H
#define LDB_HNSW_PARSE_OP_H

#include <postgres.h>

#include <nodes/pg_list.h>
#include <parser/parse_node.h>

// Function to check if an operator is used outside of the ORDER BY clause
bool         validate_operator_usage(Node *node, List *oidList);
static bool  isOperatorUsedOutsideOrderBy(Node *node, List *oidList, List *sortGroupRefs);
static List *get_sort_group_refs(Node *node, List *sort_group_refs);

// Function to retrieve operator OIDs for specific operators
List *get_operator_oids(ParseState *pstate);

#endif  // LDB_HNSW_PARSE_OP_H