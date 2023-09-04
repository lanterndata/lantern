#ifndef LDB_HNSW_PARSE_OP_H
#define LDB_HNSW_PARSE_OP_H

#include <postgres.h>

#include <nodes/pg_list.h>
#include <parser/parse_node.h>

// Function to check if an operator is used outside of the ORDER BY clause
bool isOperatorUsedOutsideOrderBy(Node *node, List *oidList);

// Function to retrieve operator OIDs for specific operators
List *get_operator_oids(ParseState *pstate);

#endif  // LDB_HNSW_PARSE_OP_H