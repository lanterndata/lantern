#ifndef LDB_HNSW_UTILS_H
#define LDB_HNSW_UTILS_H
#include <utils/relcache.h>

#include "usearch.h"

void LogUsearchOptions(usearch_init_options_t *opts);
void PopulateUsearchOpts(Relation index, usearch_init_options_t *opts);
int  CheckOperatorUsage(const char *query);
#define UTILS_H
#endif  // LDB_HNSW_UTILS_H
