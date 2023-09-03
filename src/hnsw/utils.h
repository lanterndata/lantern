#ifndef LDB_HNSW_UTILS_H
#define LDB_HNSW_UTILS_H
#include <access/amapi.h>

#include "usearch.h"

void            LogUsearchOptions(usearch_init_options_t *opts);
void            PopulateUsearchOpts(Relation index, usearch_init_options_t *opts);
usearch_label_t GetUsearchLabel(ItemPointer itemPtr);
#define UTILS_H
#endif  // LDB_HNSW_UTILS_H
