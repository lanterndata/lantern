#ifndef LDB_HNSW_VALIDATE_INDEX_H
#define LDB_HNSW_VALIDATE_INDEX_H

#include <postgres.h>

/*
 * This function checks integrity of the data structures in the index relation.
 *
 * How it works:
 * - it creates ldb_vi_block for each block of the index relation and
 *   ldb_vi_node for each node inside the index relation;
 * - it loads all blockmap groups and analyzes mappings between nodes and
 *   blocks;
 * - it loads all the nodes with their neighbors;
 * - it also prints statistics about blocks and nodes, which is useful for
 *   understanding of what's inside the index;
 * - it assumes that PostgreSQL-level data structures are intact (i.e. the page
 *   header and the mapping between offsets and items is correct for each page);
 * - in case if a corruption of the data structure is found the function prints
 *   an error message with details about the place and surrounding data
 *   structures.
 */
void ldb_validate_index(Oid indrelid, bool print_info);

#endif
