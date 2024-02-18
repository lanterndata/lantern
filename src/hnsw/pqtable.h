#ifndef LDB_PQTABLE_H
#define LDB_PQTABLE_H

#include <postgres.h>

#include <fmgr.h>

/* Exported functions */
PGDLLEXPORT Datum create_pq_codebook(PG_FUNCTION_ARGS);

#endif  // LDB_PQTABLE_H
