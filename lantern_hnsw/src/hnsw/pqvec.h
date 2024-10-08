#ifndef PQVEC_H
#define PQVEC_H

#include <postgres.h>

#include <catalog/pg_type.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/guc.h>

#define DatumGetPQVec(x)  ((PQVec *)PG_DETOAST_DATUM(x))
#define PQVEC_DATA_PTR(a) (((void *)(a->data)))

typedef struct
{
    int32  vl_len_; /* varlena header (do not touch directly!) */
    uint16 dim;     /* number of dimensions */
    uint16 elem_type;
    char   data[ FLEXIBLE_ARRAY_MEMBER ];
} PQVec;

PGDLLEXPORT Datum ldb_pqvec_in(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum ldb_pqvec_out(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum ldb_pqvec_send(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum ldb_pqvec_recv(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum ldb_cast_pqvec_array(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum ldb_cast_array_pqvec(PG_FUNCTION_ARGS);

#endif  // PQVEC_H
