#include <postgres.h>

#include "pqvec.h"

#include <assert.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/guc.h>

#if PG_VERSION_NUM < 130000
#define TYPALIGN_INT 'i'
#endif

static inline PQVec *NewPQVec(int dim)
{
    PQVec *result;
    int    size;

    // currently the scalar size is hardcoded to 1 byte
    size = sizeof(PQVec) + dim * 1;
    result = (PQVec *)palloc0(size);
    SET_VARSIZE(result, size);
    result->dim = dim;

    return result;
}

/*
 * Convert ArrayType to PQVec downcasting elements to 1byte
 * */
PQVec *ldb_array_to_pqvec(ArrayType *array)
{
    int32  i;
    int32  maxItemSize = (1 << 8) - 1;
    int32  ndims = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    PQVec *res = NewPQVec(ndims);
    int32 *intarr = (int32 *)ARR_DATA_PTR(array);

    for(i = 0; i < ndims; i++) {
        if(intarr[ i ] > maxItemSize) {
            elog(ERROR, "Compressed vector element can not be bigger than %d", maxItemSize);
        }
        if(intarr[ i ] < 0) {
            elog(ERROR, "Compressed vector element can not be smaller than 0");
        }
        res->data[ i ] = (uint8)intarr[ i ];
    }

    return res;
}

/*
 * Convert PQVec to INT[]
 * */
ArrayType *ldb_pqvec_to_array(uint8 *array_elems, int dim)
{
    ArrayType *res;
    Datum     *array_elems_datum = palloc0(sizeof(Datum) * dim);

    for(int i = 0; i < dim; i++) {
        array_elems_datum[ i ] = UInt32GetDatum((uint32)array_elems[ i ]);
    }
    res = construct_array(array_elems_datum, dim, INT4OID, sizeof(uint32), true, TYPALIGN_INT);

    return res;
}

/*
 * Cast PQVec to INT[]
 * */
PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_cast_pqvec_array);
Datum       ldb_cast_pqvec_array(PG_FUNCTION_ARGS)
{
    PQVec     *vec = DatumGetPQVec(PG_GETARG_DATUM(0));
    ArrayType *res = ldb_pqvec_to_array(PQVEC_DATA_PTR(vec), vec->dim);
    return PointerGetDatum(res);
}

/*
 * Cast INT[] to PQVec
 * */
PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_cast_array_pqvec);
Datum       ldb_cast_array_pqvec(PG_FUNCTION_ARGS)
{
    ArrayType *array = PG_GETARG_ARRAYTYPE_P(0);
    PQVec     *res = ldb_array_to_pqvec(array);
    return PointerGetDatum(res);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_pqvec_in);
Datum       ldb_pqvec_in(FunctionCallInfo fcinfo)
{
    char      *str = PG_GETARG_CSTRING(0);
    int32      typioparam = INT4OID;
    int32      typmod = PG_GETARG_INT32(2);
    Oid        oid = fmgr_internal_function("array_in");
    ArrayType *array;
    PQVec     *res;

    array = (ArrayType *)DatumGetPointer(OidInputFunctionCall(oid, str, typioparam, typmod));

    if(ARR_NDIM(array) == 0) {
        elog(ERROR, "pqvector can not be empty");
    }

    if(typmod != -1 && ARR_NDIM(array) != typmod) {
        elog(ERROR, "array type expected %d dimensions but provided array has %d dimensions", typmod, ARR_NDIM(array));
    }

    res = ldb_array_to_pqvec(array);
    return PointerGetDatum(res);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_pqvec_out);
Datum       ldb_pqvec_out(FunctionCallInfo fcinfo)
{
    ArrayType *arr;
    PQVec     *vec = DatumGetPQVec(PG_GETARG_DATUM(0));
    assert(vec->dim != 0);

    arr = ldb_pqvec_to_array(PQVEC_DATA_PTR(vec), vec->dim);

    Oid   oid = fmgr_internal_function("array_out");
    char *res = OidOutputFunctionCall(oid, PointerGetDatum(arr));
    return CStringGetDatum(res);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_pqvec_send);
Datum       ldb_pqvec_send(PG_FUNCTION_ARGS)
{
    StringInfoData buf;
    PQVec         *vec = DatumGetPQVec(PG_GETARG_DATUM(0));

    pq_begintypsend(&buf);
    pq_sendint(&buf, vec->dim, sizeof(uint16));
    pq_sendbytes(&buf, vec->data, vec->dim);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_pqvec_recv);
Datum       ldb_pqvec_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    PQVec     *result;
    uint16     dim;

    dim = pq_getmsgint(buf, sizeof(uint16));

    result = NewPQVec(dim);
    // 1 is scalar size (for now 1 byte)
    pq_copymsgbytes(buf, result->data, dim * 1);
    PG_RETURN_POINTER(result);
}
