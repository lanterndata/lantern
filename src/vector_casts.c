#include <postgres.h>

#include "vector_casts.h"

#include <assert.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/lsyscache.h>  // for get_typlenbyvalalign

#include "usearch.h"
#include "vec_type.h"

/*** Functions generic over all vec types (uvec8, vec8, vec16, vec32) */

ArrayType *ldb_generic_vec_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);

    // The second argument is the type OID of our newly created type
    // we do not need that. For arrays, it would be the element type
    // and not the array type. It would be good to have this be the
    // element type for our vec types as well. But I do not think that
    // is possible for custom types
    // We always read the passed array element type as FLOAT4OID
    // In the future, we may want to have vec64 (but unlikely, since
    // postgres's own double precision works just fine)
    int32 typioparam = FLOAT4OID;

    // represents the number of dimensions in the vec type: '{1,1,1}'::vec(3)
    //                                                                 ....^...
    int32 typmod = PG_GETARG_INT32(2);

    Oid        oid = fmgr_internal_function("array_in");
    ArrayType *array;

    array = (ArrayType *)DatumGetPointer(OidInputFunctionCall(oid, str, typioparam, typmod));
    // postgres does not enforce the array bounds(cite?), but we want to enforce them
    /* todo:: this is a possible potential bug (or atleast very surprising behavior).
     * Say we create a type and have typemod_in set up for it and provide a type for it
     * e.g. '{1,2,3}::vec8(3)' for us.
     * Postgres calls typemod_in and validates the '3' parameter before calling this function
     * BUT, postgres does not pass typmod returned by the typmod function to this function
     * as its third argument.
     * So, the branch below is not triggered for the case above: even if we had passed '4' as
     * type parameter, the validation below would not trigger
     * Validation currently is still triggered through the cast function (cast from vec8 to vec8)
     * so UX is not affected but we should probably report this or talk to postgres devs
     * to see if this is expected behaviour.
     */
    if(typmod != -1 && ARR_NDIM(array) != typmod) {
        elog(ERROR, "vec type expected %d dimensions but provided array has %d dimensions", typmod, ARR_NDIM(array));
    }
    return array;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_generic_vec_typmod_in);
/*
 * Provides Type modifier with semantics similar to postgres arrays
 * The only difference is that we actually enforce these constraints
 * '{1,2,3}'::int[4] is valid but '{1,2,3}'::vec8(4) is not
 * Postgres Docs: The type_modifier_input_function is passed the
 * declared modifier(s) in the form of a cstring array.
 * It must check the values for validity (throwing an error if they are wrong),
 * and if they are correct, return a single non-negative integer value that
 * will be stored as the column “typmod”
 */
Datum ldb_generic_vec_typmod_in(PG_FUNCTION_ARGS)
{
    ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
    int32     *tl;
    int        n;

    tl = ArrayGetIntegerTypmods(ta, &n);

    if(n != 1) {
        elog(ERROR, "wrong number of modifiers");
    }

    if(*tl < 1) {
        elog(ERROR, "vector dimensions must be at least 1");
    }

    if(*tl > LDB_VEC_MAX_DIM) {
        elog(ERROR, "vector dimensions must be at most %d", LDB_VEC_MAX_DIM);
    }

    return Int32GetDatum(*tl);
}

Datum ldb_generic_vec_out(ArrayType *arr)
{
    Oid   oid = fmgr_internal_function("array_out");
    char *res = OidOutputFunctionCall(oid, PointerGetDatum(arr));
    return CStringGetDatum(res);
}

/*
 * Convert LDB's 'vec*' external binary representation to the internal representation
 */
Datum ldb_generic_vec_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    int32      typmod = PG_GETARG_INT32(2);
    LDBVec    *result;
    uint16     dim;
    uint16     elem_size;
    elog(ERROR, "vec recv called");

    dim = pq_getmsgint(buf, sizeof(uint16));
    elem_size = pq_getmsgint(buf, sizeof(uint16));

    if(dim < 1 || dim > LDB_VEC_MAX_DIM) {
        elog(ERROR, "received binary vec with invalid invalid dimension %d", dim);
    }

    if(typmod != -1 && dim != typmod) {
        elog(ERROR, "received binary vec with wrong dimension %d, expected %d", dim, typmod);
    }

    result = NewLDBVec(dim, elem_size);
    pq_copymsgbytes(buf, result->data, elem_size * dim);
    // todo:: validate that the copy succeeded and result is not corrupted
    PG_RETURN_POINTER(result);
}

/*
 * Convert LDB's 'vec*' internal representation to the external binary representation
 */
Datum ldb_generic_vec_send(PG_FUNCTION_ARGS)
{
    StringInfoData buf;
    LDBVec        *vec = DatumGetLDBVec(PG_GETARG_DATUM(0));

    elog(ERROR, "vec send called");
    pq_begintypsend(&buf);
    pq_sendint(&buf, vec->dim, sizeof(uint16));
    pq_sendint(&buf, vec->elem_size, sizeof(uint16));
    pq_sendbytes(&buf, vec->data, vec->elem_size * vec->dim);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/******************************* UVEC8 ******************************/

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_uvec8_in);

Datum ldb_uvec8_in(PG_FUNCTION_ARGS)
{
    LDBVec         *res;
    usearch_error_t error = NULL;
    // fcinfo below is from the macro PG_FUNCTION_ARGS and passes SQL-arguments
    // to the generic vec reader
    ArrayType *array = ldb_generic_vec_in(fcinfo);
    assert(array != NULL);
    // if the element type ever changes (e.g. to FLOAT8OID),
    // the usearch_cast call must be reworked.
    // on 64bit systems FLOAT8OID fits inline in a Datum BUT on 32 bit systems it
    // does not so when casting from FLOAT8 array, I can no longer assume
    // ARR_DATA_PTR(array) is an array of element values (there will be another
    // indirection via a pointer)
    assert(array->elemtype == FLOAT4OID);
    int ndims = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    res = ldb_cast_array_vec(array, ndims, usearch_scalar_f8_k);
    // in vec8, each vector element is one byte, hense the 1
    // res = NewLDBVec(ndims, 1);
    // float *array_elems = (float *)ARR_DATA_PTR(array);

    if(error) {
        elog(ERROR, "error in float downcasting: %s", error);
    }

    return PointerGetDatum(res);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_uvec8_out);

Datum ldb_uvec8_out(PG_FUNCTION_ARGS)
{
    ArrayType      *res;
    float          *array_elems;
    Datum          *array_elems_datum;
    int             array_elems_size;
    usearch_error_t error = NULL;
    LDBVec         *vec = DatumGetLDBVec(PG_GETARG_DATUM(0));
    assert(vec->dim != 0);
    assert(vec->elem_size == 1);
    array_elems_size = sizeof(float) * vec->dim;
    array_elems = palloc0(array_elems_size);
    array_elems_datum = palloc0(sizeof(Datum) * vec->dim);
    usearch_cast(usearch_scalar_f8_k, vec->data, usearch_scalar_f32_k, array_elems, array_elems_size, vec->dim, &error);
    if(error != NULL) {
        elog(ERROR, "error casting: %s", error);
    }

    for(int i = 0; i < vec->dim; i++) {
        array_elems_datum[ i ] = Float4GetDatum(array_elems[ i ]);
    }

    res = construct_array(array_elems_datum, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);
    assert(res != NULL);

    assert(res->elemtype == FLOAT4OID);
    int ndims = ArrayGetNItems(ARR_NDIM(res), ARR_DIMS(res));
    assert(ndims = vec->dim);

    return ldb_generic_vec_out(res);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_uvec8_recv);
Datum       ldb_uvec8_recv(PG_FUNCTION_ARGS) { return ldb_generic_vec_recv(fcinfo); }

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_uvec8_send);
Datum       ldb_uvec8_send(PG_FUNCTION_ARGS) { return ldb_generic_vec_send(fcinfo); }

// PGDLLEXPORT PG_FUNCTION_INFO_V1(array_to_vector);
// Datum
// array_to_vec8(PG_FUNCTION_ARGS){}

/************ CASTS *************/
/*
 * Casts for  type vec* to type vec*
 * Note: these functions are called in callsites like '{1,2,3}'::vec8(4)
 * The call to this function is what throws the dimension mismatch error
 * vect*_in calls have a typmod parameter but the parsed typmod result is not
 * passed to them.
 */

// dest must be provided iff from != to and it must be initialized
// and sized appropriately for 'to'
Datum ldb_cast_vec_vec(FunctionCallInfo fcinfo, int from, int to, LDBVec *dest, int ddata_size)
{
    LDBVec         *src = DatumGetLDBVec(PG_GETARG_DATUM(0));
    int32           typmod = PG_GETARG_INT32(1);
    usearch_error_t error = NULL;
    assert((from == to) ^ (dest != NULL));

    if(typmod != -1 && src->dim != typmod) {
        elog(ERROR, "vector cast wrong dimension %d, expected %d", src->dim, typmod);
    }

    if(from != to) {
        assert(src->dim == dest->dim);
        usearch_cast(from, src->data, to, dest->data, ddata_size, src->dim, &error);
        // todo:: q:: who manages the memory of the src?
        // pretty sure the caller must deal with it but would be good to know for sure from code
        // or documentation
        return PointerGetDatum(dest);
    }

    return PointerGetDatum(src);
}

// clang-format off
PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_cast_uvec8_uvec8);
PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_cast_vec32_vec32);

Datum ldb_cast_uvec8_uvec8(PG_FUNCTION_ARGS) { return ldb_cast_vec_vec(fcinfo, usearch_scalar_f8_k, usearch_scalar_f8_k, NULL, 0); }
Datum ldb_cast_vec32_vec32(PG_FUNCTION_ARGS) { return ldb_cast_vec_vec(fcinfo, usearch_scalar_f32_k, usearch_scalar_f32_k, NULL, 0); }
// clang-format on

// if expected_dim is -1, then the dimension is not checked
// otherwise, an error is thrown if the dimension is not as expected
LDBVec *ldb_cast_array_vec(ArrayType *array, int expected_dim, usearch_scalar_kind_t to)
{
    usearch_error_t error = NULL;

    LDBVec *result;
    int16   typlen;
    bool    typbyval;
    char    typalign;
    Datum  *elemsp;
    bool   *nullsp;
    int     nelemsp;

    if(ARR_NDIM(array) > 1) {
        elog(ERROR, "array must be 1-D");
    }

    if(ARR_HASNULL(array) && array_contains_nulls(array)) {
        elog(ERROR, "array must not contain nulls");
    }

    get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
    deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);
    if(nelemsp == 0) {
        elog(ERROR, "array must not be empty");
    }

    if(nelemsp > LDB_VEC_MAX_DIM) {
        elog(ERROR, "array too large. max vec dimension is %d", LDB_VEC_MAX_DIM);
    }

    if(expected_dim != -1 && nelemsp != expected_dim) {
        elog(ERROR, "array has wrong dimension %d, expected %d", nelemsp, expected_dim);
    }

    result = NewLDBVec(nelemsp, scalar_size(to));
    // rsult->data is usually opeque to us and handled by usearch
    // since it contains types like float16, float8, ufloat8, which are not available in C.
    // here, we init it with 32-bit floats which are the same here and in usearch
    // so this is fine
    float *vec_floats = (float *)palloc0(nelemsp * sizeof(float));

    switch(ARR_ELEMTYPE(array)) {
        case INT4OID:
            for(int i = 0; i < nelemsp; i++) {
                vec_floats[ i ] = DatumGetInt32(elemsp[ i ]);
            }
            break;
        case FLOAT4OID:
            for(int i = 0; i < nelemsp; i++) {
                vec_floats[ i ] = DatumGetFloat4(elemsp[ i ]);
            }
            break;
        case FLOAT8OID:
        case NUMERICOID:
            // todo:: x, FLOAT8, NUMERIC
            elog(ERROR, "unimplemented cast to vec type");
            break;
        default:
            elog(ERROR, "unknown array elem type %d", ARR_ELEMTYPE(array));
    }

    if (usearch_scalar_f8_k == to) {
        // sanity check and throw an error if uvec8 is used with floats outside of [-1, 1]
        for(int i = 0; i < nelemsp; i++) {
            if(vec_floats[ i ] < -1 || vec_floats[ i ] > 1) {
                elog(ERROR, "uvec8 must be in range [-1, 1]");
            }
        }
    }

    usearch_cast(
        usearch_scalar_f32_k, vec_floats, to, LDBVEC_DATA_PTR(result), result->dim, LDBVEC_DATA_SIZE(result), &error);

    return result;
}