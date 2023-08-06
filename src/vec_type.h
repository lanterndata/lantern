#ifndef LDB_VEC_H
#define LDB_VEC_H
#include <postgres.h>

typedef struct
{
    int32  vl_len_; /* varlena header (do not touch directly!) */
    uint16 dim;     /* number of dimensions */
    uint16 elem_size;
    char   data[ FLEXIBLE_ARRAY_MEMBER ];
} LDBVec;

static inline LDBVec *NewLDBVec(int dim, int elem_size)
{
    LDBVec *result;
    int     size;

    size = sizeof(LDBVec) + dim * elem_size;
    result = (LDBVec *)palloc0(size);
    SET_VARSIZE(result, size);
    result->dim = dim;
    result->elem_size = elem_size;

    return result;
}

/* Confined by uint16 in LDBVec structure */
#define LDB_VEC_MAX_DIM 1 << 16
/*
 * Returns a pointer to the actual array data.
 */
#define LDBVEC_DATA_SIZE(a) (((a->dim)) * (a->elem_size))

/*
 * Returns a pointer to the actual array data.
 */
#define LDBVEC_DATA_PTR(a) (((void *)(a->data)))

#define DatumGetLDBVec(x) ((LDBVec *)PG_DETOAST_DATUM(x))

#endif  // LDB_VEC_H