#ifndef LDB_VEC_H
#define LDB_VEC_H
#include <postgres.h>

typedef struct
{
    int32  vl_len_; /* varlena header (do not touch directly!) */
    uint16 dim;     /* number of dimensions */
    uint16 elem_type;
    char   data[ FLEXIBLE_ARRAY_MEMBER ];
} LDBVec;

static inline int VecScalarSize(usearch_scalar_kind_t s)
{
    switch(s) {
        // clang-format off
        case usearch_scalar_f64_k: return 8;
        case usearch_scalar_f32_k: return 4;
        case usearch_scalar_f16_k: return 2;
        case usearch_scalar_f8_k: return 1;
        case usearch_scalar_b1_k: return 1;
            // clang-format on
    }
    assert(false);
}

static inline LDBVec *NewLDBVec(int dim, int elem_type)
{
    LDBVec *result;
    int     size;

    size = sizeof(LDBVec) + dim * VecScalarSize(elem_type);
    result = (LDBVec *)palloc0(size);
    SET_VARSIZE(result, size);
    result->dim = dim;
    result->elem_type = elem_type;

    return result;
}

/* Confined by uint16 in LDBVec structure */
#define LDB_VEC_MAX_DIM ((1 << 16) - 1)
/*
 * Returns a pointer to the actual array data.
 */
#define LDBVEC_DATA_SIZE(a) (((a->dim)) * (VecScalarSize(a->elem_type)))

/*
 * Returns a pointer to the actual array data.
 */
#define LDBVEC_DATA_PTR(a) (((void *)(a->data)))

#define DatumGetLDBVec(x) ((LDBVec *)PG_DETOAST_DATUM(x))

#endif  // LDB_VEC_H