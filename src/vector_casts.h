#ifndef LDB_VECTOR_CASTS_H
#define LDB_VECTOR_CASTS_H

#include <assert.h>
#include <utils/array.h>

#include "usearch.h"
#include "vec_type.h"

static inline void CheckVecDimConstraint(int dim, int cast)
{
    if(cast != -1 && dim != cast) {
        elog(ERROR, "invalid cast. vector dim: %d, cast dim:%d", dim, cast);
    }
}

ArrayType *ldb_generic_cast_vec_array(float *array_elems, int dim);
LDBVec    *ldb_generic_cast_array_vec(ArrayType *array, int expected_dim, usearch_scalar_kind_t to);
#endif  // LDB_VECTOR_CASTS_H
