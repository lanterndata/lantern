#ifndef LDB_VECTOR_CASTS_H
#define LDB_VECTOR_CASTS_H

#include <utils/array.h>
#include <assert.h>

#include "usearch.h"
#include "vec_type.h"

static inline int scalar_size(usearch_scalar_kind_t s)
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

LDBVec *ldb_cast_array_vec(ArrayType *array, int expected_dim, usearch_scalar_kind_t to);
#endif  // LDB_VECTOR_CASTS_H