#ifndef LDB_HNSW_UTILS_H
#define LDB_HNSW_UTILS_H
#include <access/amapi.h>
#include <assert.h>
#include <utils/array.h>

#include "options.h"
#include "usearch.h"

void                  CheckMem(int limit, Relation index, usearch_index_t uidx, uint32 n_nodes, char *msg);
void                  LogUsearchOptions(usearch_init_options_t *opts);
void                  PopulateUsearchOpts(Relation index, usearch_init_options_t *opts);
usearch_label_t       GetUsearchLabel(ItemPointer itemPtr);
float4               *ToFloat4Array(ArrayType *arr, int *dim_out);
bool                  VersionsMatch();
uint32                EstimateRowCount(Relation heap);
int32                 GetColumnAttributeNumber(Relation rel, const char *columnName);
usearch_metric_kind_t GetMetricKindFromStr(char *metric_kind_str);

inline size_t divide_round_up(size_t num, size_t denominator) {
    return (num + denominator - 1) / denominator;
}

inline size_t GetUsearchBitsPerScalar(usearch_scalar_kind_t scalar_kind) {
    switch (scalar_kind) {
    case usearch_scalar_f64_k: return 64;
    case usearch_scalar_f32_k: return 32;
    case usearch_scalar_f16_k: return 16;
    case usearch_scalar_i8_k: return 8;
    case usearch_scalar_b1_k: return 1;
    default: return 0;
    }
}

inline usearch_scalar_kind_t GetUsearchScalarKindFromIndexMeta(metadata_t meta) {
    return meta.init_options.quantization;
}

// hoping to throw the error via an assertion, if those are on, before elog(ERROR)-ing as a last resort
// We prefer Assert() because this function is used in contexts where the stack contains non-POD types
// in which case elog-s long jumps cause undefined behaviour.
// if assertions are off, we fall back to elog(ERROR) and hope the user restart the session
#define ldb_invariant(condition, msg, ...)                                                   \
    {                                                                                        \
        if(unlikely(false == (condition))) {                                                 \
            elog(WARNING, "LanternDB invariant violation: " msg __VA_OPT__(, ) __VA_ARGS__); \
            assert(false);                                                                   \
            elog(ERROR, "LanternDB invariant violation: " msg __VA_OPT__(, ) __VA_ARGS__);   \
        }                                                                                    \
    }

// When calling elog(LEVEL, ...), even if the logs at LEVEL are disabled and nothing is
// printed, it seems elog always does string interpolation and prepares the argument for printing
// which adds ~10us overhead per call. This becomes significant at high throughput
// search operations.
// For this reason on hot codepaths we should avoid elog all together.
// To print debug or test output on these hot codepaths, use ldb_dlog.
#define ldb_dlog(...)                  \
    do {                               \
        if(unlikely(ldb_is_test)) {    \
            elog(DEBUG5, __VA_ARGS__); \
        }                              \
    } while(0)

#endif  // LDB_HNSW_UTILS_H
