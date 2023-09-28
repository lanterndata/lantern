#ifndef LDB_HNSW_UTILS_H
#define LDB_HNSW_UTILS_H
#include <access/amapi.h>

#include "options.h"
#include "usearch.h"

void            LogUsearchOptions(usearch_init_options_t *opts);
void            PopulateUsearchOpts(Relation index, usearch_init_options_t *opts);
usearch_label_t GetUsearchLabel(ItemPointer itemPtr);
ItemPointer     GetTidFromLabel(usearch_label_t label);
HeapTuple       GetTupleFromItemPointer(Relation rel, ItemPointer tid);

static inline void ldb_invariant(bool condition, const char *msg, ...)
{
    if(likely(condition)) {
        return;
    }
    // todo:: actually do something with the variable arguments in this case
    elog(WARNING, "LanternDB invariant violation: %s", msg);
    // hoping to throw the error via an assertion, if those are on, before elog(ERROR)-ing as a last resort
    // We prefer Assert() because this function is used in contexts where the stack contains non-POD types
    // in which case elog-s long jumps cause undefined behaviour.
    // if assertions are off, we fall back to elog(ERROR) and hope the user restart the session
    Assert(false);
    elog(ERROR, "LanternDB invariant violation: %s. Please restart your DB session and report this error", msg);
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
