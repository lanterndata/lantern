#ifndef LDB_HNSW_H
#define LDB_HNSW_H

#include <postgres.h>

#include <assert.h>
#include <fmgr.h>
#include <storage/itemptr.h>
#include <utils/relcache.h>

#include "usearch.h"

#define LANTERN_INTERNAL_SCHEMA_NAME "_lantern_internal"

#if PG_VERSION_NUM < 110000
#error "Requires PostgreSQL 11+"
#endif

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define LDB_PROGRESS_HNSW_PHASE_IN_MEMORY_INSERT 2
#define LDB_PROGRESS_HNSW_PHASE_LOAD             3

typedef enum
{
    REAL_ARRAY,
    INT_ARRAY,
    VECTOR,
    UNKNOWN
} HnswColumnType;

// compilers warn about potential UB when members of this struct are accessed directly
// though the struct is always accessed via memcpy so the warning does not apply
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpacked-not-aligned"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpacked-not-aligned"

static const usearch_label_t INVALID_ELEMENT_LABEL = 0;
// C version of uint48_t in c++/usearch
typedef union __attribute__((__packed__))
{
    ItemPointerData itemPointerData;
    uint32          seqid;
} ldb_unaligned_slot_union_t;

// the slot must fit in a usearch label
static_assert(sizeof(ldb_unaligned_slot_union_t) <= sizeof(usearch_label_t), "index label too small for lantern slot");

#pragma clang diagnostic pop

#pragma GCC diagnostic pop

static_assert(sizeof(ldb_unaligned_slot_union_t) >= sizeof(ItemPointerData),
              "ldb_unaligned_slot_union_t must be large enough for ItemPointerData");

/* Exported functions */
PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void _PG_fini(void);

PGDLLEXPORT Datum l2sq_dist(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum vector_l2sq_dist(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum hamming_dist(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum hamming_dist_with_guard(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum cos_dist(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum cos_dist_with_guard(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum vector_cos_dist(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum lantern_reindex_external_index(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum lantern_internal_get_binary_version(PG_FUNCTION_ARGS);

HnswColumnType GetColumnTypeFromOid(Oid oid);
HnswColumnType GetIndexColumnType(Relation index);
void*          DatumGetSizedArray(Datum datum, HnswColumnType type, int dimensions, bool copy);

#define LDB_UNUSED(x) (void)(x)

#endif  // LDB_HNSW_H
