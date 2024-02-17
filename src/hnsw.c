#include <postgres.h>

#include "hnsw.h"

#include <access/amapi.h>
#include <access/heapam.h>
#include <catalog/namespace.h>
#include <commands/vacuum.h>
#include <float.h>
#include <math.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/selfuncs.h>
#include <utils/spccache.h>

#if PG_VERSION_NUM <= 120000
#include <access/htup_details.h>
#endif

#include "hnsw/build.h"
#include "hnsw/delete.h"
#include "hnsw/failure_point.h"
#include "hnsw/insert.h"
#include "hnsw/options.h"
#include "hnsw/scan.h"
#include "hnsw/utils.h"
#include "hnsw/validate_index.h"
#include "hnsw/vector.h"
#include "usearch.h"

PG_MODULE_MAGIC;

#if PG_VERSION_NUM >= 120000
#include "commands/progress.h"
#endif

/*
 * Get the name of index build phase
 */
#if PG_VERSION_NUM >= 120000
static char *hnswbuildphasename(int64 phasenum)
{
    switch(phasenum) {
        case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
            return "initializing";
        case LDB_PROGRESS_HNSW_PHASE_IN_MEMORY_INSERT:
            return "performing hnsw in-memory insertions";
        case LDB_PROGRESS_HNSW_PHASE_LOAD:
            return "loading tuples into postgres index";
        default:
            return NULL;
    }
}
#endif

/*
 * Helper to compute number of levels we expect to see in our index with N vectors.
 * We could just pull this out of the hnsw header and get rid of this function.
 * However, before doing so, we should benchmark that implementation and compare with
 * this implementation first that computes an expectation.
 *
 * An element will be in level given by random variable`floor(-ln(unif(0, 1)) * mL)`, based on the paper.
 * Every time an element is inserted into the index, we "draw" from this random variable.
 *
 * The Expected Value of this distribtion is mL, as the author says in 4.2.2.
 * I.e. when we "draw" a number, we expect it to be mL.
 *
 * However, this is not what we care about. We care about what happens when we do
 * `num_tuples_in_index` "draw"s from this distribution--the expected maximum of
 * all the draws. This is an order statistic.
 * https://en.wikipedia.org/wiki/Order_statistic
 *
 * In particular, let D be a random variable given by `-ln(unif(0, 1)) * mL`.
 * We care about E[Max_N{D}], where Max_N{D} means Maximum out of N draws from D.
 *
 * Let's strip out the constants and irrelevant transformations.
 * E[Max_N{D}] = -ln(E[Min_N{unif(0,1)}]) * mL
 *
 * So we need to compute E[Min_N{unif(0,1)}]. This is well understood, and based on wiki above
 * is 1/(1+n).
 *
 * -ln(1/(1+n)) * mL = ln(1+n)*mL
 *
 * This is O(log(N)), which is what the author claims the scaling with dateset is in 4.2.1 and 4.2.2.
 */
static uint64 expected_number_of_levels(double num_tuples_in_index, double mL)
{
    return ceil(log(1.0 + num_tuples_in_index) * mL);
}

/*
 * Bound on the expected number of tuples we expect hnsw to visit on a search query.
 */
static uint64 estimate_number_tuples_accessed(Oid index_relation, double num_tuples_in_index)
{
    if(num_tuples_in_index <= 0) return 0;
    int M, ef;
    {  // index_rel scope
        Relation index_rel = relation_open(index_relation, AccessShareLock);
        M = ldb_HnswGetM(index_rel);
        ef = ldb_HnswGetEf(index_rel);
        relation_close(index_rel, AccessShareLock);
    }

    // mL, the level normalization factor, from the paper, Algorithm 1.
    // Section 4.1 on the paper says optimal choice for this value
    // is 1/ln(M). Usearch also follows this.
    const double mL = 1.0 / log(M);
    // S, the expected number of steps in a layer, from the paper.
    const double S = 1.0 / (1.0 - exp(-1.0 * mL));

    const uint64 tuples_visited_per_non_base_level = S * M;
    // the base level has M * 2 neighbors, and we do ef searches,
    // so need to account for both of that here
    const uint64 tuples_visited_for_base_level = ef * S * M * 2;

    // this scales logarithmically based on the number of elements in the index
    const uint64 expected_num_levels = expected_number_of_levels(num_tuples_in_index, mL);

    // note that since num_tuples_in_index > 0, we have expected_number_of_levels >= 1 (so we can't underflow below)
    uint64 total_tuple_visits = tuples_visited_per_non_base_level * (expected_num_levels - 1);
    total_tuple_visits += tuples_visited_for_base_level;

    // `total_tuple_visits` can be larger than the number of tuples in the index
    // if the database doesn't have a lot of tuples in it.
    // in this case, we should still prefer to use the hnsw index over a sequential scan.
    // The "3.0" is arbitrary here.
    return Min(total_tuple_visits, num_tuples_in_index / 3.0);
}

static uint64 estimate_number_blocks_accessed(uint64 num_tuples_in_index, uint64 num_pages, uint64 num_tuples_accessed)
{
    if(num_tuples_in_index == 0 || num_pages == 0 || num_tuples_accessed == 0) {
        return 0;
    }
    const uint64 num_header_pages = 1;
    // TODO: remove blockmap from cost estimation once
    // we switch away from blockmaps.
    const uint64 num_blockmaps_used = ceil(num_tuples_in_index / HNSW_BLOCKMAP_BLOCKS_PER_PAGE);
    const uint64 num_blockmap_allocated = pow(2, floor(log2(num_blockmaps_used)) + 1);
    const uint64 num_datablocks = Max((int64)num_pages - 1 - (int64)num_blockmap_allocated, 1);

    const uint64 num_datablocks_accessed = ((double)num_tuples_accessed / (double)num_tuples_in_index) * num_datablocks;
    const uint64 num_blockmaps_accessed
        = ((double)num_datablocks_accessed / (double)num_datablocks) * num_blockmaps_used;
    const uint64 num_block_accesses = num_header_pages + num_datablocks_accessed + num_blockmaps_accessed;
    return num_block_accesses;
}

/*
 * Estimate the cost of an index scan
 */
static void hnswcostestimate(PlannerInfo *root,
                             IndexPath   *path,
                             double       loop_count,
                             Cost        *indexStartupCost,
                             Cost        *indexTotalCost,
                             Selectivity *indexSelectivity,
                             double      *indexCorrelation,
                             double      *indexPages)
{
    GenericCosts costs;
#if PG_VERSION_NUM < 120000
    List *qinfos;
#endif

    /* Never use index without order */
    if(path->indexorderbys == NULL) {
        *indexStartupCost = DBL_MAX;
        *indexTotalCost = DBL_MAX;
        *indexSelectivity = 0;
        *indexCorrelation = 0;
        *indexPages = 0;
        return;
    }
    /* ALWAYS use index when asked*/
    MemSet(&costs, 0, sizeof(costs));

    double num_tuples_in_index = path->indexinfo->tuples;
    costs.numIndexTuples = estimate_number_tuples_accessed(path->indexinfo->indexoid, num_tuples_in_index);
    uint64 num_blocks_accessed
        = estimate_number_blocks_accessed(num_tuples_in_index, path->indexinfo->pages, costs.numIndexTuples);

#if PG_VERSION_NUM >= 120000
    genericcostestimate(root, path, loop_count, &costs);
#else
    qinfos = deconstruct_indexquals(path);
    genericcostestimate(root, path, loop_count, qinfos, &costs);
#endif

    *indexStartupCost = 0;
    *indexTotalCost = costs.numIndexPages ? costs.indexTotalCost * (num_blocks_accessed / costs.numIndexPages) : 0;
    // indexSelectivity is the fraction of all rows in the table that our index is expected to return
    // (https://www.postgresql.org/docs/current/index-cost-estimation.html) We are an order-by only index, and so we
    // return all of the rows in our index. So, this is just the fraction of all rows in the table that is in the index
    // (recall that partial indexes can exclude rows from the table), and this is what genericcostestimate computes
    // above
    *indexSelectivity = costs.indexSelectivity;

    // since we try to insert index tuples at the last datablock,
    // there is no "order" at all that can be assumed.
    *indexCorrelation = 0;
    *indexPages = num_blocks_accessed;

    ldb_dlog("LANTERN - Query cost estimator");
    ldb_dlog("LANTERN - ---------------------");
    ldb_dlog("LANTERN - Total cost: %lf", *indexTotalCost);
    ldb_dlog("LANTERN - Selectivity: %lf", *indexSelectivity);
    ldb_dlog("LANTERN - Num pages: %lf", *indexPages);
    ldb_dlog("LANTERN - Num tuples: %lf", costs.numIndexTuples);
    ldb_dlog("LANTERN - ---------------------");
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool hnswvalidate(Oid opclassoid)
{
    LDB_UNUSED(opclassoid);
    return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_handler);
Datum       hnsw_handler(PG_FUNCTION_ARGS __attribute__((unused)))
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = 4;
#if PG_VERSION_NUM >= 130000
    amroutine->amoptsprocnum = 0;
#endif
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = false;
    /**
     * todo:: review!!
     * Seems to indicate that amoptionalkey must be false
     * https://www.postgresql.org/docs/current/index-api.html
     *
     * However, this argument fails when an index scan has no restriction clause
     * for a given index column. In practice this means that indexes that have
     * amoptionalkey true must index nulls, since the planner might
     * decide to use such an index with no scan keys at all
     */
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
#if PG_VERSION_NUM >= 130000
    amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
    amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
#endif
    amroutine->amkeytype = InvalidOid;
    /* Interface functions */
    amroutine->ambuild = ldb_ambuild;
    amroutine->ambuildempty = ldb_ambuildunlogged;
    amroutine->aminsert = ldb_aminsert;
    amroutine->ambulkdelete = ldb_ambulkdelete;
    amroutine->amvacuumcleanup = ldb_amvacuumcleanup;
    amroutine->amcanreturn = NULL;
    amroutine->amcostestimate = hnswcostestimate;
    amroutine->amoptions = ldb_amoptions;
    amroutine->amproperty = NULL;
#if PG_VERSION_NUM >= 120000
    amroutine->ambuildphasename = hnswbuildphasename;
#endif
    amroutine->amvalidate = hnswvalidate;
#if PG_VERSION_NUM >= 140000
    amroutine->amadjustmembers = NULL;
#endif
    amroutine->ambeginscan = ldb_ambeginscan;
    amroutine->amrescan = ldb_amrescan;
    amroutine->amgettuple = ldb_amgettuple;
    amroutine->amgetbitmap = NULL;
    amroutine->amendscan = ldb_amendscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;

    /* Interface functions to support parallel index scans */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}

static float4 array_dist(ArrayType *a, ArrayType *b, usearch_metric_kind_t metric_kind)
{
    int a_dim = ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a));
    int b_dim = ArrayGetNItems(ARR_NDIM(b), ARR_DIMS(b));

    if(a_dim != b_dim) {
        elog(ERROR, "expected equally sized arrays but got arrays with dimensions %d and %d", a_dim, b_dim);
    }

    float4          result;
    usearch_error_t error = NULL;

    if(metric_kind == usearch_metric_hamming_k) {
        // when computing hamming distance, array element type must be an integer type
        if(ARR_ELEMTYPE(a) != INT4OID || ARR_ELEMTYPE(b) != INT4OID) {
            elog(ERROR, "expected integer array but got array with element type %d", ARR_ELEMTYPE(a));
        }
        int32 *ax_int = (int32 *)ARR_DATA_PTR(a);
        int32 *bx_int = (int32 *)ARR_DATA_PTR(b);

        // calling usearch_scalar_f32_k here even though it's an integer array is fine
        // the hamming distance in usearch actually ignores the scalar type
        // and it will get casted appropriately in usearch even with this scalar type
        result = usearch_distance(ax_int, bx_int, usearch_scalar_f32_k, a_dim, metric_kind, &error);
        assert(!error);
    } else {
        float4 *ax = ToFloat4Array(a);
        float4 *bx = ToFloat4Array(b);

        result = usearch_distance(ax, bx, usearch_scalar_f32_k, a_dim, metric_kind, &error);
        assert(!error);
    }

    return result;
}

static float8 vector_dist(Vector *a, Vector *b, usearch_metric_kind_t metric_kind)
{
    usearch_error_t error = NULL;
    if(a->dim != b->dim) {
        elog(ERROR, "expected equally sized vectors but got vectors with dimensions %d and %d", a->dim, b->dim);
    }

    float8 dist = usearch_distance(a->x, b->x, usearch_scalar_f32_k, a->dim, metric_kind, &error);
    if(error) {
        elog(ERROR, "unexpected distance metric error: %s", error);
    }
    return dist;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_generic_dist);
Datum       ldb_generic_dist(PG_FUNCTION_ARGS)
{
    if(ldb_pgvector_compat) {
        elog(ERROR, "Operator can only be used when lantern.pgvector_compat=FALSE");
    }
    PG_RETURN_NULL();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(l2sq_dist);
Datum       l2sq_dist(PG_FUNCTION_ARGS)
{
    ArrayType *a = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *b = PG_GETARG_ARRAYTYPE_P(1);
    PG_RETURN_FLOAT4(array_dist(a, b, usearch_metric_l2sq_k));
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(cos_dist);
Datum       cos_dist(PG_FUNCTION_ARGS)
{
    ArrayType *a = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *b = PG_GETARG_ARRAYTYPE_P(1);
    PG_RETURN_FLOAT4(array_dist(a, b, usearch_metric_cos_k));
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hamming_dist);
Datum       hamming_dist(PG_FUNCTION_ARGS)
{
    ArrayType *a = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *b = PG_GETARG_ARRAYTYPE_P(1);
    PG_RETURN_INT32((int32)array_dist(a, b, usearch_metric_hamming_k));
}

// The guard functions are not used anymore
// They are left for updates from <0.0.9 to >0.0.9 to work
// As in update 0.0.9 it will try to create _guard functions
// And will fail if the corresponding functions will not exist in C
// This can happen for example when updating from v0.0.8 to v0.0.10
PGDLLEXPORT PG_FUNCTION_INFO_V1(hamming_dist_with_guard);
Datum       hamming_dist_with_guard(PG_FUNCTION_ARGS) { PG_RETURN_NULL(); }

PGDLLEXPORT PG_FUNCTION_INFO_V1(cos_dist_with_guard);
Datum       cos_dist_with_guard(PG_FUNCTION_ARGS) { PG_RETURN_NULL(); }

PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_l2sq_dist);
Datum       vector_l2sq_dist(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_FLOAT8((double)vector_dist(a, b, usearch_metric_l2sq_k));
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_cos_dist);
Datum       vector_cos_dist(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_FLOAT8((double)vector_dist(a, b, usearch_metric_cos_k));
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(lantern_internal_validate_index);
Datum       lantern_internal_validate_index(PG_FUNCTION_ARGS)
{
    Oid  indrelid = PG_GETARG_OID(0);
    bool print_info = PG_GETARG_BOOL(1);

    ldb_validate_index(indrelid, print_info);
    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(lantern_internal_failure_point_enable);
Datum       lantern_internal_failure_point_enable(PG_FUNCTION_ARGS)
{
    const char *func = text_to_cstring(PG_GETARG_TEXT_PP(0));
    const char *name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    uint32      dont_trigger_first_nr = PG_GETARG_UINT32(2);

    ldb_failure_point_enable(func, name, dont_trigger_first_nr);
    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(lantern_internal_continue_blockmap_group_initialization);
Datum       lantern_internal_continue_blockmap_group_initialization(PG_FUNCTION_ARGS)
{
    Oid indrelid = PG_GETARG_OID(0);

    ldb_continue_blockmap_group_initialization(indrelid);
    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(lantern_reindex_external_index);
Datum       lantern_reindex_external_index(PG_FUNCTION_ARGS)
{
    Oid indrelid = PG_GETARG_OID(0);
    ldb_reindex_external_index(indrelid);
    PG_RETURN_VOID();
}

/*
 * Get data type for give oid
 * */
HnswColumnType GetColumnTypeFromOid(Oid oid)
{
    ldb_invariant(OidIsValid(oid), "Invalid oid passed");

    if(oid == FLOAT4ARRAYOID) {
        return REAL_ARRAY;
    } else if(oid == TypenameGetTypid("vector")) {
        return VECTOR;
    } else if(oid == INT4ARRAYOID) {
        return INT_ARRAY;
    } else {
        return UNKNOWN;
    }
}

/*
 * Get data type of index
 */
HnswColumnType GetIndexColumnType(Relation index)
{
    TupleDesc         indexTupDesc = RelationGetDescr(index);
    Form_pg_attribute attr = TupleDescAttr(indexTupDesc, 0);
    return GetColumnTypeFromOid(attr->atttypid);
}

/*
 * Given vector data and vector type, read it as either a float4 or int32 array and return as void*
 */
void *DatumGetSizedArray(Datum datum, HnswColumnType type, int dimensions)
{
    if(type == VECTOR) {
        Vector *vector = DatumGetVector(datum);
        if(vector->dim != dimensions) {
            elog(ERROR, "Expected vector with dimension %d, got %d", dimensions, vector->dim);
        }
        return (void *)vector->x;
    } else if(type == REAL_ARRAY) {
        ArrayType *array = DatumGetArrayTypePCopy(datum);
        int        array_dim = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
        if(array_dim != dimensions) {
            elog(ERROR, "Expected real array with dimension %d, got %d", dimensions, array_dim);
        }
        return (void *)((float4 *)ARR_DATA_PTR(array));
    } else if(type == INT_ARRAY) {
        ArrayType *array = DatumGetArrayTypePCopy(datum);
        int        array_dim = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
        if(array_dim != dimensions) {
            elog(ERROR, "Expected int array with dimension %d, got %d", dimensions, array_dim);
        }

        int32 *intArray = (int32 *)ARR_DATA_PTR(array);
        return (void *)intArray;
    } else {
        elog(ERROR, "Unsupported type");
    }
}
