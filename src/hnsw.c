#include <postgres.h>

#include "hnsw.h"

#include <access/amapi.h>
#include <access/heapam.h>
#include <catalog/namespace.h>
#include <commands/vacuum.h>
#include <float.h>
#include <math.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/selfuncs.h>
#include <utils/spccache.h>

#include "hnsw/build.h"
#include "hnsw/delete.h"
#include "hnsw/insert.h"
#include "hnsw/options.h"
#include "hnsw/scan.h"
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
        case PROGRESS_HNSW_PHASE_IN_MEMORY_INSERT:
            return "performing hnsw in-memory insertions";
        case PROGRESS_HNSW_PHASE_LOAD:
            return "loading tuples into postgres index";
        default:
            return NULL;
    }
}
#endif

/*
 * An element will given a level by random variable`floor(-ln(unif(0, 1)) * mL)`, based on the paper.
 * Link to paper: https://arxiv.org/pdf/1603.09320.pdf
 * 
 * Let the N draws be X_1, ..., X_N where each X_i is unif(0, 1). Let X = min(X_1, ..., X_N).
 * The total number of levels is floor(-ln(X) * mL) + 1. We want E[floor(-ln(X) * mL) + 1]. 
 * We will estimate E[-ln(X) * mL + 1]. It can be easily verified that 
 * E[-ln(X) * mL + 1] - E[floor(-ln(X) * mL) + 1] < 1. Also, we will use n = N.
 * 
 * Let Y = ln(X). Then P[Y < z] = P[X < e^z] = 1 - (1 - e^z)^n. 
 * To see this, note that (1 - x)^n is the probability all n draws are greater than x.
 * Then, using the fact that E[X] = int_0^inf (1 - P[X < z]) dz - int_-inf^0 P[X < z] dz, (*)
 * we get E[Y] = -1 * int_-inf^0 (1 - (1 - e^z)^n) dz. We can find equation (*) in the 
 * Properties section of this page: https://en.wikipedia.org/wiki/Expected_value
 * 
 * Substituting y = e^z, we get int_-inf^0 (1 - (1 - e^z)^n) dz = int_0^1 (1 - (1 - y)^n)/y dy.
 * Substituting t = 1-y, we get int_0^1 (1 - (1 - y)^n)/y dy = int_0^1 (1 - t^n)/(1 - t) dt.
 * Factoring, we get int_0^1 (1 - t^n)/(1 - t) dt = int_0^1 1 + t + t^2 + ... + t^(n-1) dt
 * which is equal to 1 + 1/2 + ... + 1/n = H(n). 
 * 
 * A well-known approximation for H(n) is ln(n) + 1/2n + 1/12n^2 + g where g = 0.5772156649 is the Euler-Mascheroni constant.
 * See here: https://math.stackexchange.com/questions/496116/is-there-a-partial-sum-formula-for-the-harmonic-series 
 * 
 * We approximate E[-ln(X) * mL + 1] = (ln(n) + 1/2n + 1/12n^2 + g) * mL + 1.
 *
 * This takes O(log(N)) to calculate, which is what the author claims the scaling with dateset is in 4.2.1 and 4.2.2.
 */
static uint64 estimate_expected_number_of_levels(double num_tuples_in_index, double mL)
{
    const double g = 0.5772156649;
    return floor((log(num_tuples_in_index) + 
                  1.0 / (2.0 * num_tuples_in_index) + 
                  1.0 / (12.0 * num_tuples_in_index * num_tuples_in_index) + g) * mL) + 1;
}

/*
 * Bound on the expected number of tuples we expect hnsw to visit on a search query.
 */
static uint64 estimate_number_tuples_accessed(Oid index_relation, double num_tuples_in_index)
{
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
    const uint64 expected_number_of_levels = estimate_expected_number_of_levels(num_tuples_in_index, mL);

    uint64 total_tuple_visits = tuples_visited_per_non_base_level * (expected_number_of_levels - 1);
    total_tuple_visits += expected_number_of_levels > 0 ? tuples_visited_for_base_level : 0;

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
    const uint64 num_datablocks = Max(num_pages - 1 - num_blockmap_allocated, 1);

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

    double num_tuples_in_index = path->indexinfo->rel->tuples;
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
    *indexSelectivity = 1.0;
    // since we try to insert index tuples at the last datablock,
    // there is no "order" at all that can be assumed.
    *indexCorrelation = 0;
    *indexPages = num_blocks_accessed;

    elog(DEBUG5, "LANTERN - Query cost estimator");
    elog(DEBUG5, "LANTERN - ---------------------");
    elog(DEBUG5, "LANTERN - Total cost: %lf", *indexTotalCost);
    elog(DEBUG5, "LANTERN - Selectivity: %lf", *indexSelectivity);
    elog(DEBUG5, "LANTERN - Num pages: %lf", *indexPages);
    elog(DEBUG5, "LANTERN - Num tuples: %lf", costs.numIndexTuples);
    elog(DEBUG5, "LANTERN - ---------------------");
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

    float4 *ax = (float4 *)ARR_DATA_PTR(a);
    float4 *bx = (float4 *)ARR_DATA_PTR(b);

    return usearch_dist(ax, bx, metric_kind, a_dim, usearch_scalar_f32_k);
}

static float8 vector_dist(Vector *a, Vector *b, usearch_metric_kind_t metric_kind)
{
    if(a->dim != b->dim) {
        elog(ERROR, "expected equally sized vectors but got vectors with dimensions %d and %d", a->dim, b->dim);
    }

    return usearch_dist(a->x, b->x, metric_kind, a->dim, usearch_scalar_f32_k);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ldb_generic_dist);
Datum       ldb_generic_dist(PG_FUNCTION_ARGS) { PG_RETURN_NULL(); }

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
    PG_RETURN_INT32(array_dist(a, b, usearch_metric_hamming_k));
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_l2sq_dist);
Datum       vector_l2sq_dist(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_FLOAT8((double)vector_dist(a, b, usearch_metric_l2sq_k));
}

/*
 * Get data type of index
 */
HnswColumnType GetIndexColumnType(Relation index)
{
    TupleDesc         indexTupDesc = RelationGetDescr(index);
    Form_pg_attribute attr = TupleDescAttr(indexTupDesc, 0);
    Oid               columnType = attr->atttypid;

    if(columnType == FLOAT4ARRAYOID) {
        return REAL_ARRAY;
    } else if(columnType == TypenameGetTypid("vector")) {
        return VECTOR;
    } else if(columnType == INT4ARRAYOID) {
        return INT_ARRAY;
    } else {
        return UNKNOWN;
    }
}

/*
 * Given vector data and vector type, convert it to a float4 array
 */
float4 *DatumGetSizedFloatArray(Datum datum, HnswColumnType type, int dimensions)
{
    if(type == VECTOR) {
        Vector *vector = DatumGetVector(datum);
        if(vector->dim != dimensions) {
            elog(ERROR, "Expected vector with dimension %d, got %d", dimensions, vector->dim);
        }
        return vector->x;
    } else if(type == REAL_ARRAY) {
        ArrayType *array = DatumGetArrayTypePCopy(datum);
        int        array_dim = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
        if(array_dim != dimensions) {
            elog(ERROR, "Expected real array with dimension %d, got %d", dimensions, array_dim);
        }
        return (float4 *)ARR_DATA_PTR(array);
    } else if(type == INT_ARRAY) {
        ArrayType *array = DatumGetArrayTypePCopy(datum);
        int        array_dim = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
        if(array_dim != dimensions) {
            elog(ERROR, "Expected int array with dimension %d, got %d", dimensions, array_dim);
        }
        int    *intArray = (int *)ARR_DATA_PTR(array);
        float4 *floatArray = (float4 *)palloc(sizeof(float) * array_dim);
        for(int i = 0; i < array_dim; i++) {
            floatArray[ i ] = (float)intArray[ i ];
        }
        // todo:: free this array
        return floatArray;
    } else {
        elog(ERROR, "Unsupported type");
    }
}
