#include <postgres.h>

#include "hnsw.h"

#include <access/amapi.h>
#include <commands/vacuum.h>
#include <float.h>
#include <utils/guc.h>
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

    // todo:: estimate number of leaf tuples visited
    costs.numIndexTuples = 0;

#if PG_VERSION_NUM >= 120000
    genericcostestimate(root, path, loop_count, &costs);
#else
    qinfos = deconstruct_indexquals(path);
    genericcostestimate(root, path, loop_count, qinfos, &costs);
#endif

    *indexStartupCost = 0;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = 0;
    *indexPages = costs.numIndexPages;
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
Datum       hnsw_handler(PG_FUNCTION_ARGS)
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
        elog(ERROR, "expected equally sized vectors but got vecors with dimensions %d and %d", a->dim, b->dim);
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
