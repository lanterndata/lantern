#include "postgres.h"

#include "hnsw.h"

#include <access/amapi.h>
#include <commands/vacuum.h>
#include <float.h>
#include <utils/guc.h>
#include <utils/selfuncs.h>
#include <utils/spccache.h>

#include "hnsw/build.h"
#include "hnsw/delete.h"
#include "hnsw/distfunc.h"
#include "hnsw/insert.h"
#include "hnsw/options.h"
#include "hnsw/scan.h"

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
    double       ratio;
    double       spc_seq_page_cost;
    Relation     indexRel;
#if PG_VERSION_NUM < 120000
    List *qinfos;
#endif

    elog(INFO, "cost estimate");
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

    elog(INFO, "returning small cost to always use the index");
    *indexStartupCost = 0;  //.4444;
    *indexTotalCost = 0;    //.4444;
    *indexSelectivity = 0;
    *indexCorrelation = 0;
    *indexPages = 0;
    return;

    MemSet(&costs, 0, sizeof(costs));

    indexRel = index_open(path->indexinfo->indexoid, NoLock);
    index_close(indexRel, NoLock);

    /*
     * This gives us the subset of tuples to visit. This value is passed into
     * the generic cost estimator to determine the number of pages to visit
     * during the index scan.
     */
    costs.numIndexTuples = path->indexinfo->tuples * ratio;

#if PG_VERSION_NUM >= 120000
    genericcostestimate(root, path, loop_count, &costs);
#else
    qinfos = deconstruct_indexquals(path);
    genericcostestimate(root, path, loop_count, qinfos, &costs);
#endif

    get_tablespace_page_costs(path->indexinfo->reltablespace, NULL, &spc_seq_page_cost);

    /* Adjust cost if needed since TOAST not included in seq scan cost */
    if(costs.numIndexPages > path->indexinfo->rel->pages && ratio < 0.5) {
        /* Change all page cost from random to sequential */
        costs.indexTotalCost -= costs.numIndexPages * (costs.spc_random_page_cost - spc_seq_page_cost);

        /* Remove cost of extra pages */
        costs.indexTotalCost -= (costs.numIndexPages - path->indexinfo->rel->pages) * spc_seq_page_cost;
    } else {
        /* Change some page cost from random to sequential */
        costs.indexTotalCost -= 0.5 * costs.numIndexPages * (costs.spc_random_page_cost - spc_seq_page_cost);
    }

    /*
     * If the list selectivity is lower than what is returned from the generic
     * cost estimator, use that.
     */
    if(ratio < costs.indexSelectivity) costs.indexSelectivity = ratio;

    /* Use total cost since most work happens before first tuple is returned */
    *indexStartupCost = costs.indexTotalCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool hnswvalidate(Oid opclassoid) { return true; }

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
    amroutine->ambuildempty = ldb_ambuildempty;
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

// dummy handler needed to safely upgrade access method handler
// from embedding_handler to hnsw_handler in 0.0.1 to 0.0.2 transition
PGDLLEXPORT PG_FUNCTION_INFO_V1(embedding_handler);

Datum embedding_handler(PG_FUNCTION_ARGS) { return hnsw_handler(fcinfo); }

PGDLLEXPORT PG_FUNCTION_INFO_V1(l2sq_dist);

static float4 calc_distance(ArrayType *a, ArrayType *b)
{
    int     a_dim = ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a));
    int     b_dim = ArrayGetNItems(ARR_NDIM(b), ARR_DIMS(b));
    float4 *ax = (float4 *)ARR_DATA_PTR(a);
    float4 *bx = (float4 *)ARR_DATA_PTR(b);

    if(a_dim != b_dim) {
        elog(ERROR, "expected equally sized arrays but got arrays with dimensions %d and %d", a_dim, b_dim);
    }

    return l2sq_dist_impl(ax, bx, a_dim);
}

Datum l2sq_dist(PG_FUNCTION_ARGS)
{
    ArrayType *a = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *b = PG_GETARG_ARRAYTYPE_P(1);
    PG_RETURN_FLOAT4(calc_distance(a, b));
}
