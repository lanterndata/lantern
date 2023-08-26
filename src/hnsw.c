#include <postgres.h>

#include "hnsw.h"

#include <access/amapi.h>
#include <access/xact.h>
#include <c.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits.h>
#include <commands/defrem.h>
#include <commands/event_trigger.h>
#include <commands/tablecmds.h>
#include <commands/vacuum.h>
#include <float.h>
#include <parser/parse_utilcmd.h>
#include <parser/parser.h>
#include <utils/builtins.h>
#include <utils/elog.h>
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

static void create_index_from_file(const char *tablename_str, const char *index_path_str)
{
    // Column name
    const char *columnName = "vector";

    // Use a query string
    char  *queryString;
    size_t buffer_size = strlen("CREATE INDEX on ") + strlen(tablename_str) + strlen(" using hnsw(")
                         + strlen(columnName) + strlen(")") + 1;
    queryString = (char *)palloc(buffer_size);
    snprintf(queryString, buffer_size, "CREATE INDEX on %s using hnsw(%s);", tablename_str, columnName);

    // Define a statement for execution
    IndexStmt *stmt = (IndexStmt *)palloc(sizeof(IndexStmt));

    char tablename[ strlen(tablename_str) ];  // Adjust the size as needed
    strcpy(tablename, tablename_str);         // Copy contents of tablename_str into tablename

    RangeVar rv = {
        .type = T_RangeVar,
        .catalogname = 0x0,
        .schemaname = 0x0,
        .relname = tablename,
        .inh = true,
        .relpersistence = RELKIND_RELATION,
        .alias = 0x0,
        .location = 16,  // location of "small_world" token in "CREATE INDEX on small_world using hnsw(vector)"
    };
    IndexElem ie = {.type = T_IndexElem,
                    .name = "vector",  // column name on which to index
                    .expr = 0x0,
                    .indexcolname = 0x0,
                    .collation = 0x0,
                    .opclass = 0x0,
                    .opclassopts = 0x0,
                    .ordering = SORTBY_DEFAULT,
                    .nulls_ordering = SORTBY_NULLS_DEFAULT};

    stmt->type = T_IndexStmt;
    stmt->relation = &rv;
    stmt->accessMethod = "hnsw";  // name of access method

    stmt->indexParams = list_make1(&ie);

    Node *parsetree = (Node *)(stmt);  // Node *parsetree = pstmt->utilityStmt;
    bool  isTopLevel
        = true;  // TODO understand and fix if needed. utility.c value -> (context == PROCESS_UTILITY_TOPLEVEL);
    bool isCompleteQuery
        = true;  // TODO understand and fix if needed. utility.c value -> (context != PROCESS_UTILITY_SUBCOMMAND);
    bool          needCleanup;
    bool          commandCollected = false;
    ObjectAddress address;
    ObjectAddress secondaryObject = InvalidObjectAddress;

    /* All event trigger calls are done only when isCompleteQuery is true */
    needCleanup = isCompleteQuery && EventTriggerBeginCompleteQuery();

    /* PG_TRY block is to ensure we call EventTriggerEndCompleteQuery */
    PG_TRY();
    {
        if(isCompleteQuery) EventTriggerDDLCommandStart(parsetree);

        IndexStmt *stmt = (IndexStmt *)parsetree;
        Oid        relid;
        LOCKMODE   lockmode;
        bool       is_alter_table;

        if(stmt->concurrent) PreventInTransactionBlock(isTopLevel, "CREATE INDEX CONCURRENTLY");

        /*
         * Look up the relation OID just once, right here at the
         * beginning, so that we don't end up repeating the name
         * lookup later and latching onto a different relation
         * partway through.  To avoid lock upgrade hazards, it's
         * important that we take the strongest lock that will
         * eventually be needed here, so the lockmode calculation
         * needs to match what DefineIndex() does.
         */
        lockmode = stmt->concurrent ? ShareUpdateExclusiveLock : ShareLock;
        relid = RangeVarGetRelidExtended(stmt->relation, lockmode, 0, RangeVarCallbackOwnsRelation, NULL);

        /*
         * CREATE INDEX on partitioned tables (but not regular
         * inherited tables) recurses to partitions, so we must
         * acquire locks early to avoid deadlocks.
         *
         * We also take the opportunity to verify that all
         * partitions are something we can put an index on, to
         * avoid building some indexes only to fail later.
         */
        if(stmt->relation->inh && get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE) {
            ListCell *lc;
            List     *inheritors = NIL;

            inheritors = find_all_inheritors(relid, lockmode, NULL);
            foreach(lc, inheritors) {
                char relkind = get_rel_relkind(lfirst_oid(lc));

                if(relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW && relkind != RELKIND_PARTITIONED_TABLE
                   && relkind != RELKIND_FOREIGN_TABLE)
                    elog(ERROR, "unexpected relkind \"%c\" on partition \"%s\"", relkind, stmt->relation->relname);

                if(relkind == RELKIND_FOREIGN_TABLE && (stmt->unique || stmt->primary))
                    ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                             errmsg("cannot create unique index on partitioned table \"%s\"", stmt->relation->relname),
                             errdetail("Table \"%s\" contains partitions that are foreign tables.",
                                       stmt->relation->relname)));
            }
            list_free(inheritors);
        }

        /*
         * If the IndexStmt is already transformed, it must have
         * come from generateClonedIndexStmt, which in current
         * usage means it came from expandTableLikeClause rather
         * than from original parse analysis.  And that means we
         * must treat it like ALTER TABLE ADD INDEX, not CREATE.
         * (This is a bit grotty, but currently it doesn't seem
         * worth adding a separate bool field for the purpose.)
         */
        is_alter_table = stmt->transformed;

        /* Run parse analysis ... */
        stmt = transformIndexStmt(relid, stmt, queryString);

        /* ... and do it */
        EventTriggerAlterTableStart(parsetree);
        address = DefineIndex(relid, /* OID of heap relation */
                              stmt,
                              InvalidOid, /* no predefined OID */
                              InvalidOid, /* no parent index */
                              InvalidOid, /* no parent constraint */
                              is_alter_table,
                              true,   /* check_rights */
                              true,   /* check_not_in_use */
                              true,   /* skip_build : THIS IS A NOTABLE CHANGE */
                              false); /* quiet */

        /*
         * Add the CREATE INDEX node itself to stash right away;
         * if there were any commands stashed in the ALTER TABLE
         * code, we need them to appear after this one.
         */
        EventTriggerCollectSimpleCommand(address, secondaryObject, parsetree);

        EventTriggerAlterTableEnd();

        if(isCompleteQuery) {
            EventTriggerSQLDrop(parsetree);
            EventTriggerDDLCommandEnd(parsetree);
        }
    }
    PG_FINALLY();
    {
        if(needCleanup) EventTriggerEndCompleteQuery();
    }
    PG_END_TRY();

    pfree(queryString);
    pfree(stmt);


    // TODO : Run the ldb_ambuild from file equivalent after this
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

PGDLLEXPORT PG_FUNCTION_INFO_V1(index_from_external);
Datum       index_from_external(PG_FUNCTION_ARGS)
{
    text *tablename_text = PG_GETARG_TEXT_P(0);
    text *index_path_text = PG_GETARG_TEXT_P(1);

    char *tablename_str = text_to_cstring(tablename_text);
    char *index_path_str = text_to_cstring(index_path_text);

    create_index_from_file(tablename_str, index_path_str);

    PG_RETURN_VOID();
}