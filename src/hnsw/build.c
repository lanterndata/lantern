#include <postgres.h>

#include "build.h"

#include <access/heapam.h>
#include <assert.h>
#include <catalog/index.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <storage/bufmgr.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>

#include "bench.h"
#include "external_index.h"
#include "hnsw.h"
#include "options.h"
#include "utils.h"
#include "vector.h"

#if PG_VERSION_NUM >= 140000
#include <utils/backend_progress.h>
#elif PG_VERSION_NUM >= 120000
#include <pgstat.h>
#endif

#if PG_VERSION_NUM <= 120000
#include <access/htup_details.h>
#endif

#if PG_VERSION_NUM >= 120000
#include <access/tableam.h>
#include <commands/progress.h>
#else
#define PROGRESS_CREATEIDX_SUBPHASE     0
#define PROGRESS_CREATEIDX_TUPLES_TOTAL 0
#define PROGRESS_CREATEIDX_TUPLES_DONE  0
#endif

#if PG_VERSION_NUM >= 130000
#define CALLBACK_ITEM_POINTER ItemPointer tid
#else
#define CALLBACK_ITEM_POINTER HeapTuple hup
#endif

#if PG_VERSION_NUM >= 120000
#define UpdateProgress(index, val) pgstat_progress_update_param(index, val)
#else
#define UpdateProgress(index, val) ((void)val)
#endif

static void AddTupleToUsearchIndex(ItemPointer tid, Datum *values, HnswBuildState *buildstate)
{
    /* Detoast once for all calls */
    usearch_error_t       error = NULL;
    Datum                 value = PointerGetDatum(PG_DETOAST_DATUM(values[ 0 ]));
    usearch_scalar_kind_t usearch_scalar;
    float4               *vector = DatumGetSizedFloatArray(value, buildstate->columnType, buildstate->dimensions);

    switch(buildstate->columnType) {
        case REAL_ARRAY:
        case VECTOR:
            usearch_scalar = usearch_scalar_f32_k;
            break;
        case INT_ARRAY:
            // q:: I think in this case we need to do a type conversion from int to float
            // before passing the buffer to usearch
            usearch_scalar = usearch_scalar_f32_k;
            break;
        default:
            pg_unreachable();
    }

        // casting tid structure to a number to be used as value in vector search
        // tid has info about disk location of this item and is 6 bytes long
#ifdef LANTERN_USE_LIBHNSW
    if(buildstate->hnsw != NULL) hnsw_add(buildstate->hnsw, vector, *(unsigned long *)tid);
#endif
#ifdef LANTERN_USE_USEARCH
    if(buildstate->usearch_index != NULL)
        usearch_add(buildstate->usearch_index, *(unsigned long *)tid, vector, usearch_scalar, &error);
#endif
    assert(error == NULL);
    buildstate->tuples_indexed++;
    buildstate->reltuples++;
    UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, buildstate->tuples_indexed);
    UpdateProgress(PROGRESS_CREATEIDX_TUPLES_TOTAL, buildstate->reltuples);
}

/*
 * Callback for table_index_build_scan
 */
static void BuildCallback(
    Relation index, CALLBACK_ITEM_POINTER, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    HnswBuildState *buildstate = (HnswBuildState *)state;
    MemoryContext   oldCtx;
    // we can later use this for some optimizations I think
    LDB_UNUSED(tupleIsAlive);

#if PG_VERSION_NUM < 130000
    ItemPointer tid = &hup->t_self;
#endif

    /* Skip nulls */
    if(isnull[ 0 ]) return;

    CheckHnswIndexDimensions(index, values[ 0 ], buildstate->dimensions);

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    // todo:: the argument values is assumed to be a real[] or vector (they have the same layout)
    // do proper type checking instead of this assumption and test int int arrays and others
    AddTupleToUsearchIndex(tid, values, buildstate);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

static int GetArrayLengthFromHeap(Relation heap, int indexCol)
{
#if PG_VERSION_NUM < 120000
    HeapScanDesc scan;
#else
    TableScanDesc scan;
#endif
    HeapTuple  tuple;
    Snapshot   snapshot;
    ArrayType *array;
    Datum      datum;
    bool       isNull;
    int        n_items = HNSW_DEFAULT_DIMS;
    //
    // Get the first row off the heap
    // if it's NULL we don't infer a length. Since vectors are expected to have fixed nonzero dimension this will result
    // in an error later
    snapshot = GetTransactionSnapshot();
#if PG_VERSION_NUM < 120000
    scan = heap_beginscan(heap, snapshot, 0, NULL);
#else
    scan = heap_beginscan(heap, snapshot, 0, NULL, NULL, SO_TYPE_SEQSCAN);
#endif
    tuple = heap_getnext(scan, ForwardScanDirection);
    if(tuple == NULL) {
        heap_endscan(scan);
        return n_items;
    }

    // Get the indexed column out of the row and return it's dimensions
    datum = heap_getattr(tuple, indexCol, RelationGetDescr(heap), &isNull);
    if(!isNull) {
        array = DatumGetArrayTypePCopy(datum);
        n_items = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    }

    heap_endscan(scan);

    return n_items;
}

int GetHnswIndexDimensions(Relation index)
{
    HnswColumnType columnType = GetIndexColumnType(index);

    // check if column is type of real[] or integer[]
    if(columnType == REAL_ARRAY || columnType == INT_ARRAY) {
        // The dimension in options is not set if the dimension is inferred so we need to actually check the key
        int opt_dim = HnswGetDims(index);
        if(opt_dim == HNSW_DEFAULT_DIMS) {
            // If the option's still the default it needs to be updated to match what was inferred
            // todo: is there a way to do this earlier? (rd_options is null in BuildInit)
            Relation     heap;
            HnswOptions *opts;
            int          attrNum;

            // We know there's one key because we generate an error during inference if multiple keys are specified
            attrNum = index->rd_index->indkey.values[ 0 ];
            heap = table_open(index->rd_index->indrelid, AccessShareLock);
            opt_dim = GetArrayLengthFromHeap(heap, attrNum);
            opts = (HnswOptions *)index->rd_options;
            if(opts != NULL) {
                opts->dims = opt_dim;
            }
            table_close(heap, AccessShareLock);
        }
        return opt_dim;
    } else if(columnType == VECTOR) {
        return TupleDescAttr(index->rd_att, 0)->atttypmod;
    } else {
        elog(ERROR,
             "Unsupported type"
             " LanternDB currently supports only real[] and vector types");
    }

    return -1;
}

void CheckHnswIndexDimensions(Relation index, Datum arrayDatum, int dimensions)
{
    ArrayType   *array;
    int          n_items;
    HnswColumnType indexType = GetIndexColumnType(index);

    if(indexType == REAL_ARRAY || indexType == INT_ARRAY) {
        /* Check dimensions of vector */
        array = DatumGetArrayTypePCopy(arrayDatum);
        n_items = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
        if(n_items != dimensions) {
            elog(ERROR, "Wrong number of dimensions: %d instead of %d expected", n_items, dimensions);
        }
    }
}

/*
 * Infer the dimensionality of the index from the heap
 */
static int InferDimension(Relation heap, IndexInfo *indexInfo)
{
    int indexCol;

    // If NumIndexAttrs isn't 1 the index has been instantiated on multiple columns and there's no clear way to infer
    // the dim
    if(indexInfo->ii_NumIndexAttrs != 1) {
        return HNSW_DEFAULT_DIMS;
    }

    indexCol = indexInfo->ii_IndexAttrNumbers[ 0 ];
    return GetArrayLengthFromHeap(heap, indexCol);
}

/*
 * Initialize the build state
 */
static void InitBuildState(HnswBuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->columnType = GetIndexColumnType(index);
    buildstate->dimensions = GetHnswIndexDimensions(index);

    // If a dimension wasn't specified try to infer it
    if(buildstate->columnType == REAL_ARRAY || buildstate->columnType == INT_ARRAY)
        if(buildstate->dimensions < 1) buildstate->dimensions = InferDimension(heap, indexInfo);
    /* Require column to have dimensions to be indexed */
    if(buildstate->dimensions < 1) elog(ERROR, "column does not have dimensions");

    // not supported because of 8K page limit in postgres WAL pages
    // can pass this limit once quantization is supported
    if(buildstate->dimensions > HNSW_MAX_DIMS)
        elog(ERROR,
             "vector dimension %d is too large. "
             "LanternDB currently supports up to %ddim vectors",
             buildstate->dimensions,
             HNSW_MAX_DIMS);

    // keeps track of number of tuples added to index
    buildstate->tuples_indexed = 0;
    // number of tuples in the relation to be indexed
    buildstate->reltuples = 0;

    // todo:: add additional possible distance functions to be used in the index
    //  use index_getprocinfo to get the user defined distance functions from relevant index op-class
    //  when added, read the distance function info here once and store it on buildstate for scan to use

    buildstate->tmpCtx
        = AllocSetContextCreate(CurrentMemoryContext, "hnsw build temporary context", ALLOCSET_DEFAULT_SIZES);
}

/*
 * Free resources
 */
static void FreeBuildState(HnswBuildState *buildstate)
{
    // todo:: in debug/or stats mode collect stats from the tmpCtx before deleting it
    MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Scan table for tuples to index
 */
static void ScanTable(HnswBuildState *buildstate)
{
#if PG_VERSION_NUM >= 120000
    buildstate->reltuples = table_index_build_scan(buildstate->heap,
                                                   buildstate->index,
                                                   buildstate->indexInfo,
                                                   true,
                                                   true,
                                                   BuildCallback,
                                                   (void *)buildstate,
                                                   NULL);
#else
    buildstate->reltuples = IndexBuildHeapScan(
        buildstate->heap, buildstate->index, buildstate->indexInfo, true, BuildCallback, (void *)buildstate, NULL);
#endif
}

/*
 * Build the index
 */
static void BuildIndex(
    Relation heap, Relation index, IndexInfo *indexInfo, HnswBuildState *buildstate, ForkNumber forkNum)
{
    usearch_init_options_t opts;
    MemSet(&opts, 0, sizeof(opts));
    InitBuildState(buildstate, heap, index, indexInfo);

    opts.dimensions = buildstate->dimensions;
    PopulateUsearchOpts(index, &opts);

    usearch_error_t error = NULL;
    buildstate->hnsw = NULL;
    buildstate->usearch_index = usearch_init(&opts, &error);

    elog(INFO, "done init usearch index");
    assert(error == NULL);

    usearch_reserve(buildstate->usearch_index, 1100000, &error);
    assert(error == NULL);

    UpdateProgress(PROGRESS_CREATEIDX_PHASE, PROGRESS_HNSW_PHASE_IN_MEMORY_INSERT);
    LanternBench("build hnsw index", ScanTable(buildstate));

    elog(INFO, "inserted %ld elements", usearch_size(buildstate->usearch_index, &error));
    assert(error == NULL);

    char *result_buf = NULL;
    usearch_save(buildstate->usearch_index, NULL, &result_buf, &error);
    assert(error == NULL && result_buf != NULL);

    size_t num_added_vectors = usearch_size(buildstate->usearch_index, &error);
    assert(error == NULL);

    elog(INFO, "done saving %ld vectors", num_added_vectors);

    //****************************** saving to WAL BEGIN ******************************//
    UpdateProgress(PROGRESS_CREATEIDX_PHASE, PROGRESS_HNSW_PHASE_LOAD);
    StoreExternalIndex(
        index, buildstate->usearch_index, forkNum, result_buf, buildstate->dimensions, num_added_vectors);

    //****************************** saving to WAL END ******************************//

    usearch_free(buildstate->usearch_index, &error);
    free(result_buf);
    assert(error == NULL);
    buildstate->usearch_index = NULL;

    FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *ldb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    HnswBuildState    buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.tuples_indexed;

    return result;
}

/*
 * Build the index for an unlogged table
 */
void ldb_ambuildunlogged(Relation index)
{
    LDB_UNUSED(index);
    // todo::
    elog(ERROR, "hnsw index on unlogged tables is currently not supported");
}
