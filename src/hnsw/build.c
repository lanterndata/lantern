#include "postgres.h"

#include <assert.h>
#include <catalog/index.h>
#include <storage/bufmgr.h>
#include <sys/mman.h>  // `mmap`
#include <sys/stat.h>  // `fstat` for file size
#include <unistd.h>    // `open`, `close`
#include <utils/memutils.h>

#include "bench.h"
#include "hnsw.h"
#include "external_index.h"
#include "usearch.h"
#include "utils.h"
#include "bench.h"
#include "vector.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#elif PG_VERSION_NUM >= 120000
#include "pgstat.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#include "commands/progress.h"
#else
#define PROGRESS_CREATEIDX_SUBPHASE     0
#define PROGRESS_CREATEIDX_TUPLES_TOTAL 0
#define PROGRESS_CREATEIDX_TUPLES_DONE  0
#endif

#include "build.h"
#include "options.h"

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

static void AddTupleToIndex(Relation index, ItemPointer tid, Datum *values, HnswBuildState *buildstate)
{
    /* Detoast once for all calls */
    usearch_error_t error = NULL;
    Datum           value = PointerGetDatum(PG_DETOAST_DATUM(values[ 0 ]));

    // casting tid structure to a number to be used as value in vector search
    // tid has info about disk location of this item and is 6 bytes long
#ifdef LANTERN_USE_LIBHNSW
    if(buildstate->hnsw != NULL) hnsw_add(buildstate->hnsw, DatumGetVector(value)->x, *(unsigned long *)tid);
#endif
#ifdef LANTERN_USE_USEARCH
    if(buildstate->usearch_index != NULL)
        usearch_add(
            buildstate->usearch_index, *(unsigned long *)tid, DatumGetVector(value)->x, usearch_scalar_f32_k, &error);
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

#if PG_VERSION_NUM < 130000
    ItemPointer tid = &hup->t_self;
#endif

    /* Skip nulls */
    if(isnull[ 0 ]) return;

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    AddTupleToIndex(index, tid, values, buildstate);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Initialize the build state
 */
static void InitBuildState(HnswBuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;

    buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* Require column to have dimensions to be indexed */
    if(buildstate->dimensions < 0) elog(ERROR, "column does not have dimensions");

    // todo:: check here that type of column is vector

    // not supported because of 8K page limit in postgres WAL pages
    // can pass this limit once quantization is supported
    if(buildstate->dimensions > 2000)
        elog(ERROR,
             "vector dimension %d is too large. "
             "LanternDB currently supports up to 2000dim vectors",
             buildstate->dimensions);

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
    // char *alg;
    usearch_init_options_t opts;
    usearch_error_t        error = NULL;
    size_t                 num_added_vectors = 0;
    int                    fd;
    struct stat            file_stat;
    char                  *data;

    // alg = HnswGetAlgorithm(index);

    InitBuildState(buildstate, heap, index, indexInfo);

    // buildstate->hnsw =
    // 	hnsw_new(buildstate->dimensions, max_elems, M, ef_construction);

    opts.connectivity = HnswGetM(index);
    opts.dimensions = buildstate->dimensions;
    opts.expansion_add = HnswGetEfConstruction(index);
    opts.expansion_search = HnswGetEf(index);
    opts.metric_kind = usearch_metric_l2sq_k;
    opts.metric = NULL;
    opts.quantization = usearch_scalar_f32_k;

    LogUsearchOptions(&opts);

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

    usearch_save(buildstate->usearch_index, GLOBAL_HNSW_IDX_NAME, &error);
    assert(error == NULL);
    num_added_vectors = usearch_size(buildstate->usearch_index, &error);
    assert(error == NULL);
    elog(INFO, "done saving %ld vectors", num_added_vectors);

    //****************************** saving to WAL BEGIN ******************************//
    // int pos = 0;
#if defined(LINUX)
    fd = open(GLOBAL_HNSW_IDX_NAME, O_RDONLY | O_NOATIME);
#else
    fd = open(GLOBAL_HNSW_IDX_NAME, O_RDONLY);
#endif
    if(fstat(fd, &file_stat) < 0) {
        close(fd);
        elog(ERROR, "Failed to stat file: %s", GLOBAL_HNSW_IDX_NAME);
    }

    // Map the entire file
    data = (char *)mmap(NULL, file_stat.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if(data == MAP_FAILED) {
        close(fd);
        elog(ERROR, "Failed to mmap file: %s", GLOBAL_HNSW_IDX_NAME);
    }

    UpdateProgress(PROGRESS_CREATEIDX_PHASE, PROGRESS_HNSW_PHASE_LOAD);
    StoreExternalIndex(
        index, buildstate->usearch_index, forkNum, data, file_stat.st_size, buildstate->dimensions, num_added_vectors);

    //****************************** saving to WAL END ******************************//

    usearch_free(buildstate->usearch_index, &error);
    munmap(data, file_stat.st_size);
    close(fd);
    assert(error == NULL);
    buildstate->usearch_index = NULL;

    // hnsw_save(buildstate->hnsw, GLOBAL_HNSW_IDX_NAME);
    // elog(INFO, "inserted %d elements", hnsw_size(buildstate->hnsw));
    // hnsw_destroy(buildstate->hnsw);
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
void ldb_ambuildempty(Relation index)
{
    IndexInfo     *indexInfo = BuildIndexInfo(index);
    HnswBuildState buildstate;

    // q:: why is this init_forknum??
    BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
