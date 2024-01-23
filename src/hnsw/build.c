#include <postgres.h>

#include "build.h"

#include <access/heapam.h>
#include <assert.h>
#include <catalog/index.h>
#include <catalog/namespace.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <executor/executor.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <nodes/execnodes.h>
#include <stdint.h>
#include <storage/bufmgr.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>

#include "usearch.h"

#ifdef _WIN32
#define access _access
#else
#include <unistd.h>
#endif

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

static void AddTupleToUsearchIndex(ItemPointer tid, Datum *values, HnswBuildState *buildstate, Relation index)
{
    /* Detoast once for all calls */
    usearch_error_t       error = NULL;
    Datum                 value = PointerGetDatum(PG_DETOAST_DATUM(values[ 0 ]));
    usearch_scalar_kind_t usearch_scalar;

    void *vector = DatumGetSizedArray(value, buildstate->columnType, buildstate->dimensions);
    switch(buildstate->columnType) {
        case REAL_ARRAY:
        case VECTOR:
            usearch_scalar = usearch_scalar_f32_k;
            break;
        case INT_ARRAY:
            // this is fine, since we only use integer arrays with hamming distance metric
            // and hamming distance in usearch doesn't care about scalar type
            // also, usearch will appropriately cast integer arrays even with this scalar type
            usearch_scalar = usearch_scalar_f32_k;
            break;
        default:
            pg_unreachable();
    }

    // casting tid structure to a number to be used as value in vector search
    // tid has info about disk location of this item and is 6 bytes long
    usearch_label_t label = GetUsearchLabel(tid);
#ifdef LANTERN_USE_LIBHNSW
    if(buildstate->hnsw != NULL) hnsw_add(buildstate->hnsw, vector, label);
#endif
#ifdef LANTERN_USE_USEARCH
    if(buildstate->usearch_index != NULL) {
        size_t capacity = usearch_capacity(buildstate->usearch_index, &error);
        if(capacity == usearch_size(buildstate->usearch_index, &error)) {
            CheckMem(maintenance_work_mem,
                     index,
                     buildstate->usearch_index,
                     2 * usearch_size(buildstate->usearch_index, &error),
                     "index size exceeded maintenance_work_mem during index construction, consider increasing "
                     "maintenance_work_mem");
            usearch_reserve(buildstate->usearch_index, 2 * capacity, &error);
            assert(error == NULL);
        }
        usearch_add(buildstate->usearch_index, label, vector, usearch_scalar, &error);
    }
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
    AddTupleToUsearchIndex(tid, values, buildstate, index);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

static int GetArrayLengthFromExpression(Expr *expression, Relation heap, HeapTuple tuple)
{
    ExprContext    *econtext;
    ExprState      *exprstate;
    EState         *estate;
    Datum           result;
    bool            isNull;
    Oid             resultOid;
    TupleTableSlot *slot;
    TupleDesc       tupdesc = RelationGetDescr(heap);

#if PG_VERSION_NUM >= 120000
    slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
#else
    slot = MakeSingleTupleTableSlot(tupdesc);
#endif

    // Create an expression context
    econtext = CreateStandaloneExprContext();
    estate = CreateExecutorState();

    // Build the expression state for your expression
    exprstate = ExecPrepareExpr(expression, estate);

#if PG_VERSION_NUM >= 120000
    ExecStoreHeapTuple(tuple, slot, false);
#else
    ExecStoreTuple(tuple, slot, InvalidBuffer, false);
#endif
    // Set up the tuple for the expression evaluation
    econtext->ecxt_scantuple = slot;

    // Evaluate the expression for the first row
    result = ExecEvalExprSwitchContext(exprstate, econtext, &isNull);

    // Release tuple descriptor
    ReleaseTupleDesc(tupdesc);

    // Get the return type information
    get_expr_result_type((Node *)exprstate->expr, &resultOid, NULL);

    HnswColumnType columnType = GetColumnTypeFromOid(resultOid);

    if(columnType == REAL_ARRAY || columnType == INT_ARRAY) {
        ArrayType *array = DatumGetArrayTypeP(result);
        return ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    } else if(columnType == VECTOR) {
        Vector *vector = DatumGetVector(result);
        return vector->dim;
    } else {
        // Check if the result is not null and is supported type
        // There is a guard in postgres that wont' allow passing
        // Anything else from the defined operator class types
        // Throwing an error like: ERROR:  data type text has no default operator class for access method "hnsw"
        // So this case will be marked as invariant
        ldb_invariant(!isNull && columnType != UNKNOWN,
                      "Expression used in CREATE INDEX statement did not result in hnsw-index compatible array");
    }

    return HNSW_DEFAULT_DIM;
}

static int GetArrayLengthFromHeap(Relation heap, int indexCol, IndexInfo *indexInfo)
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
    int        n_items = HNSW_DEFAULT_DIM;
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

    if(indexInfo->ii_Expressions != NULL) {
        // We don't suport multicolumn indexes
        // So trying to pass multiple expressions on index creation
        // Will result an error before getting here
        ldb_invariant(indexInfo->ii_Expressions->length == 1,
                      "Index expressions can not be greater than 1 as multicolumn indexes are not supported");
        Expr *indexpr_item = lfirst(list_head(indexInfo->ii_Expressions));
        n_items = GetArrayLengthFromExpression(indexpr_item, heap, tuple);
    } else {
        // Get the indexed column out of the row and return it's dimensions
        datum = heap_getattr(tuple, indexCol, RelationGetDescr(heap), &isNull);
        if(!isNull) {
            array = DatumGetArrayTypePCopy(datum);
            n_items = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
        }
    }

    heap_endscan(scan);

    return n_items;
}

int GetHnswIndexDimensions(Relation index, IndexInfo *indexInfo)
{
    HnswColumnType columnType = GetIndexColumnType(index);

    // check if column is type of real[] or integer[]
    if(columnType == REAL_ARRAY || columnType == INT_ARRAY) {
        // The dimension in options is not set if the dimension is inferred so we need to actually check the key
        int opt_dim = ldb_HnswGetDim(index);
        if(opt_dim == HNSW_DEFAULT_DIM) {
            // If the option's still the default it needs to be updated to match what was inferred
            // todo: is there a way to do this earlier? (rd_options is null in BuildInit)
            Relation         heap;
            ldb_HnswOptions *opts;
            int              attrNum;

            assert(index->rd_index->indnatts == 1);
            attrNum = index->rd_index->indkey.values[ 0 ];
#if PG_VERSION_NUM < 120000
            heap = heap_open(index->rd_index->indrelid, AccessShareLock);
#else
            heap = table_open(index->rd_index->indrelid, AccessShareLock);
#endif
            opt_dim = GetArrayLengthFromHeap(heap, attrNum, indexInfo);
            opts = (ldb_HnswOptions *)index->rd_options;
            if(opts != NULL) {
                opts->dim = opt_dim;
            }
#if PG_VERSION_NUM < 120000
            heap_close(heap, AccessShareLock);
#else
            table_close(heap, AccessShareLock);
#endif
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
    ArrayType     *array;
    int            n_items;
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

    // If NumIndexAttrs isn't 1 the index has been instantiated on multiple keys and there's no clear way to infer
    // the dim
    if(indexInfo->ii_NumIndexAttrs != 1) {
        return HNSW_DEFAULT_DIM;
    }

    indexCol = indexInfo->ii_IndexAttrNumbers[ 0 ];
    return GetArrayLengthFromHeap(heap, indexCol, indexInfo);
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
    buildstate->dimensions = GetHnswIndexDimensions(index, indexInfo);
    buildstate->index_file_path = ldb_HnswGetIndexFilePath(index);

    // If a dimension wasn't specified try to infer it
    if(heap != NULL && buildstate->dimensions < 1) {
        buildstate->dimensions = InferDimension(heap, indexInfo);
    }
    /* Require column to have dimensions to be indexed */
    if(buildstate->dimensions < 1) elog(ERROR, "column does not have dimensions, please specify one");

    // not supported because of 8K page limit in postgres WAL pages
    // can pass this limit once quantization is supported
    if(buildstate->dimensions > HNSW_MAX_DIM)
        elog(ERROR,
             "vector dimension %d is too large. "
             "LanternDB currently supports up to %ddim vectors",
             buildstate->dimensions,
             HNSW_MAX_DIM);

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
 * Build the index, writing to the main fork
 */
static void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, HnswBuildState *buildstate)
{
    usearch_error_t        error = NULL;
    usearch_init_options_t opts;
    struct stat            index_file_stat;
    char                  *result_buf = NULL;
    char                  *tmp_index_file_path = NULL;
    const char            *tmp_index_file_fmt_str = "%s/ldb-index-%d.bin";
    // parent_dir + max digits of uint32 (Oid) 10
    const uint32       tmp_index_file_char_cnt = MAXPGPATH + strlen(tmp_index_file_fmt_str) + 10;
    int                index_file_fd;
    int                munmap_ret;
    usearch_metadata_t metadata;
    size_t             num_added_vectors;

    MemSet(&opts, 0, sizeof(opts));

    InitBuildState(buildstate, heap, index, indexInfo);
    opts.dimensions = buildstate->dimensions;
    PopulateUsearchOpts(index, &opts);

    buildstate->usearch_index = usearch_init(&opts, &error);
    elog(INFO, "done init usearch index");
    assert(error == NULL);

    buildstate->hnsw = NULL;
    if(buildstate->index_file_path) {
        if(access(buildstate->index_file_path, F_OK) != 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("Invalid index file path. "
                            "If this is REINDEX operation call `SELECT "
                            "lantern_reindex_external_index('%s')` to recreate index",
                            RelationGetRelationName(index))));
        }
        usearch_load(buildstate->usearch_index, buildstate->index_file_path, &error);
        if(error != NULL) {
            elog(ERROR, "%s", error);
        }
        elog(INFO, "done loading usearch index");

        metadata = usearch_metadata(buildstate->usearch_index, &error);
        assert(error == NULL);
        opts.connectivity = metadata.connectivity;
        opts.dimensions = metadata.dimensions;
        opts.expansion_add = metadata.expansion_add;
        opts.expansion_search = metadata.expansion_search;
        opts.metric_kind = metadata.metric_kind;
    } else {
        BlockNumber numBlocks = RelationGetNumberOfBlocks(heap);
        uint32_t    estimated_row_count = 0;
        if(numBlocks > 0) {
            // Read the first block
            Buffer buffer = ReadBufferExtended(heap, MAIN_FORKNUM, 0, RBM_NORMAL, NULL);
            // Lock buffer so there won't be any new writes during this operation
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
            // This is like converting block buffer to Page struct
            Page page = BufferGetPage(buffer);
            // Getting the maximum tuple index on the page
            OffsetNumber offset = PageGetMaxOffsetNumber(page);

            // Reasonably accurate first guess, assuming tuples are fixed length it will err towards over allocating.
            // In the case of under allocation the logic in AddTupleToUsearchIndex should expand it as needed
            estimated_row_count = offset * numBlocks;
            // Unlock and release buffer
            UnlockReleaseBuffer(buffer);
        }
        CheckMem(maintenance_work_mem,
                 index,
                 buildstate->usearch_index,
                 estimated_row_count,
                 "index size exceeded maintenance_work_mem during index construction, consider increasing "
                 "maintenance_work_mem");
        usearch_reserve(buildstate->usearch_index, estimated_row_count, &error);
        if(error != NULL) {
            // There's not much we can do if free throws an error, but we want to preserve the contents of the first one
            // in case it does
            usearch_error_t local_error = NULL;
            usearch_free(buildstate->usearch_index, &local_error);
            elog(ERROR, "Error reserving space for index: %s", error);
        }

        UpdateProgress(PROGRESS_CREATEIDX_PHASE, PROGRESS_HNSW_PHASE_IN_MEMORY_INSERT);
        LanternBench("build hnsw index", ScanTable(buildstate));

        elog(INFO, "inserted %ld elements", usearch_size(buildstate->usearch_index, &error));
        assert(error == NULL);
    }

    metadata = usearch_metadata(buildstate->usearch_index, &error);
    assert(error == NULL);

    if(buildstate->index_file_path == NULL) {
        // Save index into temporary file
        // To later mmap it into memory
        // The file will be removed in the end
        tmp_index_file_path = palloc0(tmp_index_file_char_cnt);
        // Create index file directory string: $pg_data_dir/ldb_indexes/index-$relfilenode.bin
        snprintf(
            tmp_index_file_path, tmp_index_file_char_cnt, tmp_index_file_fmt_str, DataDir, index->rd_rel->relfilenode);
        usearch_save(buildstate->usearch_index, tmp_index_file_path, NULL, &error);
        assert(error == NULL);
        index_file_fd = open(tmp_index_file_path, O_RDONLY);
    } else {
        index_file_fd = open(buildstate->index_file_path, O_RDONLY);
    }
    assert(index_file_fd > 0);

    num_added_vectors = usearch_size(buildstate->usearch_index, &error);
    assert(error == NULL);
    elog(INFO, "done saving %ld vectors", num_added_vectors);

    //****************************** mmap index to memory BEGIN ******************************//
    usearch_free(buildstate->usearch_index, &error);
    assert(error == NULL);
    buildstate->usearch_index = NULL;

    fstat(index_file_fd, &index_file_stat);
    result_buf = mmap(NULL, index_file_stat.st_size, PROT_READ, MAP_PRIVATE, index_file_fd, 0);
    assert(result_buf != MAP_FAILED);
    //****************************** mmap index to memory END ******************************//

    //****************************** saving to WAL BEGIN ******************************//
    UpdateProgress(PROGRESS_CREATEIDX_PHASE, PROGRESS_HNSW_PHASE_LOAD);
    StoreExternalIndex(index, &metadata, MAIN_FORKNUM, result_buf, &opts, num_added_vectors);
    //****************************** saving to WAL END ******************************//

    munmap_ret = munmap(result_buf, index_file_stat.st_size);
    assert(munmap_ret == 0);
    LDB_UNUSED(munmap_ret);
    close(index_file_fd);

    if(tmp_index_file_path) {
        // remove index file if it was not externally provided
        unlink(tmp_index_file_path);
        pfree(tmp_index_file_path);
    }

    FreeBuildState(buildstate);
}

/*
 * Build an empty index, writing to the init fork
 */
static void BuildEmptyIndex(Relation index, IndexInfo *indexInfo, HnswBuildState *buildstate)
{
    usearch_error_t        error = NULL;
    usearch_init_options_t opts;
    MemSet(&opts, 0, sizeof(opts));

    InitBuildState(buildstate, NULL, index, indexInfo);
    opts.dimensions = buildstate->dimensions;
    PopulateUsearchOpts(index, &opts);

    buildstate->usearch_index = usearch_init(&opts, &error);
    assert(error == NULL);

    buildstate->hnsw = NULL;

    char *result_buf = NULL;
    usearch_save(buildstate->usearch_index, NULL, &result_buf, &error);
    assert(error == NULL && result_buf != NULL);

    StoreExternalEmptyIndex(index, INIT_FORKNUM, result_buf, &opts);

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

    // todo:: change the warning to error once VersionsMismatch learns how to differntiate when an update script is
    // running - it is fine to temporarily have version mismatch when we are running an update script
    if(!VersionsMatch()) {
        elog(WARNING,
             "Attempting to build lantern index, but the SQL version and binary version do not match. This can cause "
             "errors. Please run `ALTER EXTENSION lantern UPDATE and reconnect");
    }

    BuildIndex(heap, index, indexInfo, &buildstate);

    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.tuples_indexed;

    return result;
}

/*
 * Build an empty index for an unlogged table
 */
void ldb_ambuildunlogged(Relation index)
{
    HnswBuildState buildstate;
    IndexInfo     *indexInfo = BuildIndexInfo(index);
    BuildEmptyIndex(index, indexInfo, &buildstate);
}

void ldb_reindex_external_index(Oid indrelid)
{
    HnswIndexHeaderPage *headerp;
    FmgrInfo             reindex_finfo = {0};
    BlockNumber          HEADER_BLOCK = 0;
    Relation             index_rel;
    Buffer               buf;
    Page                 page;
    Oid                  lantern_extras_namespace_oid = InvalidOid;
    Oid                  function_oid;
    Oid                  function_argtypes_oid[ 6 ];
    oidvector           *function_argtypes;
    char                *metric_kind;
    const char          *lantern_extras_schema = "lantern_extras";
    uint32_t             dim = 0;
    uint32_t             m = 0;
    uint32_t             ef_construction = 0;
    uint32_t             ef = 0;
    char *ext_not_found_err = "Please install 'lantern_extras' extension or update it to the latest version";

    lantern_extras_namespace_oid = get_namespace_oid(lantern_extras_schema, true);

    if(!OidIsValid(lantern_extras_namespace_oid)) {
        elog(ERROR, "%s", ext_not_found_err);
    }

    // Check if _reindex_external_index function exists in lantern schema
    function_argtypes_oid[ 0 ] = REGCLASSOID;
    function_argtypes_oid[ 1 ] = TEXTOID;
    function_argtypes_oid[ 2 ] = INT4OID;
    function_argtypes_oid[ 3 ] = INT4OID;
    function_argtypes_oid[ 4 ] = INT4OID;
    function_argtypes_oid[ 5 ] = INT4OID;
    function_argtypes = buildoidvector(function_argtypes_oid, 6);

    function_oid = GetSysCacheOid(PROCNAMEARGSNSP,
#if PG_VERSION_NUM >= 120000
                                  Anum_pg_proc_oid,
#endif
                                  CStringGetDatum("_reindex_external_index"),
                                  PointerGetDatum(function_argtypes),
                                  ObjectIdGetDatum(lantern_extras_namespace_oid),
                                  0);

    if(!OidIsValid(function_oid)) {
        elog(ERROR, "%s", ext_not_found_err);
    }
    // Get index params from index header page
    index_rel = relation_open(indrelid, AccessShareLock);
    buf = ReadBuffer(index_rel, HEADER_BLOCK);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    headerp = (HnswIndexHeaderPage *)PageGetContents(page);

    assert(headerp->magicNumber == LDB_WAL_MAGIC_NUMBER);

    // Convert metric_kind enum to string representation
    switch(headerp->metric_kind) {
        case usearch_metric_l2sq_k:
            metric_kind = "l2sq";
            break;
        case usearch_metric_cos_k:
            metric_kind = "cos";
            break;
        case usearch_metric_hamming_k:
            metric_kind = "hamming";
            break;
        default:
            metric_kind = NULL;
            ldb_invariant(true, "Unsupported metric kind");
    }

    dim = headerp->vector_dim;
    m = headerp->m;
    ef = headerp->ef;
    ef_construction = headerp->ef_construction;

    UnlockReleaseBuffer(buf);
    relation_close(index_rel, AccessShareLock);

    // We can not have external index without knowing dimensions
    if(dim <= 0) {
        elog(ERROR, "Column does not have dimensions: can not create external index on empty table");
    }

    // Get _reindex_external_index function info to do direct call into it
    fmgr_info(function_oid, &reindex_finfo);

    assert(reindex_finfo.fn_addr != NULL);

    DirectFunctionCall6(reindex_finfo.fn_addr,
                        ObjectIdGetDatum(indrelid),
                        CStringGetTextDatum(metric_kind),
                        Int32GetDatum(dim),
                        Int32GetDatum(m),
                        Int32GetDatum(ef_construction),
                        Int32GetDatum(ef));
}
