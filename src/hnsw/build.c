#include <postgres.h>

#include "build.h"

#include <access/heapam.h>
#include <access/relscan.h>
#include <access/sdir.h>
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
#include <storage/lockdefs.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
// sockets
#include <arpa/inet.h>
#include <sys/socket.h>

#include "usearch.h"

#ifdef _WIN32
#define access _access
#else
#include <unistd.h>
#endif

#include "bench.h"
#include "external_index.h"
#include "hnsw.h"
#include "hnsw/pqtable.h"
#include "hnsw/retriever.h"
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
#define LDB_PROGRESS_CREATEIDX_SUBPHASE     0
#define LDB_PROGRESS_CREATEIDX_TUPLES_TOTAL 0
#define LDB_PROGRESS_CREATEIDX_TUPLES_DONE  0
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

// ============= EXTERNAL INDEXING ============
#define EXTERNAL_INDEX_MAGIC_MSG_SIZE 4
#define EXTERNAL_INDEX_INIT_MSG       0x13333337
#define EXTERNAL_INDEX_END_MSG        0x31333337
#define EXTERNAL_INDEX_ERR_MSG        0x37333337
#define BUFFER_SIZE                   1024 * 1024 * 10  // 10MB

typedef struct external_index_params_t
{
    bool                  pq;
    usearch_metric_kind_t metric_kind;
    usearch_scalar_kind_t quantization;
    uint32_t              dim;
    uint32_t              m;
    uint32_t              ef_construction;
    uint32_t              ef;
    uint32_t              num_centroids;
    uint32_t              num_subvectors;
    uint32_t              estimated_capcity;

} external_index_params_t;

static bool is_little_endian()
{
    int i = 1;

    return *((char *)&i) == 1;
}

#define BYTES_TO_UINT32(bytes) \
    ((uint32)(bytes[ 0 ]) + ((uint32)(bytes[ 1 ]) << 8) + ((uint32)(bytes[ 2 ]) << 16) + ((uint32)(bytes[ 3 ]) << 24))

#define BYTES_TO_UINT64(bytes)                                                                                        \
    ((uint64)(bytes[ 0 ]) + ((uint64)(bytes[ 1 ]) << 8) + ((uint64)(bytes[ 2 ]) << 16) + ((uint64)(bytes[ 3 ]) << 24) \
     + ((uint64)(bytes[ 4 ]) << 32) + ((uint64)(bytes[ 5 ]) << 40) + ((uint64)(bytes[ 6 ]) << 48)                     \
     + ((uint64)(bytes[ 7 ]) << 56))

void check_external_index_response_error(uint32 client_fd, unsigned char *buffer, int32 size)
{
    if(size < 0) {
        close(client_fd);
        // elog(ERROR, "external index socket send failed with %s", strerror(errno));
        elog(ERROR, "external index socket read failed");
    }

    if(size < sizeof(uint32)) return;

    uint8 *bytes = (uint8 *)(buffer);
    uint32 hdr = BYTES_TO_UINT32(bytes);

    if(hdr != EXTERNAL_INDEX_ERR_MSG) return;

    // append nullbyte
    buffer[ size ] = '\0';
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    elog(ERROR, "external index error: %s", buffer + EXTERNAL_INDEX_MAGIC_MSG_SIZE);
}

void check_external_index_request_error(uint32 client_fd, int32 bytes_written)
{
    if(bytes_written > 0) return;

    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    elog(ERROR, "external index socket send failed");
}

static void external_index_send_codebook(
    uint32 client_fd, float *codebook, uint32 dimensions, uint32 num_centroids, uint32 num_subvectors)
{
    int           data_size = dimensions * sizeof(float);
    int           bytes_written = -1;
    unsigned char buf[ data_size ];

    for(int i = 0; i < num_centroids; i++) {
        memcpy(buf, &codebook[ i * dimensions ], data_size);
        bytes_written = send(client_fd, buf, data_size, 0);
        check_external_index_request_error(client_fd, bytes_written);
    }

    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    bytes_written = send(client_fd, &end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    check_external_index_request_error(client_fd, bytes_written);
}

static int create_external_index_session(const char                   *host,
                                         int                           port,
                                         const usearch_init_options_t *params,
                                         const ldb_HnswBuildState     *buildstate,
                                         uint32                        estimated_row_count)
{
    int                client_fd, status;
    unsigned char      init_buf[ sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE ];
    struct sockaddr_in serv_addr;
    unsigned char      init_response[ 1024 ] = {0};

    if((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        elog(ERROR, "external index: socket creation failed");
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if(inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        elog(ERROR, "external index: invalid address");
    }

    if((status = connect(client_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr))) < 0) {
        elog(ERROR, "external index: connection with server failed");
    }

    external_index_params_t index_params = {
        .pq = params->pq,
        .metric_kind = params->metric_kind,
        .quantization = params->quantization,
        .dim = params->dimensions,
        .m = params->connectivity,
        .ef_construction = params->expansion_add,
        .ef = params->expansion_search,
        .num_centroids = params->num_centroids,
        .num_subvectors = params->num_subvectors,
        .estimated_capcity = estimated_row_count,
    };

    uint32 hdr_msg = EXTERNAL_INDEX_INIT_MSG;
    memcpy(init_buf, &hdr_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE);
    memcpy(init_buf + EXTERNAL_INDEX_MAGIC_MSG_SIZE, &index_params, sizeof(external_index_params_t));
    uint32 bytes_written
        = send(client_fd, init_buf, sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    check_external_index_request_error(client_fd, bytes_written);

    if(params->pq) {
        external_index_send_codebook(
            client_fd, buildstate->pq_codebook, params->dimensions, params->num_centroids, params->num_subvectors);
    }

    uint32 buf_size = read(client_fd, &init_response, 1024);

    check_external_index_response_error(client_fd, init_response, buf_size);

    return client_fd;
}

// ============= EXTERNAL INDEXING END ============

static void AddTupleToUsearchIndex(ItemPointer         tid,
                                   Datum               detoasted_vector,
                                   ldb_HnswBuildState *buildstate,
                                   Relation            index)
{
    usearch_error_t       error = NULL;
    usearch_scalar_kind_t usearch_scalar;
    uint8                 scalar_bits = 32;
    uint32                tuple_size, bytes_written;
    // maximum tuple size can be 8kb (8192 byte) + 8 byte label
    unsigned char tuple[ 8200 ];

    void *vector = DatumGetSizedArray(detoasted_vector, buildstate->columnType, buildstate->dimensions, false);
    switch(buildstate->columnType) {
        case REAL_ARRAY:
        case VECTOR:
            usearch_scalar = usearch_scalar_f32_k;
            break;
        case INT_ARRAY:
            // this is taken in hamming distance
            usearch_scalar = usearch_scalar_b1_k;
            scalar_bits = 1;
            break;
        default:
            pg_unreachable();
    }

    // casting tid structure to a number to be used as value in vector search
    // tid has info about disk location of this item and is 6 bytes long
    usearch_label_t label = ItemPointer2Label(tid);

    if(buildstate->external_client_fd) {
        // send tuple over socket if this is external indexing
        tuple_size = sizeof(usearch_label_t) + buildstate->dimensions * (scalar_bits / 8);
        memcpy(tuple, &label, sizeof(usearch_label_t));
        memcpy(tuple + sizeof(usearch_label_t), vector, tuple_size - sizeof(usearch_label_t));
        bytes_written = send(buildstate->external_client_fd, tuple, tuple_size, 0);
        check_external_index_request_error(buildstate->external_client_fd, bytes_written);
    } else if(buildstate->usearch_index != NULL) {
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
    Datum               detoasted_array;
    ldb_HnswBuildState *buildstate = (ldb_HnswBuildState *)state;
    MemoryContext       oldCtx;
    // we can later use this for some optimizations I think
    LDB_UNUSED(tupleIsAlive);

#if PG_VERSION_NUM < 130000
    ItemPointer tid = &hup->t_self;
#endif

    /* Skip nulls */
    if(isnull[ 0 ]) return;
    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Detoast once for all calls */
    detoasted_array = PointerGetDatum(PG_DETOAST_DATUM(values[ 0 ]));
    CheckHnswIndexDimensions(index, detoasted_array, buildstate->dimensions);

    // todo:: the argument values is assumed to be a real[] or vector (they have the same layout)
    // do proper type checking instead of this assumption and test int int arrays and others
    LanternBench("AddTupleToUsearch", AddTupleToUsearchIndex(tid, detoasted_array, buildstate, index));

    // free the detoasted value if we allocated for it, to avoid accumulating them during index construction
    // would be cleaner to create a memory context but not sure how much overhead
    // creating/destroying memory context in a tight loop has
    // if we did not allocate for value, then PG_DETOAST_DATUM returns the same pointer - that is what the check below
    // is
    if(detoasted_array != values[ 0 ]) {
        pfree(DatumGetPointer(detoasted_array));
    }

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
            array = DatumGetArrayTypeP(datum);
            n_items = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
            // todo:: probably can pfree the array
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
        array = DatumGetArrayTypeP(arrayDatum);
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
static void InitBuildState(ldb_HnswBuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->columnType = GetIndexColumnType(index);
    buildstate->dimensions = GetHnswIndexDimensions(index, indexInfo);
    buildstate->index_file_path = ldb_HnswGetIndexFilePath(index);
    buildstate->external = ldb_HnswGetExternal(index);

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
static void FreeBuildState(ldb_HnswBuildState *buildstate)
{
    // todo:: in debug/or stats mode collect stats from the tmpCtx before deleting it
    MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Scan table for tuples to index
 */
static void ScanTable(ldb_HnswBuildState *buildstate)
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
static void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, ldb_HnswBuildState *buildstate)
{
    usearch_error_t        error = NULL;
    usearch_init_options_t opts;
    struct stat            index_file_stat;
    char                  *result_buf = NULL;
    char                  *tmp_index_file_path = NULL;
    const char            *tmp_index_file_fmt_str = "%s/ldb-index-%d.bin";
    // parent_dir + max digits of uint32 (Oid) 10
    const uint32 tmp_index_file_char_cnt = MAXPGPATH + strlen(tmp_index_file_fmt_str) + 10;
    int          index_file_fd;
    int          munmap_ret;
    metadata_t   metadata;
    uint64       num_added_vectors;

    MemSet(&opts, 0, sizeof(opts));

    InitBuildState(buildstate, heap, index, indexInfo);

    opts.dimensions = buildstate->dimensions;

    PopulateUsearchOpts(index, &opts);
    if(opts.pq) {
        buildstate->pq_codebook = load_pq_codebook(index, opts.dimensions, &opts.num_centroids, &opts.num_subvectors);
        assert(0 < opts.num_centroids && opts.num_centroids <= 256);
    }

    buildstate->usearch_scalar = usearch_scalar_f32_k;
    if(opts.metric_kind == usearch_metric_hamming_k) {
        // when using hamming distance, we pass usearch dimension as number of bits
        opts.dimensions *= sizeof(int32) * CHAR_BIT;
        opts.quantization = usearch_scalar_b1_k;
        buildstate->usearch_scalar = usearch_scalar_b1_k;
    }
    // retrievers are not called from here,
    // but we are setting them so the storage layer knows objects are managed
    // externally and does not try to load objects from stream when we call
    // usearch_load
    opts.retriever = ldb_wal_index_node_retriever;
    opts.retriever_mut = ldb_wal_index_node_retriever_mut;

    buildstate->usearch_index = usearch_init(&opts, buildstate->pq_codebook, &error);
    elog(INFO, "done init usearch index");
    assert(error == NULL);

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

        metadata = usearch_index_metadata(buildstate->usearch_index, &error);
        assert(error == NULL);
        opts.connectivity = metadata.connectivity;
        opts.dimensions = metadata.dimensions;
        opts.expansion_add = metadata.expansion_add;
        opts.expansion_search = metadata.expansion_search;
        opts.metric_kind = metadata.init_options.metric_kind;
        opts.pq = metadata.init_options.pq;
        opts.num_centroids = metadata.init_options.num_centroids;
        opts.num_subvectors = metadata.init_options.num_subvectors;
    } else {
        uint32_t estimated_row_count = EstimateRowCount(heap);
        CheckMem(maintenance_work_mem,
                 index,
                 buildstate->usearch_index,
                 estimated_row_count,
                 "index size exceeded maintenance_work_mem during index construction, consider increasing "
                 "maintenance_work_mem");

        if(buildstate->external) {
            assert(is_little_endian());
            elog(INFO,
                 "connecting to external indexing daemon on %s:%d",
                 ldb_external_index_host,
                 ldb_external_index_port);
            buildstate->external_client_fd = create_external_index_session(
                ldb_external_index_host, ldb_external_index_port, &opts, buildstate, estimated_row_count);
            assert(buildstate->external_client_fd > 0);
        } else {
            usearch_reserve(buildstate->usearch_index, estimated_row_count, &error);
        }

        if(error != NULL) {
            // There's not much we can do if free throws an error, but we want to preserve the contents of the first one
            // in case it does
            usearch_error_t local_error = NULL;
            usearch_free(buildstate->usearch_index, &local_error);
            elog(ERROR, "Error reserving space for index: %s", error);
        }

        UpdateProgress(PROGRESS_CREATEIDX_PHASE, LDB_PROGRESS_HNSW_PHASE_IN_MEMORY_INSERT);
        LanternBench("build hnsw index", ScanTable(buildstate));

        if(!buildstate->external) {
            elog(INFO, "inserted %ld elements", usearch_size(buildstate->usearch_index, &error));
        }
        assert(error == NULL);
    }

    metadata = usearch_index_metadata(buildstate->usearch_index, &error);
    assert(error == NULL);

    if(buildstate->index_file_path) {
        index_file_fd = open(buildstate->index_file_path, O_RDONLY);
    } else if(buildstate->external) {
        uint32        end_msg = EXTERNAL_INDEX_END_MSG;
        unsigned char buffer[ sizeof(uint64_t) ];
        int32         bytes_read, bytes_written;
        uint64        index_size = 0, total_received = 0;

        // send message indicating that we have finished streaming tuples
        bytes_written = send(buildstate->external_client_fd, &end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);
        check_external_index_request_error(buildstate->external_client_fd, bytes_written);

        // read how many tuples have been indexed
        bytes_read = read(buildstate->external_client_fd, buffer, sizeof(uint64));
        check_external_index_response_error(buildstate->external_client_fd, buffer, bytes_read);
        num_added_vectors = BYTES_TO_UINT64(buffer);

        // read index file size
        bytes_read = read(buildstate->external_client_fd, buffer, sizeof(uint64));
        check_external_index_response_error(buildstate->external_client_fd, buffer, bytes_read);
        index_size = BYTES_TO_UINT64(buffer);

        result_buf = palloc0(index_size);

        assert(result_buf != NULL);

        // start reading index into buffer
        while(total_received < index_size) {
            bytes_read = read(buildstate->external_client_fd, result_buf + total_received, BUFFER_SIZE);

            // Check for CTRL-C interrupts
            CHECK_FOR_INTERRUPTS();

            check_external_index_response_error(
                buildstate->external_client_fd, (unsigned char *)result_buf + total_received, bytes_read);

            if(bytes_read == 0) {
                break;
            }

            total_received += bytes_read;
        }

    } else {
        // Save index into temporary file
        // To later mmap it into memory
        // The file will be removed in the end
        tmp_index_file_path = palloc0(tmp_index_file_char_cnt);
        // Create index file directory string: $pg_data_dir/ldb_indexes/index-$relfilenode.bin
        snprintf(
            tmp_index_file_path, tmp_index_file_char_cnt, tmp_index_file_fmt_str, DataDir, index->rd_rel->relfilenode);
        usearch_save(buildstate->usearch_index, tmp_index_file_path, &error);
        assert(error == NULL);
        index_file_fd = open(tmp_index_file_path, O_RDONLY);
    }

    assert(index_file_fd > 0);

    if(!buildstate->external) {
        num_added_vectors = usearch_size(buildstate->usearch_index, &error);
    }

    assert(error == NULL);
    elog(INFO, "done saving %lu vectors", num_added_vectors);

    //****************************** mmap index to memory BEGIN ******************************//
    usearch_free(buildstate->usearch_index, &error);
    assert(error == NULL);
    buildstate->usearch_index = NULL;

    if(!buildstate->external) {
        fstat(index_file_fd, &index_file_stat);
        result_buf = mmap(NULL, index_file_stat.st_size, PROT_READ, MAP_PRIVATE, index_file_fd, 0);
        assert(result_buf != MAP_FAILED);
    }
    //****************************** mmap index to memory END ******************************//

    // save the index to WAL
    UpdateProgress(PROGRESS_CREATEIDX_PHASE, LDB_PROGRESS_HNSW_PHASE_LOAD);
    StoreExternalIndex(index, &metadata, MAIN_FORKNUM, result_buf, &opts, num_added_vectors);

    munmap_ret = munmap(result_buf, index_file_stat.st_size);
    assert(munmap_ret == 0);
    LDB_UNUSED(munmap_ret);
    close(index_file_fd);

    if(buildstate->external) {
        shutdown(buildstate->external_client_fd, SHUT_RDWR);
        close(buildstate->external_client_fd);
    }

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
static void BuildEmptyIndex(Relation index, IndexInfo *indexInfo, ldb_HnswBuildState *buildstate)
{
    usearch_error_t        error = NULL;
    usearch_init_options_t opts;
    MemSet(&opts, 0, sizeof(opts));

    InitBuildState(buildstate, NULL, index, indexInfo);
    opts.dimensions = buildstate->dimensions;
    PopulateUsearchOpts(index, &opts);
    // when using hamming distance, we pass dimension as number of bits
    buildstate->usearch_scalar = usearch_scalar_f32_k;
    if(opts.metric_kind == usearch_metric_hamming_k) {
        opts.dimensions *= sizeof(int32) * CHAR_BIT;
        opts.quantization = usearch_scalar_b1_k;
        buildstate->usearch_scalar = usearch_scalar_b1_k;
    }

    if(opts.pq) {
        buildstate->pq_codebook = load_pq_codebook(index, opts.dimensions, &opts.num_centroids, &opts.num_subvectors);
        assert(0 < opts.num_centroids && opts.num_centroids <= 256);
    }

    buildstate->usearch_index = usearch_init(&opts, buildstate->pq_codebook, &error);
    assert(error == NULL);

    char *result_buf = palloc(USEARCH_EMPTY_INDEX_SIZE);
    usearch_save_buffer(buildstate->usearch_index, result_buf, USEARCH_EMPTY_INDEX_SIZE, &error);
    assert(error == NULL && result_buf != NULL);

    StoreExternalEmptyIndex(index, INIT_FORKNUM, result_buf, buildstate->dimensions, &opts);

    usearch_free(buildstate->usearch_index, &error);
    assert(error == NULL);
    buildstate->usearch_index = NULL;

    FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *ldb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult  *result;
    ldb_HnswBuildState buildstate;
    memset(&buildstate, 0, sizeof(ldb_HnswBuildState));

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
    ldb_HnswBuildState buildstate;
    memset(&buildstate, 0, sizeof(ldb_HnswBuildState));
    IndexInfo *indexInfo = BuildIndexInfo(index);
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
    Oid                  function_argtypes_oid[ 7 ];
    oidvector           *function_argtypes;
    char                *metric_kind;
    const char          *lantern_extras_schema = "lantern_extras";
    uint32_t             dim = 0;
    uint32_t             m = 0;
    uint32_t             ef_construction = 0;
    uint32_t             ef = 0;
    bool                 pq = false;
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
    function_argtypes_oid[ 6 ] = BOOLOID;
    function_argtypes = buildoidvector(function_argtypes_oid, 7);

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
    pq = headerp->pq;

    UnlockReleaseBuffer(buf);
    relation_close(index_rel, AccessShareLock);

    // We can not have external index without knowing dimensions
    if(dim <= 0) {
        elog(ERROR, "Column does not have dimensions: can not create external index on empty table");
    }

    // Get _reindex_external_index function info to do direct call into it
    fmgr_info(function_oid, &reindex_finfo);

    assert(reindex_finfo.fn_addr != NULL);

    DirectFunctionCall7(reindex_finfo.fn_addr,
                        ObjectIdGetDatum(indrelid),
                        CStringGetTextDatum(metric_kind),
                        Int32GetDatum(dim),
                        Int32GetDatum(m),
                        Int32GetDatum(ef_construction),
                        Int32GetDatum(ef),
                        BoolGetDatum(pq));
}
