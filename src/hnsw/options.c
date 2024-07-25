#include <postgres.h>

#include "options.h"

#include <access/htup_details.h>
#include <access/reloptions.h>
#include <catalog/pg_amproc.h>
#include <catalog/pg_type_d.h>
#include <executor/executor.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <utils/catcache.h>
#include <utils/guc.h>
#include <utils/rel.h>  // RelationData
#include <utils/syscache.h>

#include "../hooks/executor_start.h"
#include "../hooks/post_parse.h"

// We import this header file
// to access the op class support function pointers
#include "../hnsw.h"
#include "usearch.h"
#include "utils.h"

#ifndef NDEBUG
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#endif

// reloption for lantern hnsw index creation paramters in
// CREATE INDEX ... WITH (...)
//                       ^^^^
static relopt_kind ldb_hnsw_index_withopts;

#if PG_VERSION_NUM >= 130000
static relopt_enum_elt_def quant_bits_options_relopt[] = {{"1", QUANT_BITS_1},
                                                          {"2", QUANT_BITS_2},
                                                          {"4", QUANT_BITS_4},
                                                          {"8", QUANT_BITS_8},
                                                          {"16", QUANT_BITS_16},
                                                          {"32", QUANT_BITS_32},
                                                          {NULL, 0}

};
#endif

int   ldb_hnsw_init_k;
int   ldb_hnsw_ef_search;
int   ldb_external_index_port;
char *ldb_external_index_host;
bool  ldb_external_index_secure;

// if this variable is set to true
// our operator rewriting hooks will be disabled
bool ldb_pgvector_compat;

// this variable is only set during testing and controls whether
// certain elog() calls are made
// see ldb_dlog() definition and callsites for details
bool ldb_is_test;

int ldb_HnswGetDim(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(opts) return opts->dim;
    return HNSW_DEFAULT_DIM;
}

int ldb_HnswGetM(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(opts) return opts->m;
    return HNSW_DEFAULT_M;
}

int ldb_HnswGetEfConstruction(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(opts) return opts->ef_construction;
    return HNSW_DEFAULT_EF_CONSTRUCTION;
}

int ldb_HnswGetEf(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(opts) return opts->ef;
    return HNSW_DEFAULT_EF;
}

char *ldb_HnswGetIndexFilePath(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(!opts) return NULL;
    if(!opts->experimantal_index_path_offset) {
        return NULL;
    }

    return (char *)opts + opts->experimantal_index_path_offset;
}

bool ldb_HnswGetPq(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(opts) return opts->pq;
    return false;
}

usearch_metric_kind_t ldb_HnswGetMetricKind(Relation index)
{
    struct catclist *proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(index->rd_opfamily[ 0 ]));

    if(proclist->n_members == 0) {
        elog(ERROR, "no support functions found");
    }

    HeapTuple      proctup = &proclist->members[ 0 ]->tuple;
    Form_pg_amproc procform = (Form_pg_amproc)GETSTRUCT(proctup);
    FmgrInfo      *fninfo = index_getprocinfo(index, 1, procform->amprocnum);
    void          *fnaddr = fninfo->fn_addr;
    ReleaseCatCacheList(proclist);

    if(fnaddr == l2sq_dist || fnaddr == vector_l2sq_dist) {
        return usearch_metric_l2sq_k;
    } else if(fnaddr == hamming_dist) {
        return usearch_metric_hamming_k;
    } else if(fnaddr == cos_dist || fnaddr == vector_cos_dist) {
        return usearch_metric_cos_k;
    } else {
        elog(ERROR, "could not find distance function for index");
    }
}

bool ldb_HnswGetExternal(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(opts) return opts->external;
    return false;
}

usearch_scalar_kind_t ldb_HnswGetScalarKind(Relation index)
{
    ldb_HnswOptions *opts = (ldb_HnswOptions *)index->rd_options;
    if(!opts) return usearch_scalar_f32_k;
    switch(opts->quant_bits) {
        case QUANT_BITS_32:
        case QUANT_BITS_UNSET:
            return usearch_scalar_f32_k;
        case QUANT_BITS_16:
            return usearch_scalar_f16_k;
        case QUANT_BITS_8:
            return usearch_scalar_i8_k;

        case QUANT_BITS_4:
        case QUANT_BITS_2:
            elog(ERROR, "unimplemented quantization");
        case QUANT_BITS_1:
            return usearch_scalar_b1_k;
        default:
            elog(ERROR, "unrecognized quantization provided");
    }
}

/*
 * Parse and validate the reloptions
 */
bytea *ldb_amoptions(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {

        {"dim", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, dim)},
        {"m", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, m)},
        {"ef_construction", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, ef_construction)},
        {"ef", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, ef)},
        {"pq", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, pq)},
        {"external", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, external)},
#if PG_VERSION_NUM >= 130000
        {"quant_bits", RELOPT_TYPE_ENUM, offsetof(ldb_HnswOptions, quant_bits)},
#else
        {"quant_bits", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, quant_bits)},
#endif
        {"_experimental_index_path", RELOPT_TYPE_STRING, offsetof(ldb_HnswOptions, experimantal_index_path_offset)},
    };

#if PG_VERSION_NUM >= 130000
    LDB_UNUSED(validate);
    return (bytea *)build_reloptions(
        reloptions, true, ldb_hnsw_index_withopts, sizeof(ldb_HnswOptions), tab, lengthof(tab));
#else
    // clang-format off
    relopt_value *options;
    int numoptions;
    ldb_HnswOptions *rdopts;
    // clang-format on

    options = parseRelOptions(reloptions, validate, ldb_hnsw_index_withopts, &numoptions);
    rdopts = allocateReloptStruct(sizeof(ldb_HnswOptions), options, numoptions);
    fillRelOptions((void *)rdopts, sizeof(ldb_HnswOptions), options, numoptions, validate, tab, lengthof(tab));

    return (bytea *)rdopts;
#endif
}

#ifndef NDEBUG
static void ldb_wait_for_gdb(int sig)
{
    if(false) {
        elog(WARNING, "exitting due to '%s'. set LDB_GDB=1 to wait for gdb", strsignal(sig));
        signal(SIGSEGV, NULL);
        signal(SIGABRT, NULL);
        return;
    }

    pid_t pid = getpid();
    elog(WARNING, "in segv handler pid: %d", pid);
    bool wait = true;
    while(wait) {
        elog(WARNING, "waiting for gdb to connect to %d", pid);
        sleep(5);
    }
}
#endif

/*
 * Initialize index options and variables
 */
void _PG_init(void)
{
    (void)CheckExtensionVersions();

    if(process_shared_preload_libraries_in_progress) {
        elog(WARNING,
             "LanternDB HNSW index extension loaded inside shared_preload_libraries."
             "Make sure to restart the server before running ALTER EXTENSION lantern UPDATE");
    }

    // todo:: cross-check with this`
    // https://github.com/zombodb/zombodb/blob/34c732a0b143b5e424ced64c96e8c4d567a14177/src/access_method/options.rs#L895
    ldb_hnsw_index_withopts = add_reloption_kind();
    add_int_reloption(ldb_hnsw_index_withopts,
                      "dim",
                      "Number of dimensions of the vector",
                      HNSW_DEFAULT_DIM,
                      1,
                      HNSW_MAX_DIM
#if PG_VERSION_NUM >= 130000
                      ,
                      AccessExclusiveLock
#endif
    );
    add_int_reloption(ldb_hnsw_index_withopts,
                      "m",
                      "HNSW M hyperparameter",
                      HNSW_DEFAULT_M,
                      2,
                      HNSW_MAX_M
#if PG_VERSION_NUM >= 130000
                      ,
                      AccessExclusiveLock
#endif
    );
    add_int_reloption(ldb_hnsw_index_withopts,
                      "ef_construction",
                      "HNSW ef-construction hyperparameter",
                      HNSW_DEFAULT_EF_CONSTRUCTION,
                      1,
                      HNSW_MAX_EF_CONSTRUCTION
#if PG_VERSION_NUM >= 130000
                      ,
                      AccessExclusiveLock
#endif
    );

    add_int_reloption(ldb_hnsw_index_withopts,
                      "ef",
                      "HNSW ef-search hyperparameter",
                      HNSW_DEFAULT_EF,
                      1,
                      HNSW_MAX_EF
#if PG_VERSION_NUM >= 130000
                      ,
                      AccessExclusiveLock
#endif
    );
    add_string_reloption(ldb_hnsw_index_withopts,
                         "_experimental_index_path",
                         "LanternDB expored index file path",
                         NULL,
                         NULL
#if PG_VERSION_NUM >= 130000
                         ,
                         AccessExclusiveLock
#endif
    );
    add_bool_reloption(ldb_hnsw_index_withopts,
                       "pq",
                       "Whether or not use to quantized table codebook for index construction. Assumes codebook is "
                       "called [tablename]_pq_codebook",
                       false
#if PG_VERSION_NUM >= 130000
                       ,
                       AccessExclusiveLock
#endif

    );
#if PG_VERSION_NUM >= 130000
    add_enum_reloption(ldb_hnsw_index_withopts,
                       "quant_bits",
                       "When set, will quantize 32 bit vector elements into specified number of bits.",
                       quant_bits_options_relopt,
                       QUANT_BITS_UNSET,
                       "Unsupported quantization bits. Supported values are 1, 2, 4, 8, 16 and 32",
                       AccessExclusiveLock);
#else
    add_int_reloption(ldb_hnsw_index_withopts,
                      "quant_bits",
                      "When set, will quantize 32 bit vector elements into specified number of bits.",
                      QUANT_BITS_UNSET,
                      1,
                      32);
#endif
    add_bool_reloption(ldb_hnsw_index_withopts,
                       "external",
                       "Whether or not use external indexing protocol",
                       false
#if PG_VERSION_NUM >= 130000
                       ,
                       AccessExclusiveLock
#endif
    );
    DefineCustomIntVariable("lantern_hnsw.init_k",
                            "Number of elements to initially retrieve from the index in a scan",
                            "Valid values are in range [1, 1000]",
                            &ldb_hnsw_init_k,
                            LDB_HNSW_DEFAULT_K,
                            1,
                            LDB_HNSW_MAX_K,
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("lantern_hnsw.ef",
                            "Expansion factor to use during vector search in a scan",
                            "Valid values are in range [1, 400]",
                            &ldb_hnsw_ef_search,
                            USEARCH_SEARCH_EF_INVALID_VALUE,
                            USEARCH_SEARCH_EF_INVALID_VALUE,
                            HNSW_MAX_EF,
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomBoolVariable("_lantern_internal.is_test",
                             "Whether or not the DB is in a regression test",
                             "set this to 1 to enable extra logging for use in lanterndb regression tests",
                             &ldb_is_test,
                             false,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("lantern.pgvector_compat",
                             "Whether or not the operator <-> should automatically detect the right distance function",
                             "set this to 1 to disable operator rewriting hooks",
                             &ldb_pgvector_compat,
                             true,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomIntVariable("lantern.external_index_port",
                            "Port for external indexing",
                            "Change this value if you run lantern daemon on different port",
                            &ldb_external_index_port,
                            8998,
                            80,
                            65535,
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("lantern.external_index_host",
                               "Host for external indexing",
                               "Change this value if you run lantern daemon on remote host",
                               &ldb_external_index_host,
                               "127.0.0.1",
                               PGC_USERSET,
                               0,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomBoolVariable("lantern.external_index_secure",
                             "Use SSL connection when connecting to external index socket",
                             "Set this to 0 to disable secure connection",
                             &ldb_external_index_secure,
                             true,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);
#if PG_VERSION_NUM >= 150000
    MarkGUCPrefixReserved("lantern");
    MarkGUCPrefixReserved("lantern_hnsw");
    MarkGUCPrefixReserved("_lantern_internal");
#endif

    original_post_parse_analyze_hook = post_parse_analyze_hook;
    original_ExecutorStart_hook = ExecutorStart_hook;

    post_parse_analyze_hook = post_parse_analyze_hook_with_operator_check;
    ExecutorStart_hook = ExecutorStart_hook_with_operator_check;

#ifndef NDEBUG
    signal(SIGSEGV, ldb_wait_for_gdb);
    signal(SIGABRT, ldb_wait_for_gdb);
#endif
}

// Called with extension unload.
void _PG_fini(void)
{
    // Return back the original hook value.
    // This check is because there might be case if while we stop the hooks (in pgvector_compat mode)
    // Another extension will be loaded and it will overwrite the hooks
    // And when lantern extension will be unloaded it will set the hooks to original values
    // Overwriting the current changed hooks set by another extension
    if(ExecutorStart_hook == ExecutorStart_hook_with_operator_check) {
        ExecutorStart_hook = original_ExecutorStart_hook;
    }
    if(post_parse_analyze_hook == post_parse_analyze_hook_with_operator_check) {
        post_parse_analyze_hook = original_post_parse_analyze_hook;
    }
}
