#include <postgres.h>

#include "options.h"

#include <access/htup_details.h>
#include <access/reloptions.h>
#include <catalog/pg_amproc.h>
#include <catalog/pg_type_d.h>
#include <executor/executor.h>
#include <fmgr.h>
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
#include "utils.h"

// reloption for lantern hnsw index creation paramters in
// CREATE INDEX ... WITH (...)
//                       ^^^^
static relopt_kind ldb_hnsw_index_withopts;

int ldb_hnsw_init_k;

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
    } else if(fnaddr == cos_dist) {
        return usearch_metric_cos_k;
    } else {
        elog(ERROR, "could not find distance function for index");
    }
}

/*
 * Parse and validate the reloptions
 */
bytea *ldb_amoptions(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[]
        = {{"dim", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, dim)},
           {"element_limit", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, element_limit)},
           {"m", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, m)},
           {"ef_construction", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, ef_construction)},
           {"ef", RELOPT_TYPE_INT, offsetof(ldb_HnswOptions, ef)},
           {"_experimental_index_path", RELOPT_TYPE_STRING, offsetof(ldb_HnswOptions, experimantal_index_path_offset)}};

#if PG_VERSION_NUM >= 130000
    LDB_UNUSED(validate);
    return (bytea *)build_reloptions(
        reloptions, true, ldb_hnsw_index_withopts, sizeof(ldb_HnswOptions), tab, lengthof(tab));
#else
    // todo::currently unused so out of date
    relopt_value    *options;
    int              numoptions;
    ldb_HnswOptions *rdopts;

    options = parseRelOptions(reloptions, validate, ldb_hnsw_index_withopts, &numoptions);
    rdopts = allocateReloptStruct(sizeof(ldb_HnswOptions), options, numoptions);
    fillRelOptions((void *)rdopts, sizeof(ldb_HnswOptions), options, numoptions, validate, tab, lengthof(tab));

    return (bytea *)rdopts;
#endif
}

/*
 * Initialize index options and variables
 */
void _PG_init(void)
{
    original_post_parse_analyze_hook = post_parse_analyze_hook;
    original_ExecutorStart_hook = ExecutorStart_hook;

    post_parse_analyze_hook = post_parse_analyze_hook_with_operator_check;
    ExecutorStart_hook = ExecutorStart_hook_with_operator_check;
    // todo:: cross-check with this`
    // https://github.com/zombodb/zombodb/blob/34c732a0b143b5e424ced64c96e8c4d567a14177/src/access_method/options.rs#L895
    ldb_hnsw_index_withopts = add_reloption_kind();
#if 0
	add_int_reloption(ldb_hnsw_index_withopts, "element_limit",
			  "Maximum table size (needed for hnswlib)",
			  HNSW_DEFAULT_ELEMENT_LIMIT, 1, HNSW_MAX_ELEMENT_LIMIT
#if PG_VERSION_NUM >= 130000
			  ,
			  AccessExclusiveLock
#endif
	);
#endif
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
                      "HNSW ef-construction hyperparameter",
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
    DefineCustomIntVariable("hnsw.init_k",
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

    DefineCustomBoolVariable("_lantern_internal.is_test",
                             "is this a lanterndb regression test",
                             "set this to 1 to enable extra logging for use in lanterndb regression tests",
                             &ldb_is_test,
                             false,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);
}

// Called with extension unload.
void _PG_fini(void)
{
    // Return back the original hook value.
    post_parse_analyze_hook = original_post_parse_analyze_hook;
    ExecutorStart_hook = original_ExecutorStart_hook;
}
