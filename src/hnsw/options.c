#include <postgres.h>

#include "options.h"

#include <access/htup_details.h>
#include <access/reloptions.h>
#include <catalog/pg_amproc.h>
#include <executor/executor.h>
#include <fmgr.h>
#include <utils/catcache.h>
#include <utils/guc.h>
#include <utils/rel.h>  // RelationData
#include <utils/syscache.h>

// We import this header file
// to access the op class support function pointers
#include "../hnsw.h"
#include "utils.h"

// reloption for lanterndb hnsw index creation paramters in
// CREATE INDEX ... WITH (...)
//                       ^^^^
static relopt_kind ldb_hnsw_index_withopts;

static ExecutorStart_hook_type original_executor_start_hook = NULL;

int ldb_hnsw_init_k;

int HnswGetDims(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;
    if(opts) return opts->dims;
    return HNSW_DEFAULT_DIMS;
}

int HnswGetM(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;
    if(opts) return opts->m;
    return HNSW_DEFAULT_M;
}

int HnswGetEfConstruction(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;
    if(opts) return opts->ef_construction;
    return HNSW_DEFAULT_EF_CONSTRUCTION;
}

int HnswGetEf(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;
    if(opts) return opts->ef;
    return HNSW_DEFAULT_EF;
}

usearch_metric_kind_t HnswGetMetricKind(Relation index)
{
    struct catclist *proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(index->rd_opfamily[ 0 ]));

    if(&proclist->n_members == 0) {
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

static void HnswAlgorithmParamValidator(const char *value)
{
    if(value == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid algorithm value. Cannot b null"),
                 errhint("Valid values are: 'own', 'diskann', "
                         "'usearch', 'hnswlib'")));
    }

    if(strlen(value) > ALG_OPTION_MAX_STRING_LEN) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid algorithm value. Cannot be "
                        "longer than %d characters",
                        ALG_OPTION_MAX_STRING_LEN),
                 errhint("Valid values are: 'own', 'diskann', "
                         "'usearch', 'hnswlib'")));
    }

    if(strcmp(value, "own") != 0 && strcmp(value, "diskann") != 0 && strcmp(value, "usearch") != 0
       && strcmp(value, "hnswlib") != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid algorithm value. Valid values are: "
                        "'own', 'diskann', 'usearch', 'hnswlib'")));
    }
}

/*
 * Parse and validate the reloptions
 */
bytea *ldb_amoptions(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {{"dims", RELOPT_TYPE_INT, offsetof(HnswOptions, dims)},
                                           {"element_limit", RELOPT_TYPE_INT, offsetof(HnswOptions, element_limit)},
                                           {"m", RELOPT_TYPE_INT, offsetof(HnswOptions, m)},
                                           {"ef_construction", RELOPT_TYPE_INT, offsetof(HnswOptions, ef_construction)},
                                           {"ef", RELOPT_TYPE_INT, offsetof(HnswOptions, ef)},
                                           // todo:: alg reloption for some reason is not parsed properly.
                                           //  it puts the string "0" in the alg field and never anything else
                                           //  it puts 0x14 (or decimal 20) if alg is an int.
                                           //  or maybe it just does not put anything and does not even reset?
                                           {"alg", RELOPT_TYPE_STRING, offsetof(HnswOptions, alg)}};

#if PG_VERSION_NUM >= 130000
    return (bytea *)build_reloptions(
        reloptions, true, ldb_hnsw_index_withopts, sizeof(HnswOptions), tab, lengthof(tab));
#else
    // todo::currently unused so out of date
    relopt_value *options;
    int           numoptions;
    HnswOptions  *rdopts;

    options = parseRelOptions(reloptions, validate, ldb_hnsw_index_withopts, &numoptions);
    rdopts = allocateReloptStruct(sizeof(HnswOptions), options, numoptions);
    fillRelOptions((void *)rdopts, sizeof(HnswOptions), options, numoptions, validate, tab, lengthof(tab));

    return (bytea *)rdopts;
#endif
}

void executor_hook(QueryDesc *queryDesc, int eflags)
{
    if(original_executor_start_hook) {
        original_executor_start_hook(queryDesc, eflags);
    }

    if(CheckOperatorUsage(queryDesc->sourceText)) {
        elog(ERROR, "Operator <-> has no standalone meaning and is reserved for use in vector index lookups only");
    }
    standard_ExecutorStart(queryDesc, eflags);
}

/*
 * Initialize index options and variables
 */
void _PG_init(void)
{
    original_executor_start_hook = ExecutorStart_hook;
    ExecutorStart_hook = executor_hook;
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
                      "dims",
                      "Number of dimensions of the vector",
                      HNSW_DEFAULT_DIMS,
                      1,
                      HNSW_MAX_DIMS
#if PG_VERSION_NUM >= 130000
                      ,
                      AccessExclusiveLock
#endif
    );
    add_int_reloption(ldb_hnsw_index_withopts,
                      "m",
                      "HNSW M hyperparameter",
                      HNSW_DEFAULT_M,
                      1,
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
                         "alg",
                         "Algorithm or specific search library to use for vector search",
                         HNSW_DEFAULT_PROVIDER,
                         HnswAlgorithmParamValidator
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
}

// this is only applicable to hnswlib
// worry about it if/when it is back up to date again
#if 0
int
HnswGetElementLimit(Relation index)
{
	HnswOptions *opts = (HnswOptions *) index->rd_options;
	if (opts)
		return opts->element_limit;
		
	return HNSWLIB_DEFAULT_ELEMENT_LIMIT;
}
#endif

// Called with extension unload.
void _PG_fini(void)
{
    // Return back the original hook value.
    ExecutorStart_hook = original_executor_start_hook;
}
