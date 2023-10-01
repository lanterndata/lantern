#include <postgres.h>
#include <utils/rel.h>

#include "utils.h"

#include <assert.h>
#include <regex.h>
#include <string.h>

#include "hnsw.h"
#include "options.h"
#include "usearch.h"

void LogUsearchOptions(usearch_init_options_t *opts)
{
    /*todo:: in usearch.h create const char arrays like
char* scalar_names = {
    usearch_scalar_f32_k: "f32",
    usearch_scalar_f64_k: "f64"
}
so below the human readable string names can be printed
*/
    elog(INFO,
         "usearch_init_options_t: metric_kind: %d, metric: %p, "
         "quantization: %d, dimensions: %ld, connectivity: %ld, "
         "expansion_add: %ld, expansion_search: %ld",
         opts->metric_kind,
         opts->metric,
         opts->quantization,
         opts->dimensions,
         opts->connectivity,
         opts->expansion_add,
         opts->expansion_search);
}

void PopulateUsearchOpts(Relation index, usearch_init_options_t *opts)
{
    opts->connectivity = ldb_HnswGetM(index);
    opts->expansion_add = ldb_HnswGetEfConstruction(index);
    opts->expansion_search = ldb_HnswGetEf(index);
    opts->metric_kind = ldb_HnswGetMetricKind(index);
    opts->metric = NULL;
    opts->quantization = usearch_scalar_f32_k;
}

usearch_label_t GetUsearchLabel(ItemPointer itemPtr)
{
    usearch_label_t label = 0;
    memcpy((unsigned long *)&label, itemPtr, 6);
    return label;
}

void UsearchLabel2ItemPointer(usearch_label_t label, ItemPointer itemPtr)
{
    memcpy(itemPtr, &label, 6);
}

TupleDesc MakeNonkeyIndexTupleDesc(Relation index)
{
    // XXX do we need CreateTupleDescCopyConstr() here?
    TupleDesc tupleDesc = CreateTupleDescCopy(RelationGetDescr(index));
    TupleDescInitBuiltinEntry(tupleDesc, (AttrNumber)1, "heap_tid", INT8OID, -1, 0);
    return tupleDesc;
}

IndexTuple MakeNonkeyIndexTuple(TupleDesc    tupleDesc,
                                Datum       *values,
                                bool        *isnull,
                                ItemPointer  heap_tid)
{
    uint64	    heap_tid_uint64;
    int		    itup_natts;
    Datum 	   *itup_values;
    bool 	   *itup_isnull;
    IndexTuple  itup;

    assert(sizeof(*heap_tid) <= sizeof(heap_tid_uint64));
    heap_tid_uint64 = 0;
    memcpy(&heap_tid_uint64, heap_tid, sizeof(*heap_tid));

    itup_natts = tupleDesc->natts;
    itup_values = palloc_array(Datum, itup_natts);
    itup_isnull = palloc_array(bool, itup_natts);
    itup_values[0] = UInt64GetDatum(heap_tid_uint64);
    itup_isnull[0] = false;
    for (int i = 1; i < itup_natts; ++i) {
        itup_values[i] = values[i];
        itup_isnull[i] = isnull[i];
    }
    itup = index_form_tuple(tupleDesc, itup_values, itup_isnull);
    pfree(itup_isnull);
    pfree(itup_values);
    return itup;
}
