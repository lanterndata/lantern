#include <postgres.h>

#include "utils.h"

#include <assert.h>
#include <math.h>
#include <miscadmin.h>
#include <regex.h>
#include <string.h>

#if PG_VERSION_NUM >= 120000
#include <utils/memutils.h>
#endif

#include "external_index.h"
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

void CheckMem(int limit, Relation index, usearch_index_t uidx, uint32 n_nodes, char *msg)
{
    uint32 node_size = 0;
    if(index != NULL) {
        usearch_error_t    error;
        double             M = ldb_HnswGetM(index);
        double             mL = 1 / log(M);
        usearch_metadata_t meta = usearch_metadata(uidx, &error);
        // todo:: update sizeof(float) to correct vector size once #19 is merged
        node_size = UsearchNodeBytes(&meta, meta.dimensions * sizeof(float), (int)round(mL + 1));
    }
    // todo:: there's figure out a way to check this in pg <= 12
#if PG_VERSION_NUM >= 120000
    Size pg_mem = MemoryContextMemAllocated(CurrentMemoryContext, true);
#else
    Size pg_mem = 0;
#endif

    // The average number of layers for an element to be added in is mL+1 per section 4.2.2
    // Accuracy could maybe be improved by not rounding
    // This is a guess, but it's a reasonably good one
    if(pg_mem + node_size * n_nodes > (uint32)limit * 1024UL) {
        elog(WARNING, "%s", msg);
    }
}
