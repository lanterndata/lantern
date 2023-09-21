#include <postgres.h>
#include <access/htup_details.h>
#include <access/htup.h>
#include <access/relscan.h>
#include <storage/bufmgr.h>
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

ItemPointer GetTidFromLabel(usearch_label_t label)
{
    ItemPointer tid = (ItemPointer) palloc(sizeof(ItemPointerData));
    assert(tid != NULL);
    memcpy(tid, &label, 6);
    return tid;
}

// See heapam_scan_analyze_next_tuple for reference
HeapTuple GetTupleFromItemPointer(Relation rel, ItemPointer tid)
{
    Buffer buf;
    Page page;
    HeapTupleData tuple;
    OffsetNumber offset;
    ItemId itemId;

    // Get block number from the item pointer
    BlockNumber blocknum = ItemPointerGetBlockNumber(tid);
    offset = ItemPointerGetOffsetNumber(tid);

    // Read the specified block into a buffer
    buf = ReadBuffer(rel, blocknum);

    LockBuffer(buf, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buf);

    // Check if the offset is valid for the given page
    if (offset <= PageGetMaxOffsetNumber(page)) {
        itemId = PageGetItemId(page, offset);

        // Check if the item is valid
        if (ItemIdIsNormal(itemId)) {
            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = RelationGetRelid(rel);
            tuple.t_self = *tid;
        } else {
            tuple.t_data = NULL;
        }
    } else {
        tuple.t_data = NULL;
    }

    UnlockReleaseBuffer(buf);

    // Return a copy since we no longer own a lock
    if (tuple.t_data != NULL) {
        return heap_copytuple(&tuple);
    } else {
        return NULL;
    }
}
