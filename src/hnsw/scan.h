#ifndef LDB_HNSW_SCAN_H
#define LDB_HNSW_SCAN_H
#include <postgres.h>

#include <access/reloptions.h>
#include <assert.h>

#include "hnsw.h"
#include "lib_interface.h"
#include "retriever.h"
#include "usearch.h"

typedef struct HnswScanState
{
    Buffer           buf;
    ItemPointer      iptr;
    float           *distances;
    usearch_label_t *labels;
    HnswColumnType   columnType;
    int              dimensions;
    // indicates whether we are retrieving the first tuple
    // actual vector-search is run when the first tuple is requested
    bool first;
    // used and advanced through gettupple calls
    int current;
    // set when the distances and labels are populated
    int             count;
    hnsw_t          hnsw;
    usearch_index_t usearch_index;

    RetrieverCtx *retriever_ctx;
} HnswScanState;

IndexScanDesc ldb_ambeginscan(Relation index, int nkeys, int norderbys);
void          ldb_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool          ldb_amgettuple(IndexScanDesc scan, ScanDirection dir);
void          ldb_amendscan(IndexScanDesc scan);
#endif  // LDB_HNSW_SCAN_H
