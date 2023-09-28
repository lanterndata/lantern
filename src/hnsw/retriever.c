
#include <postgres.h>

#include "retriever.h"

#include <assert.h>
#include <common/relpath.h>
#include <pg_config.h>  // BLCKSZ
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "external_index.h"
#include "htab_cache.h"
#include "insert.h"

RetrieverCtx *ldb_wal_retriever_area_init(Relation index_rel, HnswIndexHeaderPage *header_page_under_wal)
{
    RetrieverCtx *ctx = palloc0(sizeof(RetrieverCtx));
    ctx->index_rel = index_rel;
    ctx->header_page_under_wal = header_page_under_wal;
    ctx->extra_dirted = extra_dirtied_new();

    ctx->node_cache = cache_create("NodeCache");

    // Allocate the hash mapping labels to index tuples for use in index only scans
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(HASHCTL));
    ctl.keysize = sizeof(unsigned long);
    ctl.entrysize = sizeof(BufferHash);
    // todo:: we can allocate based on the upper bound described in the paper, query the layer from the entry ndoe
    ctx->taken_hash = hash_create("heap tid -> index tuple", 2 * 1024, &ctl, HASH_ELEM | HASH_BLOBS);
    dlist_init(&ctx->takenbuffers);

    /* fill in a buffer with blockno index information, before spilling it to disk */
    ctx->block_numbers_cache = cache_create("BlockNumberCache");

    return ctx;
}

void ldb_wal_retriever_area_reset(RetrieverCtx *ctx, HnswIndexHeaderPage *header_page_under_wal)
{
    dlist_mutable_iter miter;
    dlist_foreach_modify(miter, &ctx->takenbuffers)
    {
        BufferNode *node = dlist_container(BufferNode, node, miter.cur);
#if LANTERNDB_COPYNODES
        pfree(node->buf);
#else
        if(node->buf != InvalidBuffer) {
            ReleaseBuffer(node->buf);
        }
#endif
        dlist_delete(miter.cur);
        pfree(node);
    }
    dlist_init(&ctx->takenbuffers);

    assert(ctx->header_page_under_wal == header_page_under_wal);
    ctx->header_page_under_wal = header_page_under_wal;
}

void ldb_wal_retriever_area_fini(RetrieverCtx *ctx)
{
    // Pointers on this should just be references to the buffer we release below
    // It seems like the containing context has expired at this point so no pfree
    hash_destroy(ctx->taken_hash);

    cache_destroy(&ctx->block_numbers_cache);
    cache_destroy(&ctx->node_cache);
    dlist_mutable_iter miter;
    dlist_foreach_modify(miter, &ctx->takenbuffers)
    {
        BufferNode *node = dlist_container(BufferNode, node, miter.cur);
#if LANTERNDB_COPYNODES
        pfree(node->buf);
#else
        if(node->buf != InvalidBuffer) {
            ReleaseBuffer(node->buf);
        }
#endif
        dlist_delete(miter.cur);
        pfree(node);
    }
    dlist_init(&ctx->takenbuffers);

    extra_dirtied_free(ctx->extra_dirted);
}
