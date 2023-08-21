
#include <postgres.h>

#include "retriever.h"

#include <assert.h>
#include <common/relpath.h>
#include <pg_config.h>  // BLCKSZ
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "cache.h"
#include "external_index.h"
#include "insert.h"

RetrieverCtx *ldb_wal_retriever_area_init(Relation index_rel, HnswIndexHeaderPage *header_page_under_wal)
{
    RetrieverCtx *ctx = palloc0(sizeof(RetrieverCtx));
    ctx->index_rel = index_rel;
    ctx->header_page_under_wal = header_page_under_wal;
    ctx->extra_dirted = extra_dirtied_new(index_rel);

#if LANTERNDB_COPYNODES
    ctx->wal_retriever_area = palloc(BLCKSZ * 100);
    ctx->wal_retriever_area_size = BLCKSZ * 100;
    ctx->wal_retriever_area_offset = 0;
#else
    ctx->takenbuffers = palloc0(sizeof(Buffer) * TAKENBUFFERS_MAX);
#endif

    /* fill in a buffer with blockno index information, before spilling it to disk */
    ctx->block_numbers_cache = cache_create();

    return ctx;
}

void ldb_wal_retriever_area_reset(RetrieverCtx *ctx, HnswIndexHeaderPage *header_page_under_wal)
{
#if LANTERNDB_COPYNODES
    wal_retriever_area_offset = 0;
#else
    for(int i = 0; i < TAKENBUFFERS_MAX; i++) {
        if(ctx->takenbuffers[ i ] == InvalidBuffer) {
            continue;
        }
        ReleaseBuffer(ctx->takenbuffers[ i ]);
        ctx->takenbuffers[ i ] = InvalidBuffer;
    }
    ctx->takenbuffers_next = 0;
    assert(ctx->header_page_under_wal == header_page_under_wal);
    ctx->header_page_under_wal = header_page_under_wal;
#endif
}

void ldb_wal_retriever_area_fini(RetrieverCtx *ctx)
{
    cache_destroy(&ctx->block_numbers_cache);
#if LANTERNDB_COPYNODES
    pfree(ctx->wal_retriever_area);
    ctx->wal_retriever_area = NULL;
    ctx->wal_retriever_area_size = 0;
    ctx->wal_retriever_area_offset = 0;
#else
    for(int i = 0; i < TAKENBUFFERS_MAX; i++) {
        if(ctx->takenbuffers[ i ] == InvalidBuffer) {
            continue;
        }
        ReleaseBuffer(ctx->takenbuffers[ i ]);
        ctx->takenbuffers[ i ] = InvalidBuffer;
    }
    pfree(ctx->takenbuffers);
    ctx->takenbuffers_next = 0;
#endif

    extra_dirtied_free(ctx->extra_dirted);
}