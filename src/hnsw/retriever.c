
#include <postgres.h>

#include "retriever.h"

#include <assert.h>
#include <common/relpath.h>
#include <storage/bufmgr.h>  // Buffer
#include <utils/hsearch.h>
#include <utils/relcache.h>

#include "external_index.h"

RetrieverCtx *ldb_wal_retriever_area_init(Relation index_rel, HnswIndexHeaderPage *header_page_under_wal, uint32 m)
{
    RetrieverCtx *ctx = palloc0(sizeof(RetrieverCtx));
    ctx->index_rel = index_rel;
    ctx->header_page_under_wal = header_page_under_wal;
    ctx->extra_dirted = extra_dirtied_new();
    ctx->takenbuffers_size = m * 2;
    ctx->takenbuffers_next = 0;
#if LANTERNDB_COPYNODES
    ctx->takenbuffers = palloc0(sizeof(char *) * ctx->takenbuffers_size);
#else
    ctx->takenbuffers = palloc0(sizeof(Buffer) * ctx->takenbuffers_size);
#endif

    return ctx;
}

void ldb_wal_retriever_area_reset(RetrieverCtx *ctx)
{
    for(uint32 i = 0; i < ctx->takenbuffers_size; i++) {
        if(ctx->takenbuffers[ i ]) {
#if LANTERNDB_COPYNODES
            pfree(ctx->takenbuffers[ i ]);
            ctx->takenbuffers[ i ] = NULL;
#else
            ReleaseBuffer(ctx->takenbuffers[ i ]);
            ctx->takenbuffers[ i ] = 0;
#endif
        }
    }
    ctx->takenbuffers_next = 0;
}

void ldb_wal_retriever_area_fini(RetrieverCtx *ctx)
{
    ldb_wal_retriever_area_reset(ctx);
    pfree(ctx->takenbuffers);
    extra_dirtied_free(ctx->extra_dirted);
}
