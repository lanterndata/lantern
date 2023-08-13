
#include "postgres.h"

#include "retriever.h"

#include <access/generic_xlog.h>  // GenericXLog
#include <assert.h>
#include <utils/hsearch.h>

#include "cache.h"
#include "common/relpath.h"
#include "external_index.h"
#include "insert.h"
#include "pg_config.h"  // BLCKSZ
#include "retriever.h"
#include "storage/bufmgr.h"  // Buffer
#include "usearch.h"
#include "utils/relcache.h"

Relation            INDEX_RELATION_FOR_RETRIEVER;
HnswIndexHeaderPage HEADER_FOR_EXTERNAL_RETRIEVER;
Buffer             *EXTRA_DIRTIED;
Page               *EXTRA_DIRTIED_PAGE;
int                 EXTRA_DIRTIED_SIZE = 0;

#if LANTERNDB_COPYNODES
static char *wal_retriever_area = NULL;
static int   wal_retriever_area_size = 0;
static int   wal_retriever_area_offset = 0;
#else

// static Buffer *takenbuffers;
// static int     takenbuffers_next = 0;
#endif

RetrieverCtx *ldb_wal_retriever_area_init()
{
    RetrieverCtx *ctx = palloc0(sizeof(RetrieverCtx));
#if LANTERNDB_COPYNODES
    wal_retriever_area = palloc(size);
    if(wal_retriever_area == NULL) elog(ERROR, "could not allocate wal_retriever_area");
    wal_retriever_area_size = size;
    wal_retriever_area_offset = 0;
#else
    ctx->takenbuffers = palloc0(sizeof(Buffer) * TAKENBUFFERS_MAX);
    if(ctx->takenbuffers_next > 0) {
        elog(ERROR, "takenbuffers_next > 0 %d", ctx->takenbuffers_next);
    }
#endif

    if(HEADER_FOR_EXTERNAL_RETRIEVER.num_vectors < 0) {
        elog(ERROR, "ldb_wal_retriever_area_init called with num_vectors < 0");
    }
    /* fill in a buffer with blockno index information, before spilling it to disk */
    ctx->block_numbers_cache = cache_create();

    if(EXTRA_DIRTIED_SIZE > 0) {
        elog(INFO, "EXTRA_DIRTIED_SIZE > 0 %d", EXTRA_DIRTIED_SIZE);
        for(int i = 0; i < EXTRA_DIRTIED_SIZE; i++) {
            elog(INFO, "buf %d in extra_dirtied : %d", i, EXTRA_DIRTIED[ i ]);
        }
    }
    return ctx;
}

void ldb_wal_retriever_area_reset(RetrieverCtx *ctx)
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
#endif
}

void ldb_wal_retriever_area_free(RetrieverCtx *ctx)
{
    cache_destroy(&ctx->block_numbers_cache);
#if LANTERNDB_COPYNODES
    pfree(wal_retriever_area);
    wal_retriever_area = NULL;
    wal_retriever_area_size = 0;
    wal_retriever_area_offset = 0;
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
}