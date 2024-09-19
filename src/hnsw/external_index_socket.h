#ifndef LDB_EXTERNAL_IDX_SOCKET_H
#define LDB_EXTERNAL_IDX_SOCKET_H
#include <postgres.h>

#include "build.h"
#include "external_index_socket_ssl.h"
#include "usearch.h"

#define EXTERNAL_INDEX_MAGIC_MSG_SIZE        4
#define EXTERNAL_INDEX_INIT_MSG              0x13333337
#define EXTERNAL_INDEX_END_MSG               0x31333337
#define EXTERNAL_INDEX_ERR_MSG               0x37333337
#define EXTERNAL_INDEX_MAX_ERR_SIZE          1024
#define EXTERNAL_INDEX_INIT_BUFFER_SIZE      1024
#define EXTERNAL_INDEX_FILE_BUFFER_SIZE      1024 * 1024 * 10  // 10MB
#define EXTERNAL_INDEX_SOCKET_TIMEOUT        10                // 10 seconds
#define EXTERNAL_INDEX_ROUTER_SOCKET_TIMEOUT 600               // 10 minutes
// maximum tuple size can be 8kb (8192 byte) + 8 byte label
#define EXTERNAL_INDEX_MAX_TUPLE_SIZE     8200
#define EXTERNAL_INDEX_PROTOCOL_VERSION   1
#define EXTERNAL_INDEX_ROUTER_SERVER_TYPE 0x2

typedef struct external_index_params_t
{
    uint32                pq;
    usearch_metric_kind_t metric_kind;
    usearch_scalar_kind_t quantization;
    uint32                dim;
    uint32                m;
    uint32                ef_construction;
    uint32                ef;
    uint32                num_centroids;
    uint32                num_subvectors;
    uint32                estimated_capcity;

} external_index_params_t;

typedef struct external_index_socket_t
{
    int      fd;
    SSL_CTX *ssl_ctx;
    SSL     *ssl;
    int (*init)(struct external_index_socket_t *self);
    int64 (*read)(struct external_index_socket_t *self, char *buf, size_t size);
    int64 (*write)(struct external_index_socket_t *self, const char *buf, size_t size);
    void (*close)(struct external_index_socket_t *self);
} external_index_socket_t;

/* PLAIN SOCKET FUNCTIONS */
int   init_plain(external_index_socket_t *socket_con);
int64 read_plain(external_index_socket_t *socket_con, char *buf, size_t size);
int64 write_plain(external_index_socket_t *socket_con, const char *buf, size_t size);
void  close_plain(external_index_socket_t *socket_con);
/* ====================== */

/* SSL SOCKET FUNCTIONS */
int   init_ssl(external_index_socket_t *socket_con);
int64 read_ssl(external_index_socket_t *socket_con, char *buf, uint32 size);
int64 write_ssl(external_index_socket_t *socket_con, const char *buf, uint32 size);
void  close_ssl(external_index_socket_t *socket_con);
/* ====================== */

void   create_external_index_session(const char                   *host,
                                     int                           port,
                                     bool                          secure,
                                     const usearch_init_options_t *params,
                                     const ldb_HnswBuildState     *buildstate,
                                     uint32                        estimated_row_count);
void   external_index_receive_metadata(external_index_socket_t *socket_con,
                                       uint64                  *num_added_vectors,
                                       uint64                  *index_size);
uint64 external_index_read_all(external_index_socket_t *socket_con, char *result_buf, uint64 size);
void   external_index_send_tuple(
      external_index_socket_t *socket_con, usearch_label_t *label, void *vector, uint8 scalar_bits, uint32 dimensions);

#endif  // LDB_EXTERNAL_IDX_SOCKET_H
