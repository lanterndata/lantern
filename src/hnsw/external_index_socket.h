#ifndef LDB_EXTERNAL_IDX_SOCKET_H
#define LDB_EXTERNAL_IDX_SOCKET_H
#include <postgres.h>

#include <hnsw/build.h>

#include "usearch.h"

#define EXTERNAL_INDEX_MAGIC_MSG_SIZE   4
#define EXTERNAL_INDEX_INIT_MSG         0x13333337
#define EXTERNAL_INDEX_END_MSG          0x31333337
#define EXTERNAL_INDEX_ERR_MSG          0x37333337
#define EXTERNAL_INDEX_INIT_BUFFER_SIZE 1024
#define EXTERNAL_INDEX_FILE_BUFFER_SIZE 1024 * 1024 * 10  // 10MB
#define EXTERNAL_INDEX_SOCKET_TIMEOUT   10                // 10 seconds
// maximum tuple size can be 8kb (8192 byte) + 8 byte label
#define EXTERNAL_INDEX_MAX_TUPLE_SIZE 8200

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

void external_index_send_codebook(
    uint32 client_fd, float *codebook, uint32 dimensions, uint32 num_centroids, uint32 num_subvectors);
int  create_external_index_session(const char                   *host,
                                   int                           port,
                                   const usearch_init_options_t *params,
                                   const ldb_HnswBuildState     *buildstate,
                                   uint32                        estimated_row_count);
void check_external_index_response_error(uint32 client_fd, unsigned char *buffer, int32 size);
void external_index_receive_index_file(uint32 external_client_fd, uint64 *num_added_vectors, char **result_buf);
void check_external_index_request_error(uint32 client_fd, int32 bytes_written);
void external_index_send_tuple(
    uint32 external_client_fd, usearch_label_t *label, void *vector, uint8 scalar_bits, uint32 dimensions);

#endif  // LDB_EXTERNAL_IDX_SOCKET_H
