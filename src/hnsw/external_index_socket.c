#include <postgres.h>

#include "external_index_socket.h"

#include <arpa/inet.h>
#include <hnsw/build.h>
#include <miscadmin.h>
#include <sys/socket.h>
#include <unistd.h>

static bool is_little_endian()
{
    int i = 1;

    return *((char *)&i) == 1;
}

static void set_read_timeout(uint32 client_fd, uint32 seconds)
{
    struct timeval timeout;

    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if(setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout) < 0) {
        elog(ERROR, "external index: failed to set receive timeout for socket");
    }
}

static void set_write_timeout(uint32 client_fd, uint32 seconds)
{
    struct timeval timeout;

    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if(setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout) < 0) {
        elog(ERROR, "external index: failed to set send timeout for socket");
    }
}

static int connect_with_timeout(int sockfd, const struct sockaddr *addr, socklen_t addrlen, int timeout)
{
    // Set the socket to non-blocking mode
    int flags = fcntl(sockfd, F_GETFL, 0);
    if(flags == -1) {
        perror("fcntl F_GETFL");
        return -1;
    }
    if(fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl F_SETFL");
        return -1;
    }

    // Attempt to connect
    int result = connect(sockfd, addr, addrlen);
    if(result == -1 && errno != EINPROGRESS) {
        perror("connect");
        return -1;
    }

    if(result == 0) {
        // Connection succeeded immediately
        return 0;
    }

    // Wait for the socket to become writable within the timeout period
    fd_set writefds;
    FD_ZERO(&writefds);
    FD_SET(sockfd, &writefds);

    struct timeval tv;
    tv.tv_sec = timeout;
    tv.tv_usec = 0;

    result = select(sockfd + 1, NULL, &writefds, NULL, &tv);
    if(result == -1) {
        perror("select");
        return -1;
    } else if(result == 0) {
        // Timeout occurred
        errno = ETIMEDOUT;
        return -1;
    } else {
        // Socket is writable, check for errors
        int       err;
        socklen_t len = sizeof(err);
        if(getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len) == -1) {
            perror("getsockopt");
            return -1;
        }
        if(err) {
            errno = err;
            return -1;
        }
    }

    // Restore the socket to blocking mode
    if(fcntl(sockfd, F_SETFL, flags) == -1) {
        perror("fcntl F_SETFL");
        return -1;
    }

    return 0;
}

/**
 * Check for error received from socket response
 * This function will return void or elog(ERROR) and exit process
 * Error conditions are the following:
 *  - read size is less then zero. (this can happen on network errors, or when the server will be closed)
 *  - packet starts with EXTERNAL_INDEX_ERR_MSG bytes. (this will be send from the server indicating that something gone
 *    wrong in the server and the following bytes will be error message and will be interpreted as string in
 *    elog(ERROR))
 */
void check_external_index_response_error(uint32 client_fd, unsigned char *buffer, int32 size)
{
    uint32 hdr;
    if(size < 0) {
        close(client_fd);
        elog(ERROR, "external index socket read failed");
    }

    if(size < sizeof(uint32)) return;

    memcpy(&hdr, buffer, sizeof(uint32));

    if(hdr != EXTERNAL_INDEX_ERR_MSG) return;

    // append nullbyte
    buffer[ size ] = '\0';
    close(client_fd);
    elog(ERROR, "external index error: %s", buffer + EXTERNAL_INDEX_MAGIC_MSG_SIZE);
}

void check_external_index_request_error(uint32 client_fd, int32 bytes_written)
{
    if(bytes_written > 0) return;

    close(client_fd);
    elog(ERROR, "external index socket send failed");
}

void external_index_send_codebook(
    uint32 client_fd, float *codebook, uint32 dimensions, uint32 num_centroids, uint32 num_subvectors)
{
    int           data_size = dimensions * sizeof(float);
    int           bytes_written = -1;
    unsigned char buf[ data_size ];

    for(int i = 0; i < num_centroids; i++) {
        memcpy(buf, &codebook[ i * dimensions ], data_size);
        bytes_written = send(client_fd, buf, data_size, 0);
        check_external_index_request_error(client_fd, bytes_written);
    }

    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    bytes_written = send(client_fd, &end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    check_external_index_request_error(client_fd, bytes_written);
}

int create_external_index_session(const char                   *host,
                                  int                           port,
                                  const usearch_init_options_t *params,
                                  const ldb_HnswBuildState     *buildstate,
                                  uint32                        estimated_row_count)
{
    int                client_fd, status;
    unsigned char      init_buf[ sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE ];
    struct sockaddr_in serv_addr;
    unsigned char      init_response[ EXTERNAL_INDEX_INIT_BUFFER_SIZE ] = {0};

    if(!is_little_endian()) {
        elog(ERROR, "external indexing is supported only for little endian byte ordering");
    }
    elog(INFO, "connecting to external indexing daemon on %s:%d", host, port);

    if((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        elog(ERROR, "external index: socket creation failed");
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if(inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        elog(ERROR, "external index: invalid address");
    }

    set_write_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);
    set_read_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);

    if((status = connect_with_timeout(
            client_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr), EXTERNAL_INDEX_SOCKET_TIMEOUT))
       < 0) {
        elog(ERROR, "external index: connection with server failed");
    }

    external_index_params_t index_params = {
        .pq = params->pq,
        .metric_kind = params->metric_kind,
        .quantization = params->quantization,
        .dim = params->dimensions,
        .m = params->connectivity,
        .ef_construction = params->expansion_add,
        .ef = params->expansion_search,
        .num_centroids = params->num_centroids,
        .num_subvectors = params->num_subvectors,
        .estimated_capcity = estimated_row_count,
    };

    uint32 hdr_msg = EXTERNAL_INDEX_INIT_MSG;
    memcpy(init_buf, &hdr_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE);
    memcpy(init_buf + EXTERNAL_INDEX_MAGIC_MSG_SIZE, &index_params, sizeof(external_index_params_t));
    uint32 bytes_written
        = send(client_fd, init_buf, sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    check_external_index_request_error(client_fd, bytes_written);

    if(params->pq) {
        external_index_send_codebook(
            client_fd, buildstate->pq_codebook, params->dimensions, params->num_centroids, params->num_subvectors);
    }

    uint32 buf_size = read(client_fd, &init_response, EXTERNAL_INDEX_INIT_BUFFER_SIZE);

    check_external_index_response_error(client_fd, init_response, buf_size);

    return client_fd;
}

void external_index_receive_index_file(uint32 external_client_fd, uint64 *num_added_vectors, char **result_buf)
{
    uint32        end_msg = EXTERNAL_INDEX_END_MSG;
    unsigned char buffer[ sizeof(uint64_t) ];
    int32         bytes_read, bytes_written;
    uint64        index_size = 0, total_received = 0;

    // disable read timeout while indexing is in progress
    set_read_timeout(external_client_fd, 0);
    // send message indicating that we have finished streaming tuples
    bytes_written = send(external_client_fd, &end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);
    check_external_index_request_error(external_client_fd, bytes_written);

    // read how many tuples have been indexed
    bytes_read = read(external_client_fd, buffer, sizeof(uint64));
    check_external_index_response_error(external_client_fd, buffer, bytes_read);
    memcpy(num_added_vectors, buffer, sizeof(uint64));

    // read index file size
    bytes_read = read(external_client_fd, buffer, sizeof(uint64));
    check_external_index_response_error(external_client_fd, buffer, bytes_read);
    memcpy(&index_size, buffer, sizeof(uint64));

    *result_buf = palloc0(index_size);

    if(*result_buf == NULL) {
        elog(ERROR, "external index: failed to allocate buffer for index file");
    }

    set_read_timeout(external_client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);
    // start reading index into buffer
    while(total_received < index_size) {
        bytes_read = read(external_client_fd, *result_buf + total_received, EXTERNAL_INDEX_FILE_BUFFER_SIZE);

        // Check for CTRL-C interrupts
        if(INTERRUPTS_PENDING_CONDITION()) {
            close(external_client_fd);
            ProcessInterrupts();
        }

        check_external_index_response_error(
            external_client_fd, (unsigned char *)*result_buf + total_received, bytes_read);

        if(bytes_read == 0) {
            break;
        }

        total_received += bytes_read;
    }
}

void external_index_send_tuple(
    uint32 external_client_fd, usearch_label_t *label, void *vector, uint8 scalar_bits, uint32 dimensions)
{
    unsigned char tuple[ EXTERNAL_INDEX_MAX_TUPLE_SIZE ];
    uint32        tuple_size, bytes_written;
    uint32        vector_size;
    uint32        dims = dimensions;

    if(scalar_bits < CHAR_BIT) {
        dims = dimensions * (sizeof(uint32) * CHAR_BIT);
        vector_size = (dims + CHAR_BIT - 1) / CHAR_BIT;  // ceiling division
        tuple_size = sizeof(usearch_label_t) + vector_size;
    } else {
        tuple_size = sizeof(usearch_label_t) + dimensions * (scalar_bits / CHAR_BIT);
    }
    // send tuple over socket if this is external indexing
    memcpy(tuple, label, sizeof(usearch_label_t));
    memcpy(tuple + sizeof(usearch_label_t), vector, tuple_size - sizeof(usearch_label_t));
    bytes_written = send(external_client_fd, tuple, tuple_size, 0);
    check_external_index_request_error(external_client_fd, bytes_written);
}
