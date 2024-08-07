#include <postgres.h>

#include "external_index_socket.h"

#include <arpa/inet.h>
#include <hnsw/build.h>
#include <miscadmin.h>
#include <netdb.h>
#include <unistd.h>

static bool is_little_endian()
{
    int i = 1;

    return *((char *)&i) == 1;
}

static void set_read_timeout(int32 client_fd, uint32 seconds)
{
    struct timeval timeout;

    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if(setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout) < 0) {
        elog(ERROR, "external index: failed to set receive timeout for socket");
    }
}

static void set_write_timeout(int32 client_fd, uint32 seconds)
{
    struct timeval timeout;

    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if(setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout) < 0) {
        elog(ERROR, "external index: failed to set send timeout for socket");
    }
}

/* PLAIN SOCKET FUNCTIONS */
int   init_plain(external_index_socket_t *socket_con) { return 0; }
int64 read_plain(external_index_socket_t *socket_con, char *buf, size_t size)
{
    return read(socket_con->fd, buf, size);
};
int64 write_plain(external_index_socket_t *socket_con, const char *buf, size_t size)
{
    return write(socket_con->fd, buf, size);
};
void close_plain(external_index_socket_t *socket_con) { close(socket_con->fd); };
/* ====================== */

/*
 * We will try to create a non-blocking socket and use select syscall with specified timeout
 * After the select will return we will check if socket is writable convert it back to blocking mode and return 0 if so
 * else we will return -1 to indicate that connection attempt was failed.
 * We are using this approach because the process hangs waiting for blocking socket
 * when trying to connect for example to non-routable ip address
 * */
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
            return -1;
        }

        if(err) {
            errno = err;
            return -1;
        }
    }

    // Restore the socket to blocking mode
    if(fcntl(sockfd, F_SETFL, flags) == -1) {
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
static void check_external_index_response_error(external_index_socket_t *socket_con, char *buffer, int64 size)
{
    uint32 hdr;
    if(size < 0) {
        socket_con->close(socket_con);
        elog(ERROR, "external index socket read failed");
    }

    if(size < sizeof(uint32)) return;

    memcpy(&hdr, buffer, sizeof(uint32));

    if(hdr != EXTERNAL_INDEX_ERR_MSG) return;

    // append nullbyte
    buffer[ size ] = '\0';
    socket_con->close(socket_con);
    elog(ERROR, "external index error: %s", buffer + EXTERNAL_INDEX_MAGIC_MSG_SIZE);
}

static void check_external_index_request_error(external_index_socket_t *socket_con, int64 bytes_written)
{
    if(bytes_written > 0) return;

    socket_con->close(socket_con);
    elog(ERROR, "external index socket send failed");
}

static void writeall_or_error(external_index_socket_t *socket_con, const char *buf, uint32 len, int flags)
{
    int32  total = 0;
    uint32 bytesleft = len;
    int64  n;

    while(total < len) {
        n = socket_con->write(socket_con, buf + total, bytesleft);
        check_external_index_request_error(socket_con, n);
        total += n;
        bytesleft -= (uint32)n;
    }
}

void external_index_send_codebook(external_index_socket_t *socket_con,
                                  float                   *codebook,
                                  uint32                   dimensions,
                                  uint32                   num_centroids,
                                  uint32                   num_subvectors)
{
    uint32 data_size = dimensions * sizeof(float);
    char   buf[ data_size ];

    for(uint32 i = 0; i < num_centroids; i++) {
        memcpy(buf, &codebook[ i * dimensions ], data_size);
        writeall_or_error(socket_con, buf, data_size, 0);
    }

    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    writeall_or_error(socket_con, (char *)&end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);
}

external_index_socket_t *create_external_index_session(const char                   *host,
                                                       int                           port,
                                                       bool                          secure,
                                                       const usearch_init_options_t *params,
                                                       const ldb_HnswBuildState     *buildstate,
                                                       uint32                        estimated_row_count)
{
    external_index_socket_t *socket_con = palloc0(sizeof(external_index_socket_t));
    int                      client_fd, status;
    char                     init_buf[ sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE ];
    char                     port_str[ 5 ];
    struct addrinfo         *serv_addr, hints = {0};
    char                     init_response[ EXTERNAL_INDEX_INIT_BUFFER_SIZE ] = {0};

    if(!is_little_endian()) {
        elog(ERROR, "external indexing is supported only for little endian byte ordering");
    }

    elog(INFO, "connecting to external indexing daemon on %s:%d", host, port);

#ifdef LANTERN_USE_OPENSSL
    if(secure) {
        socket_con->init = (void *)init_ssl;
        socket_con->read = (void *)read_ssl;
        socket_con->write = (void *)write_ssl;
        socket_con->close = (void *)close_ssl;
    }
#else
    if(secure) {
        elog(ERROR,
             "Can not use secure connection as Postgres is not compiled with openssl support. Set "
             "'lantern.external_index_secure=false' and retry");
    }
#endif  // ifdef LANTERN_USE_OPENSSL
    if(!secure) {
        socket_con->init = (void *)init_plain;
        socket_con->read = (void *)read_plain;
        socket_con->write = (void *)write_plain;
        socket_con->close = (void *)close_plain;
    }

    if((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        elog(ERROR, "external index: socket creation failed");
    }

    socket_con->fd = client_fd;
    hints.ai_socktype = SOCK_STREAM;  // TCP socket
    snprintf(port_str, 5, "%u", port);
    status = getaddrinfo(host, port_str, &hints, &serv_addr);

    if(status != 0) {
        elog(ERROR, "external index: getaddrinfo %s\n", gai_strerror(status));
    }

    set_write_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);
    set_read_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);

    if((status
        = connect_with_timeout(client_fd, serv_addr->ai_addr, serv_addr->ai_addrlen, EXTERNAL_INDEX_SOCKET_TIMEOUT))
       < 0) {
        elog(ERROR, "external index: connection with server failed");
    }

    socket_con->init(socket_con);

    external_index_params_t index_params = {
        .pq = params->pq,
        .metric_kind = params->metric_kind,
        .quantization = params->quantization,
        .dim = (uint32)params->dimensions,
        .m = (uint32)params->connectivity,
        .ef_construction = (uint32)params->expansion_add,
        .ef = (uint32)params->expansion_search,
        .num_centroids = (uint32)params->num_centroids,
        .num_subvectors = (uint32)params->num_subvectors,
        .estimated_capcity = estimated_row_count,
    };

    uint32 hdr_msg = EXTERNAL_INDEX_INIT_MSG;
    memcpy(init_buf, &hdr_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE);
    memcpy(init_buf + EXTERNAL_INDEX_MAGIC_MSG_SIZE, &index_params, sizeof(external_index_params_t));
    writeall_or_error(socket_con, init_buf, sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    if(params->pq) {
        external_index_send_codebook(socket_con,
                                     buildstate->pq_codebook,
                                     index_params.dim,
                                     index_params.num_centroids,
                                     index_params.num_subvectors);
    }

    int64 buf_size = socket_con->read(socket_con, (char *)&init_response, EXTERNAL_INDEX_INIT_BUFFER_SIZE);

    check_external_index_response_error(socket_con, (char *)init_response, buf_size);

    return socket_con;
}

void external_index_receive_index_file(external_index_socket_t *socket_con,
                                       uint64                  *num_added_vectors,
                                       char                   **result_buf)
{
    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    char   buffer[ sizeof(uint64_t) ];
    int64  bytes_read;
    uint64 index_size = 0, total_received = 0;

    // disable read timeout while indexing is in progress
    set_read_timeout(socket_con->fd, 0);
    // send message indicating that we have finished streaming tuples
    writeall_or_error(socket_con, (char *)&end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    // read how many tuples have been indexed
    bytes_read = socket_con->read(socket_con, buffer, sizeof(uint64));
    check_external_index_response_error(socket_con, buffer, bytes_read);
    memcpy(num_added_vectors, buffer, sizeof(uint64));

    // read index file size
    bytes_read = socket_con->read(socket_con, buffer, sizeof(uint64));
    check_external_index_response_error(socket_con, buffer, bytes_read);
    memcpy(&index_size, buffer, sizeof(uint64));

    *result_buf = palloc0(index_size);

    if(*result_buf == NULL) {
        elog(ERROR, "external index: failed to allocate buffer for index file");
    }

    set_read_timeout(socket_con->fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);
    // start reading index into buffer
    while(total_received < index_size) {
        bytes_read = socket_con->read(socket_con, *result_buf + total_received, EXTERNAL_INDEX_FILE_BUFFER_SIZE);

        // Check for CTRL-C interrupts
        if(INTERRUPTS_PENDING_CONDITION()) {
            socket_con->close(socket_con);
            ProcessInterrupts();
        }

        check_external_index_response_error(socket_con, (char *)*result_buf + total_received, bytes_read);

        if(bytes_read == 0) {
            break;
        }

        total_received += (uint32)bytes_read;
    }
}

void external_index_send_tuple(
    external_index_socket_t *socket_con, usearch_label_t *label, void *vector, uint8 scalar_bits, uint32 dimensions)
{
    char   tuple[ EXTERNAL_INDEX_MAX_TUPLE_SIZE ];
    uint32 tuple_size;
    uint32 vector_size;
    uint32 dims = dimensions;

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
    writeall_or_error(socket_con, tuple, tuple_size, 0);
}
