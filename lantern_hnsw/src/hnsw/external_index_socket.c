#include <postgres.h>

#include "external_index_socket.h"

#include <arpa/inet.h>
#include <miscadmin.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>

#include "build.h"
#include "failure_point.h"

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

    if(setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout) < 0
       || LDB_FAILURE_POINT_IS_ENABLED("crash_after_set_recv_timeout")) {
        elog(ERROR, "external index: failed to set receive timeout for socket");
    }
}

static void set_write_timeout(int32 client_fd, uint32 seconds)
{
    struct timeval timeout;

    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if(setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout) < 0
       || LDB_FAILURE_POINT_IS_ENABLED("crash_after_set_send_timeout")) {
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
    if(flags == -1 || LDB_FAILURE_POINT_IS_ENABLED("crash_after_get_flags")) {
        return -1;
    }
    if(fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1
       || LDB_FAILURE_POINT_IS_ENABLED("crash_after_set_non_blocking")) {
        return -1;
    }

    // Attempt to connect
    int result = connect(sockfd, addr, addrlen);
    if((result == -1 && errno != EINPROGRESS) || LDB_FAILURE_POINT_IS_ENABLED("crash_after_connect")) {
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
    if(result == -1 || LDB_FAILURE_POINT_IS_ENABLED("crash_after_select")) {
        return -1;
    } else if(result == 0 || LDB_FAILURE_POINT_IS_ENABLED("crash_on_timeout")) {
        // Timeout occurred
        errno = ETIMEDOUT;
        return -1;
    } else {
        // Socket is writable, check for errors
        int       err;
        socklen_t len = sizeof(err);
        if(getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len) == -1
           || LDB_FAILURE_POINT_IS_ENABLED("crash_after_getsockopts")) {
            return -1;
        }

        if(err || LDB_FAILURE_POINT_IS_ENABLED("crash_after_getsockopts_err")) {
            errno = err;
            return -1;
        }
    }

    // Restore the socket to blocking mode
    if(fcntl(sockfd, F_SETFL, flags) == -1 || LDB_FAILURE_POINT_IS_ENABLED("crash_after_set_blocking")) {
        return -1;
    }

    return 0;
}

static void wait_for_data(external_index_socket_t *socket_con)
{
    struct timeval timeout;
    fd_set         read_fds;
    char           errstr[ EXTERNAL_INDEX_MAX_ERR_SIZE ];

    // Set the socket to non-blocking mode
    int flags = fcntl(socket_con->fd, F_GETFL, 0);
    if(flags == -1) {
        elog(ERROR, "error getting socket flags");
    }

    if(fcntl(socket_con->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        elog(ERROR, "error setting socket to non-blocking mode");
    }

    while(1) {
        FD_ZERO(&read_fds);
        FD_SET(socket_con->fd, &read_fds);

        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int activity = select(socket_con->fd + 1, &read_fds, NULL, NULL, &timeout);

        // Check for interrupts on each iteration
        CHECK_FOR_INTERRUPTS();

        if(activity < 0) {
            // Sometimes the select syscall may be interrupted by signals
            // If this signals are important they would be handled in CHECK_FOR_INTERRUPTS()
            // If after calling CHECK_FOR_INTERRUPTS() we are still here we can ignore the signal
            if(errno == EINTR) continue;
            snprintf((char *)&errstr, EXTERNAL_INDEX_MAX_ERR_SIZE, "%s", strerror(errno));
            elog(ERROR, "select syscall error: %s", errstr);
        }

        // If socket has data to read
        if(FD_ISSET(socket_con->fd, &read_fds)) {
            // Restore the socket to blocking mode
            if(fcntl(socket_con->fd, F_SETFL, flags) == -1) {
                elog(ERROR, "error setting socket to blocking mode");
            }
            return;
        }
    }
}

/**
 * Check for error received from socket response
 * This function will return void or elog(ERROR)
 * Error conditions are the following:
 *  - read size is less then zero. (this can happen on network errors, or when the server will be closed)
 *  - packet starts with EXTERNAL_INDEX_ERR_MSG bytes. (this will be send from the server indicating that something gone
 *    wrong in the server and the following bytes will be error message and will be interpreted as string in
 *    elog(ERROR))
 */
static void check_external_index_response_status(external_index_socket_t *socket_con, char *buffer, int64 size)
{
    uint32 hdr;
    uint32 err_msg_size = 0;
    uint32 bytes_read = 0;
    uint32 total_bytes_read = 0;
    char   recv_error[ EXTERNAL_INDEX_MAX_ERR_SIZE ];

    if(size < 0 || LDB_FAILURE_POINT_IS_ENABLED("crash_on_response_size_check")) {
        elog(ERROR, "external index socket read failed");
    }

    if(size < sizeof(uint32)) {
        return;
    }

    memcpy(&hdr, buffer, sizeof(uint32));

    if(hdr != EXTERNAL_INDEX_ERR_MSG) {
        return;
    };

    // if we receive EXTERNAL_INDEX_ERR_MSG header
    // the server should send err_msg_bytes (uint32) followed by the actual error message
    // we will read and check errors here manually to not get stuck into recursion

    char   err_len_bytes[ sizeof(uint32) ];
    uint32 err_len_size_read = 0;

    // if some part of err is already read copy to buffer
    total_bytes_read = size - EXTERNAL_INDEX_MAGIC_MSG_SIZE;
    if(total_bytes_read > 0) {
        err_len_size_read = total_bytes_read >= sizeof(uint32) ? sizeof(uint32) : total_bytes_read;
        memcpy(&err_len_bytes, buffer + EXTERNAL_INDEX_MAGIC_MSG_SIZE, err_len_size_read);
        total_bytes_read -= err_len_size_read;
    }

    // if after copying the error message length
    // there are still bytes left in the buffer
    // copy them as part of error message
    if(total_bytes_read > 0) {
        memcpy(&recv_error, buffer + EXTERNAL_INDEX_MAGIC_MSG_SIZE + err_len_size_read, total_bytes_read);
    }

    // if we still need to read from socket to get the error length
    if(err_len_size_read < sizeof(uint32)) {
        bytes_read = socket_con->read(
            socket_con, (char *)&err_len_bytes + err_len_size_read, sizeof(uint32) - err_len_size_read);

        if(bytes_read != sizeof(uint32) - err_len_size_read) {
            elog(ERROR, "external index socket read failed");
        }
    }

    memcpy(&err_msg_size, &err_len_bytes, sizeof(uint32));

    while(total_bytes_read < err_msg_size) {
        bytes_read
            = socket_con->read(socket_con, (char *)&recv_error + total_bytes_read, err_msg_size - total_bytes_read);

        if(bytes_read < 0) {
            elog(ERROR, "external index socket read failed");
        }

        total_bytes_read += bytes_read;
    }

    elog(ERROR, "external index error: %s", (char *)&recv_error);
}

static void check_external_index_request_status(external_index_socket_t *socket_con, int64 bytes_written)
{
    CHECK_FOR_INTERRUPTS();

    if(bytes_written > 0) return;

    elog(ERROR, "external index socket send failed");
}

static void external_index_write_all(external_index_socket_t *socket_con, const char *buf, uint32 len, int flags)
{
    int32  total = 0;
    uint32 bytesleft = len;
    int64  n;

    while(total < len) {
        n = socket_con->write(socket_con, buf + total, bytesleft);
        check_external_index_request_status(socket_con, n);

        total += n;
        bytesleft -= (uint32)n;
    }
}

uint64 external_index_read_all(external_index_socket_t *socket_con, char *result_buf, uint64 size)
{
    int64  bytes_read;
    uint64 index_size = 0, total_received = 0;

    // start reading index into buffer
    while(total_received < size) {
        bytes_read = socket_con->read(socket_con, result_buf + total_received, size - total_received);

        // Check for CTRL-C interrupts
        CHECK_FOR_INTERRUPTS();

        check_external_index_response_status(socket_con, result_buf, bytes_read);

        if(bytes_read == 0) {
            break;
        }

        total_received += (uint32)bytes_read;
    }

    return total_received;
}

static void external_index_send_codebook(external_index_socket_t *socket_con,
                                         float                   *codebook,
                                         uint32                   dimensions,
                                         uint32                   num_centroids,
                                         uint32                   num_subvectors)
{
    uint32 data_size = dimensions * sizeof(float);
    char   buf[ data_size ];

    for(uint32 i = 0; i < num_centroids; i++) {
        memcpy(buf, &codebook[ i * dimensions ], data_size);
        external_index_write_all(socket_con, buf, data_size, 0);
    }

    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    external_index_write_all(socket_con, (char *)&end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);
}

void create_external_index_session(const char                   *host,
                                   int                           port,
                                   bool                          secure,
                                   const usearch_init_options_t *params,
                                   const ldb_HnswBuildState     *buildstate,
                                   uint32                        estimated_row_count)
{
    int              client_fd, status;
    char             init_buf[ sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE ];
    char             port_str[ 6 ];
    struct addrinfo *serv_addr, hints = {0};
    char             init_response[ EXTERNAL_INDEX_INIT_BUFFER_SIZE ] = {0};
    int64            bytes_read = 0;
    uint32           element_bits = 0;

    if(!is_little_endian() || LDB_FAILURE_POINT_IS_ENABLED("crash_on_check_little_endian")) {
        elog(ERROR, "external indexing is supported only for little endian byte ordering");
    }

    elog(INFO, "connecting to external indexing server on %s:%d", host, port);

#ifdef LANTERN_USE_OPENSSL
    if(secure) {
        buildstate->external_socket->init = (void *)init_ssl;
        buildstate->external_socket->read = (void *)read_ssl;
        buildstate->external_socket->write = (void *)write_ssl;
        buildstate->external_socket->close = (void *)close_ssl;
    }
#else
    if(secure) {
        elog(ERROR,
             "Can not use secure connection as Postgres is not compiled with openssl support. Set "
             "'lantern.external_index_secure=false' and retry");
    }
#endif  // ifdef LANTERN_USE_OPENSSL
    if(!secure) {
        buildstate->external_socket->init = (void *)init_plain;
        buildstate->external_socket->read = (void *)read_plain;
        buildstate->external_socket->write = (void *)write_plain;
        buildstate->external_socket->close = (void *)close_plain;
    }

    if((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0 || LDB_FAILURE_POINT_IS_ENABLED("crash_after_socket_create")) {
        elog(ERROR, "external index: socket creation failed");
    }

    buildstate->external_socket->fd = client_fd;
    hints.ai_socktype = SOCK_STREAM;  // TCP socket
    snprintf(port_str, 6, "%u", port);
    status = getaddrinfo(host, port_str, &hints, &serv_addr);

    if(status != 0) {
        elog(ERROR, "external index: getaddrinfo %s", gai_strerror(status));
    }

    set_write_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);

    set_read_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT);

    if((status
        = connect_with_timeout(client_fd, serv_addr->ai_addr, serv_addr->ai_addrlen, EXTERNAL_INDEX_SOCKET_TIMEOUT))
       < 0) {
        elog(ERROR, "external index: connect timeout");
    }

    elog(INFO, "successfully connected to external indexing server");
    buildstate->external_socket->init(buildstate->external_socket);

    // receive and check protocol version
    bytes_read = buildstate->external_socket->read(buildstate->external_socket, (char *)&init_response, sizeof(uint32));
    check_external_index_response_status(buildstate->external_socket, (char *)init_response, bytes_read);

    uint32 protocol_version = 0;
    memcpy(&protocol_version, init_response, sizeof(uint32));

    if(protocol_version != EXTERNAL_INDEX_PROTOCOL_VERSION
       || LDB_FAILURE_POINT_IS_ENABLED("crash_on_protocol_version_check")) {
        elog(ERROR,
             "external index protocol version mismatch - client version: %u, server version: %u",
             EXTERNAL_INDEX_PROTOCOL_VERSION,
             protocol_version);
    }
    // check server type
    bytes_read = buildstate->external_socket->read(buildstate->external_socket, (char *)&init_response, sizeof(uint32));
    check_external_index_response_status(buildstate->external_socket, (char *)init_response, bytes_read);

    uint32 server_type = 0;
    memcpy(&server_type, init_response, sizeof(uint32));

    if(server_type == EXTERNAL_INDEX_ROUTER_SERVER_TYPE) {
        uint32 is_secure = 0;
        uint32 address_length = 0;
        uint32 port_number = 0;
        char   address[ 1024 ] = {0};
        uint32 get_server_msg = 0x3;

        elog(INFO, "receiving new server address from router... (this may take up to 10m)");
        memcpy(init_buf, &get_server_msg, sizeof(uint32));
        external_index_write_all(buildstate->external_socket, init_buf, sizeof(uint32), 0);

        // wait for data to be available for read and also check for interrupts each 1s
        wait_for_data(buildstate->external_socket);

        bytes_read
            = buildstate->external_socket->read(buildstate->external_socket, (char *)&init_response, sizeof(uint32));
        check_external_index_response_status(buildstate->external_socket, (char *)init_response, bytes_read);
        memcpy(&is_secure, init_response, sizeof(uint32));

        bytes_read
            = buildstate->external_socket->read(buildstate->external_socket, (char *)&init_response, sizeof(uint32));
        check_external_index_response_status(buildstate->external_socket, (char *)init_response, bytes_read);
        memcpy(&address_length, init_response, sizeof(uint32));

        external_index_read_all(buildstate->external_socket, (char *)&address, address_length);

        bytes_read
            = buildstate->external_socket->read(buildstate->external_socket, (char *)&init_response, sizeof(uint32));
        check_external_index_response_status(buildstate->external_socket, (char *)init_response, bytes_read);
        memcpy(&port_number, init_response, sizeof(uint32));

        buildstate->external_socket->close(buildstate->external_socket);

        // connect to new address
        return create_external_index_session(
            address, port_number, (bool)is_secure, params, buildstate, estimated_row_count);
    }

    if(params->metric_kind == usearch_metric_hamming_k) {
        element_bits = 1;
    } else {
        element_bits = 32;
    }

    external_index_params_t index_params = {.pq = params->pq,
                                            .metric_kind = params->metric_kind,
                                            .quantization = params->quantization,
                                            .dim = (uint32)params->dimensions,
                                            .m = (uint32)params->connectivity,
                                            .ef_construction = (uint32)params->expansion_add,
                                            .ef = (uint32)params->expansion_search,
                                            .num_centroids = (uint32)params->num_centroids,
                                            .num_subvectors = (uint32)params->num_subvectors,
                                            .estimated_capcity = estimated_row_count,
                                            .element_bits = element_bits};

    uint32 hdr_msg = EXTERNAL_INDEX_INIT_MSG;
    memcpy(init_buf, &hdr_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE);
    memcpy(init_buf + EXTERNAL_INDEX_MAGIC_MSG_SIZE, &index_params, sizeof(external_index_params_t));

    external_index_write_all(
        buildstate->external_socket, init_buf, sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    if(params->pq) {
        external_index_send_codebook(buildstate->external_socket,
                                     buildstate->pq_codebook,
                                     index_params.dim,
                                     index_params.num_centroids,
                                     index_params.num_subvectors);
    }

    bytes_read = buildstate->external_socket->read(
        buildstate->external_socket, (char *)&init_response, EXTERNAL_INDEX_INIT_BUFFER_SIZE);

    check_external_index_response_status(buildstate->external_socket, (char *)init_response, bytes_read);
}

void external_index_receive_metadata(external_index_socket_t *socket_con, uint64 *num_added_vectors, uint64 *index_size)
{
    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    char   buffer[ sizeof(uint64_t) ];
    int64  bytes_read;

    if(LDB_FAILURE_POINT_IS_ENABLED("crash_on_end_msg")) {
        end_msg = EXTERNAL_INDEX_INIT_MSG;
    }

    // send message indicating that we have finished streaming tuples
    external_index_write_all(socket_con, (char *)&end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0);

    // disable read timeout
    set_read_timeout(socket_con->fd, 0);

    // read how many tuples have been indexed
    bytes_read = socket_con->read(socket_con, buffer, sizeof(uint64));
    check_external_index_response_status(socket_con, buffer, bytes_read);

    memcpy(num_added_vectors, buffer, sizeof(uint64));

    // read index file size
    bytes_read = socket_con->read(socket_con, buffer, sizeof(uint64));
    check_external_index_response_status(socket_con, buffer, bytes_read);

    memcpy(index_size, buffer, sizeof(uint64));
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
    external_index_write_all(socket_con, tuple, tuple_size, 0);
}
