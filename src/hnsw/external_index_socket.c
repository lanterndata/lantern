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

static void set_read_timeout(int32 client_fd, uint32 seconds, BuildIndexStatus *status)
{
    struct timeval timeout;

    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if(setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout) < 0
       || LDB_FAILURE_POINT_IS_ENABLED("crash_after_set_recv_timeout")) {
        status->code = BUILD_INDEX_FAILED;
        strncpy(status->error, "external index: failed to set receive timeout for socket", BUILD_INDEX_MAX_ERROR_SIZE);
        return;
    }
    status->code = BUILD_INDEX_OK;
}

static void set_write_timeout(int32 client_fd, uint32 seconds, BuildIndexStatus *status)
{
    struct timeval timeout;

    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if(setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout) < 0
       || LDB_FAILURE_POINT_IS_ENABLED("crash_after_set_send_timeout")) {
        status->code = BUILD_INDEX_FAILED;
        strncpy(status->error, "external index: failed to set send timeout for socket", BUILD_INDEX_MAX_ERROR_SIZE);
        return;
    }
    status->code = BUILD_INDEX_OK;
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

static void wait_for_data(external_index_socket_t *socket_con, BuildIndexStatus *status)
{
    struct timeval timeout;
    fd_set         read_fds;

    int interval = 5;

    // Set the socket to non-blocking mode
    int flags = fcntl(socket_con->fd, F_GETFL, 0);
    if(flags == -1) {
        status->code = BUILD_INDEX_FAILED;
        strncpy(status->error, "error getting socket flags", BUILD_INDEX_MAX_ERROR_SIZE);
        return;
    }

    if(fcntl(socket_con->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        status->code = BUILD_INDEX_FAILED;
        strncpy(status->error, "error setting socket to non-blocking mode", BUILD_INDEX_MAX_ERROR_SIZE);
        return;
    }

    while(1) {
        FD_ZERO(&read_fds);
        FD_SET(socket_con->fd, &read_fds);

        timeout.tv_sec = 5;
        timeout.tv_usec = 0;

        int activity = select(socket_con->fd + 1, &read_fds, NULL, NULL, &timeout);

        if(activity < 0) {
            status->code = BUILD_INDEX_FAILED;
            strncpy(status->error, "select syscall error", BUILD_INDEX_MAX_ERROR_SIZE);
            return;
        }

        // If socket has data to read
        if(FD_ISSET(socket_con->fd, &read_fds)) {
            // Restore the socket to blocking mode
            if(fcntl(socket_con->fd, F_SETFL, flags) == -1) {
                status->code = BUILD_INDEX_FAILED;
                strncpy(status->error, "error setting socket to blocking mode", BUILD_INDEX_MAX_ERROR_SIZE);
            }
            return;
        }

        // Check for interrupts on each iteration
        if(INTERRUPTS_PENDING_CONDITION()) {
            status->code = BUILD_INDEX_INTERRUPT;
            return;
        }
    }
}

/**
 * Check for error received from socket response
 * This function will return void setting the corresponding error code and error message
 * Error conditions are the following:
 *  - read size is less then zero. (this can happen on network errors, or when the server will be closed)
 *  - packet starts with EXTERNAL_INDEX_ERR_MSG bytes. (this will be send from the server indicating that something gone
 *    wrong in the server and the following bytes will be error message and will be interpreted as string in
 *    elog(ERROR))
 */
static void set_external_index_response_status(external_index_socket_t *socket_con,
                                               char                    *buffer,
                                               int64                    size,
                                               BuildIndexStatus        *status)
{
    uint32 hdr;
    uint32 err_msg_size = 0;
    uint32 bytes_read = 0;
    uint32 total_bytes_read = 0;
    char   recv_error[ BUILD_INDEX_MAX_ERROR_SIZE ];

    if(size < 0 || LDB_FAILURE_POINT_IS_ENABLED("crash_on_response_size_check")) {
        status->code = BUILD_INDEX_FAILED;
        strncpy(status->error, "external index socket read failed", BUILD_INDEX_MAX_ERROR_SIZE);
        return;
    }

    if(size < sizeof(uint32)) {
        return;
    }

    memcpy(&hdr, buffer, sizeof(uint32));

    if(hdr != EXTERNAL_INDEX_ERR_MSG) {
        status->code = BUILD_INDEX_OK;
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
            status->code = BUILD_INDEX_FAILED;
            strncpy(status->error, "external index socket read failed", BUILD_INDEX_MAX_ERROR_SIZE);
            return;
        }
    }

    memcpy(&err_msg_size, &err_len_bytes, sizeof(uint32));

    while(total_bytes_read < err_msg_size) {
        bytes_read
            = socket_con->read(socket_con, (char *)&recv_error + total_bytes_read, err_msg_size - total_bytes_read);

        if(bytes_read < 0) {
            status->code = BUILD_INDEX_FAILED;
            strncpy(status->error, "external index socket read failed", BUILD_INDEX_MAX_ERROR_SIZE);
            return;
        }

        total_bytes_read += bytes_read;
    }

    status->code = BUILD_INDEX_FAILED;
    snprintf(status->error, BUILD_INDEX_MAX_ERROR_SIZE, "external index error: %s", (char *)&recv_error);
}

static void set_external_index_request_status(external_index_socket_t *socket_con,
                                              int64                    bytes_written,
                                              BuildIndexStatus        *status)
{
    if(INTERRUPTS_PENDING_CONDITION()) {
        status->code = BUILD_INDEX_INTERRUPT;
        return;
    }

    if(bytes_written > 0) {
        status->code = BUILD_INDEX_OK;
        return;
    };

    status->code = BUILD_INDEX_FAILED;
    strncpy(status->error, "external index socket send failed", BUILD_INDEX_MAX_ERROR_SIZE);
}

static void write_all(
    external_index_socket_t *socket_con, const char *buf, uint32 len, int flags, BuildIndexStatus *status)
{
    int32  total = 0;
    uint32 bytesleft = len;
    int64  n;

    while(total < len) {
        n = socket_con->write(socket_con, buf + total, bytesleft);
        set_external_index_request_status(socket_con, n, status);

        if(status->code != BUILD_INDEX_OK) {
            return;
        }

        total += n;
        bytesleft -= (uint32)n;
    }

    status->code = BUILD_INDEX_OK;
}

uint64 external_index_receive_all(external_index_socket_t *socket_con,
                                  char                    *result_buf,
                                  uint64                   size,
                                  BuildIndexStatus        *status)
{
    int64  bytes_read;
    uint64 index_size = 0, total_received = 0;

    // start reading index into buffer
    while(total_received < size) {
        bytes_read = socket_con->read(socket_con, result_buf + total_received, size - total_received);

        // Check for CTRL-C interrupts
        if(INTERRUPTS_PENDING_CONDITION()) {
            status->code = BUILD_INDEX_INTERRUPT;
            return total_received;
        }

        set_external_index_response_status(socket_con, result_buf, bytes_read, status);

        if(status->code != BUILD_INDEX_OK) {
            return total_received;
        }

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
                                         uint32                   num_subvectors,
                                         BuildIndexStatus        *status)
{
    uint32 data_size = dimensions * sizeof(float);
    char   buf[ data_size ];

    for(uint32 i = 0; i < num_centroids; i++) {
        memcpy(buf, &codebook[ i * dimensions ], data_size);
        write_all(socket_con, buf, data_size, 0, status);

        if(status->code != BUILD_INDEX_OK) {
            return;
        }
    }

    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    write_all(socket_con, (char *)&end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0, status);
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
    char                     port_str[ 6 ];
    struct addrinfo         *serv_addr, hints = {0};
    char                     init_response[ EXTERNAL_INDEX_INIT_BUFFER_SIZE ] = {0};
    int64                    bytes_read = 0;

    if(!is_little_endian() || LDB_FAILURE_POINT_IS_ENABLED("crash_on_check_little_endian")) {
        buildstate->status->code = BUILD_INDEX_FAILED;
        strncpy(buildstate->status->error,
                "external indexing is supported only for little endian byte ordering",
                BUILD_INDEX_MAX_ERROR_SIZE);
        return NULL;
    }

    elog(INFO, "connecting to external indexing server on %s:%d", host, port);

#ifdef LANTERN_USE_OPENSSL
    if(secure) {
        socket_con->init = (void *)init_ssl;
        socket_con->read = (void *)read_ssl;
        socket_con->write = (void *)write_ssl;
        socket_con->close = (void *)close_ssl;
    }
#else
    if(secure) {
        buildstate->status->code = BUILD_INDEX_FAILED;
        strncpy(buildstate->status->error,
                "Can not use secure connection as Postgres is not compiled with openssl support. Set "
                "'lantern.external_index_secure=false' and retry",
                BUILD_INDEX_MAX_ERROR_SIZE);
        return NULL;
    }
#endif  // ifdef LANTERN_USE_OPENSSL
    if(!secure) {
        socket_con->init = (void *)init_plain;
        socket_con->read = (void *)read_plain;
        socket_con->write = (void *)write_plain;
        socket_con->close = (void *)close_plain;
    }

    if((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0 || LDB_FAILURE_POINT_IS_ENABLED("crash_after_socket_create")) {
        buildstate->status->code = BUILD_INDEX_FAILED;
        strncpy(buildstate->status->error, "external index: socket creation failed", BUILD_INDEX_MAX_ERROR_SIZE);
        return NULL;
    }

    socket_con->fd = client_fd;
    hints.ai_socktype = SOCK_STREAM;  // TCP socket
    snprintf(port_str, 6, "%u", port);
    status = getaddrinfo(host, port_str, &hints, &serv_addr);

    if(status != 0) {
        buildstate->status->code = BUILD_INDEX_FAILED;
        snprintf(buildstate->status->error,
                 BUILD_INDEX_MAX_ERROR_SIZE,
                 "external index: getaddrinfo %s",
                 gai_strerror(status));
        return socket_con;
    }

    set_write_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT, buildstate->status);

    if(buildstate->status->code != BUILD_INDEX_OK) {
        return socket_con;
    }

    set_read_timeout(client_fd, EXTERNAL_INDEX_SOCKET_TIMEOUT, buildstate->status);

    if(buildstate->status->code != BUILD_INDEX_OK) {
        return socket_con;
    }

    if((status
        = connect_with_timeout(client_fd, serv_addr->ai_addr, serv_addr->ai_addrlen, EXTERNAL_INDEX_SOCKET_TIMEOUT))
       < 0) {
        buildstate->status->code = BUILD_INDEX_FAILED;
        strncpy(buildstate->status->error, "external index: connect timeout", BUILD_INDEX_MAX_ERROR_SIZE);
        return socket_con;
    }

    elog(INFO, "successfully connected to external indexing server");
    socket_con->init(socket_con);

    // receive and check protocol version
    bytes_read = socket_con->read(socket_con, (char *)&init_response, sizeof(uint32));
    set_external_index_response_status(socket_con, (char *)init_response, bytes_read, buildstate->status);
    if(buildstate->status->code != BUILD_INDEX_OK) {
        return socket_con;
    }
    uint32 protocol_version = 0;
    memcpy(&protocol_version, init_response, sizeof(uint32));

    if(protocol_version != EXTERNAL_INDEX_PROTOCOL_VERSION
       || LDB_FAILURE_POINT_IS_ENABLED("crash_on_protocol_version_check")) {
        buildstate->status->code = BUILD_INDEX_FAILED;
        snprintf(buildstate->status->error,
                 BUILD_INDEX_MAX_ERROR_SIZE,
                 "external index protocol version mismatch - client version: %u, server version: %u",
                 EXTERNAL_INDEX_PROTOCOL_VERSION,
                 protocol_version);
        return socket_con;
    }
    // check server type
    bytes_read = socket_con->read(socket_con, (char *)&init_response, sizeof(uint32));
    set_external_index_response_status(socket_con, (char *)init_response, bytes_read, buildstate->status);
    if(buildstate->status->code != BUILD_INDEX_OK) {
        return socket_con;
    }
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
        write_all(socket_con, init_buf, sizeof(uint32), 0, buildstate->status);

        // wait for data to be available for read and also check for interrupts each 5s
        wait_for_data(socket_con, buildstate->status);

        if(buildstate->status->code != BUILD_INDEX_OK) {
            return socket_con;
        }

        bytes_read = socket_con->read(socket_con, (char *)&init_response, sizeof(uint32));
        set_external_index_response_status(socket_con, (char *)init_response, bytes_read, buildstate->status);
        if(buildstate->status->code != BUILD_INDEX_OK) {
            return socket_con;
        }
        memcpy(&is_secure, init_response, sizeof(uint32));

        bytes_read = socket_con->read(socket_con, (char *)&init_response, sizeof(uint32));
        set_external_index_response_status(socket_con, (char *)init_response, bytes_read, buildstate->status);
        if(buildstate->status->code != BUILD_INDEX_OK) {
            return socket_con;
        }
        memcpy(&address_length, init_response, sizeof(uint32));

        external_index_receive_all(socket_con, (char *)&address, address_length, buildstate->status);
        if(buildstate->status->code != BUILD_INDEX_OK) {
            return socket_con;
        }

        bytes_read = socket_con->read(socket_con, (char *)&init_response, sizeof(uint32));
        set_external_index_response_status(socket_con, (char *)init_response, bytes_read, buildstate->status);
        if(buildstate->status->code != BUILD_INDEX_OK) {
            return socket_con;
        }
        memcpy(&port_number, init_response, sizeof(uint32));

        socket_con->close(socket_con);

        // connect to new address
        return create_external_index_session(
            address, port_number, (bool)is_secure, params, buildstate, estimated_row_count);
    }

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

    write_all(
        socket_con, init_buf, sizeof(external_index_params_t) + EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0, buildstate->status);

    if(buildstate->status->code != BUILD_INDEX_OK) {
        return socket_con;
    }

    if(params->pq) {
        external_index_send_codebook(socket_con,
                                     buildstate->pq_codebook,
                                     index_params.dim,
                                     index_params.num_centroids,
                                     index_params.num_subvectors,
                                     buildstate->status);

        if(buildstate->status->code != BUILD_INDEX_OK) {
            return socket_con;
        }
    }

    bytes_read = socket_con->read(socket_con, (char *)&init_response, EXTERNAL_INDEX_INIT_BUFFER_SIZE);

    set_external_index_response_status(socket_con, (char *)init_response, bytes_read, buildstate->status);

    return socket_con;
}

void external_index_receive_metadata(external_index_socket_t *socket_con,
                                     uint64                  *num_added_vectors,
                                     uint64                  *index_size,
                                     BuildIndexStatus        *status)
{
    uint32 end_msg = EXTERNAL_INDEX_END_MSG;
    char   buffer[ sizeof(uint64_t) ];
    int64  bytes_read;

    if(LDB_FAILURE_POINT_IS_ENABLED("crash_on_end_msg")) {
        end_msg = EXTERNAL_INDEX_INIT_MSG;
    }

    // send message indicating that we have finished streaming tuples
    write_all(socket_con, (char *)&end_msg, EXTERNAL_INDEX_MAGIC_MSG_SIZE, 0, status);
    if(status->code != BUILD_INDEX_OK) {
        return;
    }

    // wait for data to be available
    wait_for_data(socket_con, status);
    if(status->code != BUILD_INDEX_OK) {
        return;
    }

    // read how many tuples have been indexed
    bytes_read = socket_con->read(socket_con, buffer, sizeof(uint64));
    set_external_index_response_status(socket_con, buffer, bytes_read, status);

    if(status->code != BUILD_INDEX_OK) {
        return;
    }

    memcpy(num_added_vectors, buffer, sizeof(uint64));

    // read index file size
    bytes_read = socket_con->read(socket_con, buffer, sizeof(uint64));
    set_external_index_response_status(socket_con, buffer, bytes_read, status);

    if(status->code != BUILD_INDEX_OK) {
        return;
    }

    memcpy(index_size, buffer, sizeof(uint64));
}

void external_index_send_tuple(external_index_socket_t *socket_con,
                               usearch_label_t         *label,
                               void                    *vector,
                               uint8                    scalar_bits,
                               uint32                   dimensions,
                               BuildIndexStatus        *status)
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
    write_all(socket_con, tuple, tuple_size, 0, status);
}
