#ifdef LANTERN_USE_OPENSSL
#include <unistd.h>

#include "external_index_socket.h"

static SSL_CTX *ssl_ctx_create(void)
{
    SSL_CTX *ctx;
    int      options;

#if(OPENSSL_VERSION_NUMBER >= 0x1010000fL)
    /* OpenSSL >= v1.1 */
    ctx = SSL_CTX_new(TLS_method());

    options = SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
#elif(OPENSSL_VERSION_NUMBER >= 0x1000000fL)
    /* OpenSSL >= v1.0 */
    ctx = SSL_CTX_new(SSLv23_method());

    options = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
#else
#error "Unsupported OpenSSL version"
#endif

    /*
     * Because we have a blocking socket, we don't want to be bothered with
     * retries.
     */
    if(NULL != ctx) {
        SSL_CTX_set_options(ctx, options);
        SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY);
    }

    return ctx;
}

int init_ssl(external_index_socket_t *socket)
{
    int ret;
    socket->ssl_ctx = ssl_ctx_create();

    if(socket->ssl_ctx == NULL) {
        elog(ERROR, "could not create ssl context");
    }

    socket->ssl = SSL_new(socket->ssl_ctx);
    if(socket->ssl == NULL) {
        elog(ERROR, "ssl initialization error");
    }

    ret = SSL_set_fd(socket->ssl, socket->fd);

    if(ret == 0) {
        elog(ERROR, "error setting file descriptor for ssl socket");
    }

    ret = SSL_connect(socket->ssl);
    if(ret <= 0) {
        elog(ERROR, "ssl connection error");
    }

    return 0;
}

int write_ssl(external_index_socket_t *socket, const char *buf, uint32 size)
{
    int ret = SSL_write(socket->ssl, buf, size);

    return ret;
}

int read_ssl(external_index_socket_t *socket, char *buf, uint32 size)
{
    int ret = SSL_read(socket->ssl, buf, size);

    return ret;
}

void close_ssl(external_index_socket_t *socket)
{
    if(socket->ssl != NULL) {
        SSL_free(socket->ssl);
        socket->ssl = NULL;
    }

    if(socket->ssl_ctx != NULL) {
        SSL_CTX_free(socket->ssl_ctx);
        socket->ssl_ctx = NULL;
    }

    close(socket->fd);
}
#endif
