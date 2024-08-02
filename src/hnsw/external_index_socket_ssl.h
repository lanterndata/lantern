#ifndef LDB_EXTERNAL_IDX_SOCKET_SSL_H
#define LDB_EXTERNAL_IDX_SOCKET_SSL_H

#ifdef LANTERN_USE_OPENSSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#else
#define SSL_CTX void
#define SSL     void
#endif

#endif  // LDB_EXTERNAL_IDX_SOCKET_SSL_H
