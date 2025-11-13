#ifndef LIBP2P_OPENSSL_COMPAT_H
#define LIBP2P_OPENSSL_COMPAT_H

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <string.h>

#if !defined(OSSL_QUIC_server_method) && OPENSSL_VERSION_NUMBER < 0x30400000L
static inline const SSL_METHOD *OSSL_QUIC_server_method(void) {
    return NULL;
}
#endif

#ifndef SSL_get_peer_addr
int SSL_get_peer_addr(SSL *ssl, BIO_ADDR *peer_addr);
#endif

#ifndef SSL_new_listener
SSL *SSL_new_listener(SSL_CTX *ctx, uint64_t flags);
#endif

#ifndef SSL_accept_connection
SSL *SSL_accept_connection(SSL *ssl, uint64_t flags);
#endif

#ifndef SSL_listen
int SSL_listen(SSL *ssl);
#endif

#ifndef SSL_set_blocking_mode
int SSL_set_blocking_mode(SSL *ssl, int blocking);
#endif

#ifndef SSL_set1_initial_peer_addr
int SSL_set1_initial_peer_addr(SSL *ssl, const BIO_ADDR *peer_addr);
#endif

#ifndef NIM_SSL_POLL_STREAM_DECLARED
#define NIM_SSL_POLL_STREAM_DECLARED
static inline int nim_SSL_poll_stream(SSL *ssl, uint64_t events, uint64_t *revents) {
    SSL_POLL_ITEM item;
    size_t result = 0;
    memset(&item, 0, sizeof(item));
    item.desc = SSL_as_poll_descriptor(ssl);
    item.events = events;
    if (!SSL_poll(&item, 1, sizeof(item), NULL, 0, &result))
        return -1;
    if (revents != NULL)
        *revents = (result > 0) ? item.revents : 0;
    return (int)result;
}
#endif

#ifndef NIM_SSL_POLL_HELPERS_DECLARED
#define NIM_SSL_POLL_HELPERS_DECLARED
typedef struct nim_ssl_poll_item_st {
    SSL_POLL_ITEM item;
} nim_ssl_poll_item;

static inline void nim_ssl_poll_item_setup(nim_ssl_poll_item *holder, SSL *ssl, uint64_t events) {
    memset(holder, 0, sizeof(*holder));
    holder->item.desc = SSL_as_poll_descriptor(ssl);
    holder->item.events = events;
}

static inline uint64_t nim_ssl_poll_item_events(const nim_ssl_poll_item *holder) {
    return holder->item.events;
}

static inline void nim_ssl_poll_item_set_events(nim_ssl_poll_item *holder, uint64_t events) {
    holder->item.events = events;
}

static inline uint64_t nim_ssl_poll_item_revents(const nim_ssl_poll_item *holder) {
    return holder->item.revents;
}

static inline void nim_ssl_poll_item_reset(nim_ssl_poll_item *holder) {
    holder->item.revents = 0;
}

static inline int nim_SSL_poll(nim_ssl_poll_item *items,
                               size_t num_items,
                               const struct timeval *timeout,
                               uint64_t flags,
                               size_t *result_count) {
    return SSL_poll(items != NULL ? &items[0].item : NULL,
                    num_items,
                    sizeof(nim_ssl_poll_item),
                    timeout,
                    flags,
                    result_count);
}
#endif

#ifndef SSL_handle_events
int SSL_handle_events(SSL *ssl);
#endif

#ifndef SSL_get_event_timeout
int SSL_get_event_timeout(SSL *ssl, struct timeval *tv, int *is_infinite);
#endif

#ifndef SSL_new_stream
SSL *SSL_new_stream(SSL *ssl, uint64_t flags);
#endif

#ifndef SSL_accept_stream
SSL *SSL_accept_stream(SSL *ssl, uint64_t flags);
#endif

#ifndef SSL_get_stream_id
uint64_t SSL_get_stream_id(SSL *ssl);
#endif

#ifndef SSL_get_stream_type
int SSL_get_stream_type(SSL *ssl);
#endif

#ifndef SSL_stream_conclude
int SSL_stream_conclude(SSL *ssl, uint64_t flags);
#endif

#ifndef SSL_shutdown
int SSL_shutdown(SSL *ssl);
#endif

#ifndef SSL_read_ex
int SSL_read_ex(SSL *ssl, void *buf, size_t num, size_t *readbytes);
#endif

#ifndef SSL_write_ex
int SSL_write_ex(SSL *ssl, const void *buf, size_t num, size_t *written);
#endif

#ifndef SSL_get_error
int SSL_get_error(const SSL *ssl, int ret);
#endif

#ifndef SSL_CTX_set_alpn_select_cb
typedef int (*nim_alpn_cb)(void *ssl, unsigned char **out, unsigned char *outlen,
                           unsigned char *in, unsigned int inlen, void *arg);
static inline void nim_SSL_CTX_set_alpn_select_cb(SSL_CTX *ctx,
                                                  nim_alpn_cb cb, void *arg) {
    SSL_CTX_set_alpn_select_cb(ctx,
        (int (*)(SSL *, const unsigned char **, unsigned char *,
                 const unsigned char *, unsigned int, void *))cb, arg);
}
#endif

#ifndef SSL_select_next_proto
int SSL_select_next_proto(unsigned char **out, unsigned char *outlen,
                          const unsigned char *server, unsigned int server_len,
                          const unsigned char *client, unsigned int client_len);
#endif

#endif /* LIBP2P_OPENSSL_COMPAT_H */
