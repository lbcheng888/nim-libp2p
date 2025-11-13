#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <stdint.h>
#include <sys/time.h>

#ifndef SSL_new_listener
SSL *SSL_new_listener(SSL_CTX *ctx, uint64_t flags)
{
    (void)flags;
    return SSL_new(ctx);
}
#endif

#ifndef SSL_accept_connection
SSL *SSL_accept_connection(SSL *ssl, uint64_t flags)
{
    (void)flags;
    return ssl;
}
#endif

#ifndef SSL_listen
int SSL_listen(SSL *ssl)
{
    (void)ssl;
    return 1;
}
#endif

#ifndef SSL_set_blocking_mode
int SSL_set_blocking_mode(SSL *ssl, int blocking)
{
    (void)ssl;
    (void)blocking;
    return 1;
}
#endif

#ifndef SSL_set1_initial_peer_addr
int SSL_set1_initial_peer_addr(SSL *ssl, const BIO_ADDR *peer_addr)
{
    (void)ssl;
    (void)peer_addr;
    return 1;
}
#endif

#ifndef SSL_handle_events
int SSL_handle_events(SSL *ssl)
{
    (void)ssl;
    return 1;
}
#endif

#ifndef SSL_get_event_timeout
int SSL_get_event_timeout(SSL *ssl, struct timeval *tv, int *is_infinite)
{
    (void)ssl;
    if (tv != NULL) {
        tv->tv_sec = 0;
        tv->tv_usec = 0;
    }
    if (is_infinite != NULL) {
        *is_infinite = 1;
    }
    return 1;
}
#endif

#ifndef SSL_new_stream
SSL *SSL_new_stream(SSL *ssl, uint64_t flags)
{
    (void)flags;
    return ssl;
}
#endif

#ifndef SSL_accept_stream
SSL *SSL_accept_stream(SSL *ssl, uint64_t flags)
{
    (void)flags;
    return ssl;
}
#endif

#ifndef SSL_get_stream_id
uint64_t SSL_get_stream_id(SSL *ssl)
{
    (void)ssl;
    return 0;
}
#endif

#ifndef SSL_get_stream_type
int SSL_get_stream_type(SSL *ssl)
{
    (void)ssl;
    return 0;
}
#endif

#ifndef SSL_stream_conclude
int SSL_stream_conclude(SSL *ssl, uint64_t flags)
{
    (void)ssl;
    (void)flags;
    return 1;
}
#endif
