#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

#undef OpenSSL_add_all_algorithms
#undef OPENSSL_add_all_algorithms_conf
#undef OPENSSL_add_all_algorithms_noconf
#undef EVP_cleanup
#undef CRYPTO_cleanup_all_ex_data
#undef ERR_free_strings
#undef SSL_library_init
#undef SSL_load_error_strings
#undef EVP_CIPHER_key_length
#undef EVP_CIPHER_iv_length
#undef EVP_CIPHER_block_size
#undef EVP_MD_size
#undef EVP_MD_block_size
#undef EVP_MD_CTX_cleanup
#undef EVP_MD_CTX_create
#undef EVP_MD_CTX_destroy
#undef EVP_MD_CTX_init

void OpenSSL_add_all_algorithms(void) {
    OPENSSL_init_crypto(
        OPENSSL_INIT_ADD_ALL_CIPHERS | OPENSSL_INIT_ADD_ALL_DIGESTS,
        NULL);
}

void OPENSSL_add_all_algorithms_conf(void) {
    OPENSSL_init_crypto(
        OPENSSL_INIT_ADD_ALL_CIPHERS | OPENSSL_INIT_ADD_ALL_DIGESTS |
        OPENSSL_INIT_LOAD_CONFIG,
        NULL);
}

void OPENSSL_add_all_algorithms_noconf(void) {
    OPENSSL_init_crypto(
        OPENSSL_INIT_ADD_ALL_CIPHERS | OPENSSL_INIT_ADD_ALL_DIGESTS,
        NULL);
}

void EVP_cleanup(void) {
    OPENSSL_cleanup();
}

void CRYPTO_cleanup_all_ex_data(void) {
    /* deprecated in OpenSSL 3, no-op for compatibility */
}

void ERR_free_strings(void) {
    /* strings freed automatically in OpenSSL 3 */
}

int SSL_library_init(void) {
    return OPENSSL_init_ssl(0, NULL);
}

void SSL_load_error_strings(void) {
    ERR_load_SSL_strings();
}

int EVP_CIPHER_key_length(const EVP_CIPHER *cipher) {
    return EVP_CIPHER_get_key_length(cipher);
}

int EVP_CIPHER_iv_length(const EVP_CIPHER *cipher) {
    return EVP_CIPHER_get_iv_length(cipher);
}

int EVP_CIPHER_block_size(const EVP_CIPHER *cipher) {
    return EVP_CIPHER_get_block_size(cipher);
}

int EVP_MD_size(const EVP_MD *md) {
    return EVP_MD_get_size(md);
}

int EVP_MD_block_size(const EVP_MD *md) {
    return EVP_MD_get_block_size(md);
}

int EVP_MD_CTX_cleanup(EVP_MD_CTX *ctx) {
    EVP_MD_CTX_reset(ctx);
    return 1;
}

EVP_MD_CTX *EVP_MD_CTX_create(void) {
    return EVP_MD_CTX_new();
}

void EVP_MD_CTX_destroy(EVP_MD_CTX *ctx) {
    EVP_MD_CTX_free(ctx);
}

void EVP_MD_CTX_init(EVP_MD_CTX *ctx) {
    EVP_MD_CTX_reset(ctx);
}
