#ifndef UNIMAKER_COMPAT_EXPLICIT_BZERO_H
#define UNIMAKER_COMPAT_EXPLICIT_BZERO_H

#include <stddef.h>
#include <string.h>

#if !defined(HAVE_EXPLICIT_BZERO)
static inline void explicit_bzero(void *ptr, size_t len)
{
    if (ptr == NULL || len == 0) {
        return;
    }
#if defined(__STDC_LIB_EXT1__)
    (void)memset_s(ptr, len, 0, len);
#else
    volatile unsigned char *p = (volatile unsigned char *)ptr;
    while (len-- != 0) {
        *p++ = 0;
    }
#endif
}
#endif

#if !defined(HAVE_MEMMEM)
static inline void *nim_compat_memmem(const void *haystack, size_t haystacklen,
                                      const void *needle, size_t needlelen)
{
    if (needlelen == 0) {
        return (void *)haystack;
    }
    if (haystack == NULL || needle == NULL || haystacklen < needlelen) {
        return NULL;
    }
    const unsigned char *h = (const unsigned char *)haystack;
    const unsigned char *n = (const unsigned char *)needle;
    size_t remaining = haystacklen - needlelen + 1;
    while (remaining--) {
        if (h[0] == n[0] && memcmp(h, n, needlelen) == 0) {
            return (void *)h;
        }
        h++;
    }
    return NULL;
}
#define memmem nim_compat_memmem
#endif

#endif /* UNIMAKER_COMPAT_EXPLICIT_BZERO_H */
