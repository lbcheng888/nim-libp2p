#ifndef ANDROID_FIX_H
#define ANDROID_FIX_H

#include <fcntl.h>

#ifndef POSIX_FADV_SEQUENTIAL
#define POSIX_FADV_SEQUENTIAL 0
#endif

#ifndef POSIX_FADV_RANDOM
#define POSIX_FADV_RANDOM 1
#endif

#ifndef POSIX_FADV_NOREUSE
#define POSIX_FADV_NOREUSE 2
#endif

#ifndef POSIX_FADV_WILLNEED
#define POSIX_FADV_WILLNEED 3
#endif

#ifndef POSIX_FADV_DONTNEED
#define POSIX_FADV_DONTNEED 4
#endif

// Define posix_fadvise as a macro that returns 0 (success)
#define posix_fadvise(fd, offset, len, advice) 0

#include <sys/socket.h>
#include <unistd.h>

#ifndef SOCK_CLOEXEC
#define SOCK_CLOEXEC 02000000
#endif

#ifndef SOCK_NONBLOCK
#define SOCK_NONBLOCK 00004000
#endif

static inline int accept4(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags) {
    int fd = accept(sockfd, addr, addrlen);
    if (fd == -1) return -1;
    
    if (flags & SOCK_CLOEXEC) {
        fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
    if (flags & SOCK_NONBLOCK) {
        int fl = fcntl(fd, F_GETFL);
        fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    }
    return fd;
}

#endif
