#ifndef _SYS_SENDFILE_H
#define _SYS_SENDFILE_H
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>

static inline ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count) {
    // Dummy implementation that returns error
    errno = ENOSYS;
    return -1; 
}
#endif
