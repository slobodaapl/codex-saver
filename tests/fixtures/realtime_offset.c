#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

int clock_gettime(clockid_t id, struct timespec *time) {
    int (*real_clock_gettime)(clockid_t, struct timespec *) =
        dlsym(RTLD_NEXT, "clock_gettime");
    int result = real_clock_gettime(id, time);
    const char *path = getenv("CODEX_SAVER_TEST_REALTIME_OFFSET_FILE");
    if (result == 0 && id == CLOCK_REALTIME && path != NULL) {
        int fd = open(path, O_RDONLY);
        if (fd >= 0) {
            char value[32] = {0};
            ssize_t size = read(fd, value, sizeof(value) - 1);
            close(fd);
            if (size > 0) {
                time->tv_sec += strtoll(value, NULL, 10);
            }
        }
    }
    return result;
}
