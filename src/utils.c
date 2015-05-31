#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "utils.h"


int format_desc(char **desc, int thread_id, struct addrinfo **addrinfos, size_t num_addrinfos) {
    int i, err;
    char *str = NULL, desc_line[1024], host[NI_MAXHOST], service[NI_MAXSERV];
    struct addrinfo *addrinfo;

    snprintf(
        desc_line,
        sizeof(desc_line),
        "Thread %d (%zu %s):\n",
        thread_id,
        num_addrinfos,
        num_addrinfos == 1 ? "address" : "addresses");
    str_append(&str, desc_line);

    for (i = 0; i < num_addrinfos; i++) {
        addrinfo = addrinfos[i];
        err = getnameinfo(addrinfo->ai_addr, addrinfo->ai_addrlen, host, NI_MAXHOST, service, NI_MAXSERV, NI_NUMERICHOST | NI_NUMERICSERV);
        if (err == 0) {
            str_append(&str, "  ");
            str_append(&str, host);
            str_append(&str, ":");
            str_append(&str, service);
            str_append(&str, "\n");
        } else {
            fprintf(stderr, "Problem getting name info: %s\n", gai_strerror(err));
            goto fail;
        }
    }

    *desc = str;
    return 0;

fail:
    if (str != NULL) {
        free(str);
    }
    return 1;
}


void str_append(char **str, const char *suffix) {
    size_t str_len, suffix_len;
    char *tmp;

    str_len = (*str == NULL) ? 0 : strlen(*str);
    suffix_len = strlen(suffix);

    tmp = realloc(*str, str_len + suffix_len + 1);
    if (tmp == NULL) {
        fprintf(stderr, "Failed to allocate larger array for string: %s\n", strerror(errno));
        abort();
    }

    *str = tmp;
    if (str_len == 0) {
        strcpy(*str, suffix);
    } else {
        strcat(*str, suffix);
    }
}


int str_to_int(int *result, const char *str) {
    int x;
    char *ptr = NULL;
    errno = 0;

    x = strtol(str, &ptr, 10);
    if (errno != 0 || str == ptr || *ptr != '\0') {
        return 1;
    }

    *result = x;
    return 0;
}


int str_dup(char **result, const char *original) {
    char *str;
    size_t len;

    len = strlen(original);
    str = malloc(len + 1);
    if (str == NULL) {
        goto fail;
    }

    strcpy(str, original);
    *result = str;
    return 0;

fail:
    return 1;
}


int set_nonblocking(int fd) {
    int flags, err;
    for (;;) {
        flags = fcntl(fd, F_GETFL);
        if (flags == -1) {
            if (errno == EINTR) {
                continue;
            }
            return flags;
        }

        err = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        if (err == -1 && errno == EINTR) {
            continue;
        }
        return err;
    }
}


int force_write(int fd, char b) {
    ssize_t bytes_written;
    for (;;) {
        bytes_written = write(fd, &b, 1);
        if (bytes_written == 0) {
            continue;
        }
        if (bytes_written == -1) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
        }
        return bytes_written;
    }
}


#ifdef __APPLE__
#include <mach/mach_time.h>

static mach_timebase_info_data_t _TIMEBASE;
static pthread_once_t _TIMEBASE_INIT = PTHREAD_ONCE_INIT;

static void _timebase_init(void) {
    if (mach_timebase_info(&_TIMEBASE) != KERN_SUCCESS) {
        fprintf(stderr, "CRITICAL! mach_timebase_info() failed\n");
        abort();
    }
}

void get_nanotime(struct timespec *timespec) {
    uint64_t t;

    pthread_once(&_TIMEBASE_INIT, _timebase_init);
    t = mach_absolute_time() * _TIMEBASE.numer / _TIMEBASE.denom;
    timespec->tv_sec = t / SECONDS_TO_NANOSECONDS(1);
    timespec->tv_nsec = t - SECONDS_TO_NANOSECONDS(timespec->tv_sec);
}
#else
void get_nanotime(struct timespec *timespec) {
    if (clock_gettime(CLOCK_MONOTONIC, timespec) != 0) {
        fprintf(stderr, "CRITICAL! clock_gettime(): %s\n", strerror(errno));
        abort();
    }
}
#endif


inline void timespec_normalize(struct timespec *result) {
    long overflow_seconds;

    overflow_seconds = result->tv_nsec / SECONDS_TO_NANOSECONDS(1);
    result->tv_sec += overflow_seconds;
    result->tv_nsec -= SECONDS_TO_NANOSECONDS(overflow_seconds);

    if (result->tv_sec > 0 && result->tv_nsec < 0) {
        result->tv_sec -= 1;
        result->tv_nsec += SECONDS_TO_NANOSECONDS(1);
    }

    if (result->tv_sec < 0 && result->tv_nsec > 0) {
        result->tv_sec += 1;
        result->tv_nsec -= SECONDS_TO_NANOSECONDS(1);
    }
}


void timespec_add(struct timespec *result, const struct timespec *a, const struct timespec *b) {
    result->tv_sec = a->tv_sec + b->tv_sec;
    result->tv_nsec = a->tv_nsec + b->tv_nsec;
    timespec_normalize(result);
}


void timespec_subtract(struct timespec *result, const struct timespec *a, const struct timespec *b) {
    result->tv_sec = a->tv_sec - b->tv_sec;
    result->tv_nsec = a->tv_nsec - b->tv_nsec;
    timespec_normalize(result);
}
