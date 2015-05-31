#ifndef __UTILS_H
#define __UTILS_H

#include <netdb.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>

#define MIN(x, y) ((x < y) ? (x) : (y))
#define MAX(x, y) ((x > y) ? (x) : (y))
#define BLOCK_LOW(id,p,n) ((id)*(n)/(p))
#define SECONDS_TO_NANOSECONDS(seconds) (seconds * 1000000000);

int format_desc(char **desc, int thread_id, struct addrinfo **addrinfos, size_t num_addrinfos);
void str_append(char **str, const char *suffix);
int str_to_int(int *result, const char *str);
int str_dup(char **result, const char *original);
int set_nonblocking(int fd);
int force_write(int fd, char b);
void get_nanotime(struct timespec *timespec);
void timespec_normalize(struct timespec *result);
void timespec_add(struct timespec *result, const struct timespec *a, const struct timespec *b);
void timespec_subtract(struct timespec *result, const struct timespec *a, const struct timespec *b);

#endif
