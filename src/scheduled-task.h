#ifndef __SCHEDULED_TASK_H
#define __SCHEDULED_TASK_H

#include <pthread.h>
#include <time.h>

struct scheduled_task {
    pthread_t thread;
    int thread_initialized;
    int shutdown_pipe[2];
    void (*callback)(void *);
    void *data;
    struct timespec interval;
};

int scheduled_task_init(struct scheduled_task **result, struct timespec *interval, void (*callback)(void *), void *data);
void scheduled_task_destroy(struct scheduled_task *scheduled_task);

#endif
