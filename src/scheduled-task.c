#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <unistd.h>
#include "scheduled-task.h"
#include "utils.h"


static void * scheduled_task_thread_main(void *ptr) {
    struct scheduled_task *scheduled_task = ptr;
    int ret;
    fd_set read_fds;
    struct timespec start, end, delta;

    for (;;) {
        get_nanotime(&start);
        scheduled_task->callback(scheduled_task->data);
        get_nanotime(&end);

        timespec_subtract(&delta, &end, &start);
        timespec_subtract(&delta, &scheduled_task->interval, &delta);
        if (delta.tv_sec < 0 || delta.tv_nsec < 0) {
            continue;
        }

        FD_ZERO(&read_fds);
        FD_SET(scheduled_task->shutdown_pipe[0], &read_fds);
        ret = pselect(scheduled_task->shutdown_pipe[0] + 1, &read_fds, NULL, NULL, &delta, NULL);
        if (ret == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "pselect(): %s\n", strerror(errno));
            }
            break;
        } else if (ret > 0) {
            break;
        }
    }

    return NULL;
}


int scheduled_task_init(struct scheduled_task **result, struct timespec *interval, void (*callback)(void *), void *data) {
    int err;
    struct scheduled_task *scheduled_task = NULL;

    scheduled_task = calloc(1, sizeof(scheduled_task[0]));
    if (scheduled_task != NULL) {
        scheduled_task->callback = callback;
        scheduled_task->data = data;
        scheduled_task->interval = *interval;
        scheduled_task->shutdown_pipe[0] = -1;
        scheduled_task->shutdown_pipe[1] = -1;
    } else {
        fprintf(stderr, "Failed to allocate scheduled_task: %s\n", strerror(errno));
        goto fail;
    }

    if (pipe(scheduled_task->shutdown_pipe) != 0) {
        fprintf(stderr, "Unable to create shutdown pipe: %s\n", strerror(errno));
        goto fail;
    }
    if (set_nonblocking(scheduled_task->shutdown_pipe[0]) != 0) {
        fprintf(stderr, "Problem setting shutdown pipe (read side) to be nonblocking: %s\n", strerror(errno));
        goto fail;
    }
    if (set_nonblocking(scheduled_task->shutdown_pipe[1]) != 0) {
        fprintf(stderr, "Problem setting shutdown pipe (write side) to be nonblocking: %s\n", strerror(errno));
        goto fail;
    }

    if ((err = pthread_create(&scheduled_task->thread, NULL, scheduled_task_thread_main, scheduled_task)) == 0) {
        scheduled_task->thread_initialized = 1;
    } else {
        fprintf(stderr, "Failed to create scheduled task thread: %s\n", strerror(err));
        goto fail;
    }

    *result = scheduled_task;
    return 0;

fail:
    scheduled_task_destroy(scheduled_task);
    return 1;
}


void scheduled_task_destroy(struct scheduled_task *scheduled_task) {
    if (scheduled_task != NULL) {
        force_write(scheduled_task->shutdown_pipe[1], 0);

        if (scheduled_task->thread_initialized) {
            pthread_join(scheduled_task->thread, NULL);
        }
        if (scheduled_task->shutdown_pipe[0] != -1) {
            close(scheduled_task->shutdown_pipe[0]);
        }
        if (scheduled_task->shutdown_pipe[1] != -1) {
            close(scheduled_task->shutdown_pipe[1]);
        }

        free(scheduled_task);
    }
}
