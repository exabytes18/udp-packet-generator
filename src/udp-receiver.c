#define _GNU_SOURCE

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include "config.h"
#include "options.h"
#include "scheduled-task.h"
#include "utils.h"


struct thread_ctx {
    volatile int *stopping;
    int id;
    int packet_size;
    int packets_per_burst;
    size_t *packets_processed_since_last_report;
    size_t *bytes_processed_since_last_report;
    pthread_mutex_t *stats_mutex;
    int *fds;
    struct addrinfo **addrinfos;
    int num_sockets;
    int shutdown_fd;
    int spin;
};

struct app {
    volatile int stopping;
    size_t num_threads;
    pthread_t *threads;
    struct thread_ctx *thread_ctx;
    size_t packets_processed_since_last_report;
    size_t bytes_processed_since_last_report;
    struct timespec last_report;
    struct scheduled_task *reporter_task;
    pthread_mutex_t stats_mutex;
    int stats_mutex_initialized;
    int *fds;
    struct addrinfo **addrinfos;
    int num_sockets;
    int shutdown_pipe[2];
};

struct mode {
    char *name;
    void * (*thread_main)(void *);
};

struct timespec REPORTER_INTERVAL = {.tv_sec = 1, .tv_nsec = 0};


static void reporter_callback(void *ptr) {
    struct app *app = ptr;
    double delta_megabytes, delta_kilopackets, delta_seconds;
    struct timespec now;

    get_nanotime(&now);
    delta_seconds = (now.tv_sec - app->last_report.tv_sec) + (now.tv_nsec - app->last_report.tv_nsec) / 1e9;

    pthread_mutex_lock(&app->stats_mutex);
    delta_megabytes = (double) app->bytes_processed_since_last_report / 1024 / 1024;
    delta_kilopackets = (double) app->packets_processed_since_last_report / 1000;
    app->bytes_processed_since_last_report = 0;
    app->packets_processed_since_last_report = 0;
    pthread_mutex_unlock(&app->stats_mutex);

    if (app->last_report.tv_sec != 0 || app->last_report.tv_nsec != 0) {
        printf("Receiving: %.3lf kpacket/s, %.3lf MB/s\n",
            delta_kilopackets / delta_seconds,
            delta_megabytes / delta_seconds);
        fflush(stdout);
    }
    app->last_report = now;
}


static void * recv_thread_main(void *ptr) {
    struct thread_ctx *thread_ctx = ptr;
    int i, j, num_ready;
    ssize_t bytes_received;
    size_t iter_bytes_received, iter_packets_received;
    char *desc = NULL, *data = NULL;
    struct pollfd *poll_fds = NULL;

    if (format_desc(&desc, thread_ctx->id, thread_ctx->addrinfos, thread_ctx->num_sockets) != 0) {
        goto done;
    }

    printf("%s", desc);
    if (thread_ctx->num_sockets > 0) {
        data = calloc(thread_ctx->packet_size, 1);
        if (data == NULL) {
            fprintf(stderr, "Failed to allocate data array: %s\n", strerror(errno));
            goto done;
        }

        poll_fds = calloc(thread_ctx->num_sockets + 1, sizeof(poll_fds[0]));
        if (poll_fds == NULL) {
            fprintf(stderr, "Failed to allocate pollfd array: %s\n", strerror(errno));
            goto done;
        }

        for (i = 0; i < thread_ctx->num_sockets; i++) {
            poll_fds[i].fd = thread_ctx->fds[i];
            poll_fds[i].events = POLLIN;
        }

        poll_fds[thread_ctx->num_sockets].fd = thread_ctx->shutdown_fd;
        poll_fds[thread_ctx->num_sockets].events = POLLIN;

        while (!*thread_ctx->stopping) {
            iter_bytes_received = 0;
            iter_packets_received = 0;

            for (i = 0; i < thread_ctx->num_sockets; i++) {
                for (j = 0; j < thread_ctx->packets_per_burst; j++) {
                    bytes_received = recv(thread_ctx->fds[i], data, thread_ctx->packet_size, 0);
                    if (bytes_received == -1) {
                        if (errno == EAGAIN) {
                            continue;
                        } else {
                            fprintf(stderr, "Problem recv'ing packet: %s\n", strerror(errno));
                            goto done;
                        }
                    } else if (bytes_received > 0) {
                        iter_bytes_received += bytes_received;
                        iter_packets_received++;
                    }
                }
            }

            pthread_mutex_lock(thread_ctx->stats_mutex);
            *thread_ctx->bytes_processed_since_last_report += iter_bytes_received;
            *thread_ctx->packets_processed_since_last_report += iter_packets_received;
            pthread_mutex_unlock(thread_ctx->stats_mutex);

            if (!thread_ctx->spin) {
                num_ready = poll(poll_fds, thread_ctx->num_sockets + 1, -1);
                if (num_ready < 0) {
                    fprintf(stderr, "Problem waiting on poll(): %s\n", strerror(errno));
                    goto done;
                }
            }
        }
    }

done:
    free(poll_fds);
    free(data);
    free(desc);
    return NULL;
}


static void * recvmsg_thread_main(void *ptr) {
    struct thread_ctx *thread_ctx = ptr;
    int i, j, num_ready;
    ssize_t bytes_received;
    size_t iter_bytes_received, iter_packets_received;
    char *desc = NULL, *data = NULL;
    struct pollfd *poll_fds = NULL;
    struct msghdr msg;
    struct iovec iov;

    if (format_desc(&desc, thread_ctx->id, thread_ctx->addrinfos, thread_ctx->num_sockets) != 0) {
        goto done;
    }

    printf("%s", desc);
    if (thread_ctx->num_sockets > 0) {
        data = calloc(thread_ctx->packet_size, 1);
        if (data == NULL) {
            fprintf(stderr, "Failed to allocate data array: %s\n", strerror(errno));
            goto done;
        }

        poll_fds = calloc(thread_ctx->num_sockets + 1, sizeof(poll_fds[0]));
        if (poll_fds == NULL) {
            fprintf(stderr, "Failed to allocate pollfd array: %s\n", strerror(errno));
            goto done;
        }

        for (i = 0; i < thread_ctx->num_sockets; i++) {
            poll_fds[i].fd = thread_ctx->fds[i];
            poll_fds[i].events = POLLIN;
        }

        poll_fds[thread_ctx->num_sockets].fd = thread_ctx->shutdown_fd;
        poll_fds[thread_ctx->num_sockets].events = POLLIN;

        iov.iov_base = data;
        iov.iov_len = thread_ctx->packet_size;
        
        memset(&msg, 0, sizeof(msg));
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;

        while (!*thread_ctx->stopping) {
            iter_bytes_received = 0;
            iter_packets_received = 0;

            for (i = 0; i < thread_ctx->num_sockets; i++) {
                for (j = 0; j < thread_ctx->packets_per_burst; j++) {
                    bytes_received = recvmsg(thread_ctx->fds[i], &msg, 0);
                    if (bytes_received == -1) {
                        if (errno == EAGAIN) {
                            continue;
                        } else {
                            fprintf(stderr, "Problem recv'ing packet: %s\n", strerror(errno));
                            goto done;
                        }
                    } else if (bytes_received > 0) {
                        iter_bytes_received += bytes_received;
                        iter_packets_received++;
                    }
                }
            }

            pthread_mutex_lock(thread_ctx->stats_mutex);
            *thread_ctx->bytes_processed_since_last_report += iter_bytes_received;
            *thread_ctx->packets_processed_since_last_report += iter_packets_received;
            pthread_mutex_unlock(thread_ctx->stats_mutex);

            if (!thread_ctx->spin) {
                num_ready = poll(poll_fds, thread_ctx->num_sockets + 1, -1);
                if (num_ready < 0) {
                    fprintf(stderr, "Problem waiting on poll(): %s\n", strerror(errno));
                    goto done;
                }
            }
        }
    }

done:
    free(poll_fds);
    free(data);
    free(desc);
    return NULL;
}


#ifdef HAVE_RECVMMSG
static void * recvmmsg_thread_main(void *ptr) {
    struct thread_ctx *thread_ctx = ptr;
    int i, j, packets_received, num_ready;
    size_t iter_bytes_received, iter_packets_received;
    char *desc = NULL, *data = NULL;
    struct pollfd *poll_fds = NULL;
    struct mmsghdr *msgs = NULL;
    struct iovec *iovs = NULL;

    if (format_desc(&desc, thread_ctx->id, thread_ctx->addrinfos, thread_ctx->num_sockets) != 0) {
        goto done;
    }

    printf("%s", desc);
    if (thread_ctx->num_sockets > 0) {
        msgs = calloc(thread_ctx->packets_per_burst, sizeof(msgs[0]));
        if (msgs == NULL) {
            fprintf(stderr, "Failed to allocate mmsghdr array: %s\n", strerror(errno));
            goto done;
        }

        iovs = calloc(thread_ctx->packets_per_burst, sizeof(iovs[0]));
        if (iovs == NULL) {
            fprintf(stderr, "Failed to allocate iov array: %s\n", strerror(errno));
            goto done;
        }

        data = calloc(thread_ctx->packets_per_burst, thread_ctx->packet_size);
        if (data == NULL) {
            fprintf(stderr, "Failed to allocate data array: %s\n", strerror(errno));
            goto done;
        }

        for (i = 0; i < thread_ctx->packets_per_burst; i++) {
            iovs[i].iov_base = &data[i * thread_ctx->packet_size];
            iovs[i].iov_len = thread_ctx->packet_size;            
            msgs[i].msg_hdr.msg_iov = &iovs[i];
            msgs[i].msg_hdr.msg_iovlen = 1;
        }

        poll_fds = calloc(thread_ctx->num_sockets + 1, sizeof(poll_fds[0]));
        if (poll_fds == NULL) {
            fprintf(stderr, "Failed to allocate pollfd array: %s\n", strerror(errno));
            goto done;
        }

        for (i = 0; i < thread_ctx->num_sockets; i++) {
            poll_fds[i].fd = thread_ctx->fds[i];
            poll_fds[i].events = POLLIN;
        }

        poll_fds[thread_ctx->num_sockets].fd = thread_ctx->shutdown_fd;
        poll_fds[thread_ctx->num_sockets].events = POLLIN;

        while (!*thread_ctx->stopping) {
            iter_bytes_received = 0;
            iter_packets_received = 0;

            for (i = 0; i < thread_ctx->num_sockets; i++) {
                packets_received = recvmmsg(thread_ctx->fds[i], msgs, thread_ctx->packets_per_burst, 0, NULL);
                if (packets_received == -1) {
                    if (errno == EAGAIN) {
                        continue;
                    } else {
                        fprintf(stderr, "Problem recv'ing packets: %s\n", strerror(errno));
                        goto done;
                    }
                } else if (packets_received > 0) {
                    for (j = 0; j < packets_received; j++) {
                        iter_bytes_received += msgs[j].msg_len;
                    }
                    iter_packets_received += packets_received;
                }
            }

            pthread_mutex_lock(thread_ctx->stats_mutex);
            *thread_ctx->bytes_processed_since_last_report += iter_bytes_received;
            *thread_ctx->packets_processed_since_last_report += iter_packets_received;
            pthread_mutex_unlock(thread_ctx->stats_mutex);

            if (!thread_ctx->spin) {
                num_ready = poll(poll_fds, thread_ctx->num_sockets + 1, -1);
                if (num_ready < 0) {
                    fprintf(stderr, "Problem waiting on poll(): %s\n", strerror(errno));
                    goto done;
                }
            }
        }
    }

done:
    free(poll_fds);
    free(data);
    free(iovs);
    free(msgs);
    free(desc);
    return NULL;
}
#endif


static void app_destroy(struct app *app) {
    size_t i;

    if (app != NULL) {
        app->stopping = 1;
        force_write(app->shutdown_pipe[1], 0);

        for (i = 0; i < app->num_threads; i++) {
            pthread_join(app->threads[i], NULL);
        }

        for (i = 0; i < app->num_sockets; i++) {
            if (app->fds[i] != -1) {
                close(app->fds[i]);
            }
        }

        if (app->reporter_task != NULL) {
            scheduled_task_destroy(app->reporter_task);
        }

        if (app->stats_mutex_initialized) {
            pthread_mutex_destroy(&app->stats_mutex);
        }

        if (app->shutdown_pipe[0] != -1) {
            close(app->shutdown_pipe[0]);
        }

        if (app->shutdown_pipe[1] != -1) {
            close(app->shutdown_pipe[1]);
        }

        for (i = 0; i < app->num_threads; i++) {
            free(app->thread_ctx[i].addrinfos);
            free(app->thread_ctx[i].fds);
        }

        free(app->threads);
        free(app->thread_ctx);
        free(app->addrinfos);
        free(app->fds);
        free(app);
    }
}


static int app_init(struct app **result, struct options *options, void *(*thread_main)(void *)) {
    int i, err;
    size_t thread_socket_start, thread_socket_end;
    struct addrinfo *addrinfo;
    struct app *app = NULL;
    struct thread_ctx *thread_ctx;

    app = calloc(1, sizeof(app[0]));
    if (app != NULL) {
        app->shutdown_pipe[0] = -1;
        app->shutdown_pipe[1] = -1;
    } else {
        fprintf(stderr, "Failed to malloc space for app struct: %s\n", strerror(errno));
        goto fail;
    }

    if (pipe(app->shutdown_pipe) != 0) {
        fprintf(stderr, "Unable to create shutdown pipe: %s\n", strerror(errno));
        goto fail;
    }
    if (set_nonblocking(app->shutdown_pipe[0]) != 0) {
        fprintf(stderr, "Problem setting shutdown pipe (read side) to be nonblocking: %s\n", strerror(errno));
        goto fail;
    }
    if (set_nonblocking(app->shutdown_pipe[1]) != 0) {
        fprintf(stderr, "Problem setting shutdown pipe (write side) to be nonblocking: %s\n", strerror(errno));
        goto fail;
    }

    if ((err = pthread_mutex_init(&app->stats_mutex, NULL)) == 0) {
        app->stats_mutex_initialized = 1;
    } else {
        fprintf(stderr, "Failed to initialize stats mutex: %s\n", strerror(err));
        goto fail;
    }

    app->threads = calloc(options->num_threads, sizeof(app->threads[0]));
    if (app->threads == NULL) {
        fprintf(stderr, "Failed to malloc space for threads array: %s\n", strerror(errno));
        goto fail;
    }

    app->thread_ctx = calloc(options->num_threads, sizeof(app->thread_ctx[0]));
    if (app->thread_ctx == NULL) {
        fprintf(stderr, "Failed to malloc space for thread_ctx array: %s\n", strerror(errno));
        goto fail;
    }

    app->num_sockets = options->num_addrinfos;
    
    app->fds = malloc(app->num_sockets * sizeof(app->fds[0]));
    if (app->fds == NULL) {
        fprintf(stderr, "Failed to malloc space for fds array: %s\n", strerror(errno));
        goto fail;
    }
    for (i = 0; i < app->num_sockets; i++) {
        app->fds[i] = -1;
    }

    app->addrinfos = malloc(app->num_sockets * sizeof(app->addrinfos[0]));
    if (app->addrinfos == NULL) {
        fprintf(stderr, "Failed to malloc space for addrinfos array: %s\n", strerror(errno));
        goto fail;
    }
    for (i = 0; i < options->num_addrinfos; i++) {
        app->addrinfos[i] = options->addrinfos[i];
    }

    for (i = 0; i < app->num_sockets; i++) {
        addrinfo = app->addrinfos[i];
        app->fds[i] = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (app->fds[i] == -1) {
            fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
            goto fail;
        }
        if (set_nonblocking(app->fds[i]) != 0) {
            fprintf(stderr, "Problem setting fd to be nonblocking: %s\n", strerror(errno));
            goto fail;
        }
        if (bind(app->fds[i], addrinfo->ai_addr, addrinfo->ai_addrlen) != 0) {
            fprintf(stderr, "Failed to connect socket: %s\n", strerror(errno));
            goto fail;
        }
    }

    while (app->num_threads < options->num_threads) {
        thread_ctx = &app->thread_ctx[app->num_threads];
        thread_ctx->stopping = &app->stopping;
        thread_ctx->id = app->num_threads;
        thread_ctx->packet_size = options->packet_size;
        thread_ctx->packets_per_burst = options->packets_per_burst;
        thread_ctx->packets_processed_since_last_report = &app->packets_processed_since_last_report;
        thread_ctx->bytes_processed_since_last_report = &app->bytes_processed_since_last_report;
        thread_ctx->stats_mutex = &app->stats_mutex;
        thread_ctx->shutdown_fd = app->shutdown_pipe[0];
        thread_ctx->spin = options->spin;

        if (options->shared_sockets) {
            thread_socket_start = 0;
            thread_socket_end = app->num_sockets;
        } else {
            thread_socket_start = BLOCK_LOW(app->num_threads, options->num_threads, app->num_sockets);
            thread_socket_end = BLOCK_LOW(app->num_threads + 1, options->num_threads, app->num_sockets);
        }

        thread_ctx->num_sockets = thread_socket_end - thread_socket_start;
        thread_ctx->fds = malloc(thread_ctx->num_sockets * sizeof(thread_ctx->fds[0]));
        if (thread_ctx->fds == NULL) {
            fprintf(stderr, "Failed to malloc space for thread fds array: %s\n", strerror(errno));
            goto fail;
        }

        thread_ctx->addrinfos = malloc(thread_ctx->num_sockets * sizeof(thread_ctx->addrinfos[0]));
        if (thread_ctx->addrinfos == NULL) {
            fprintf(stderr, "Failed to malloc space for thread addrinfos array: %s\n", strerror(errno));
            goto fail;
        }

        for (i = 0; i < thread_ctx->num_sockets; i++) {
            thread_ctx->fds[i] = app->fds[i + thread_socket_start];
            thread_ctx->addrinfos[i] = app->addrinfos[i + thread_socket_start];
        }

        if ((err = pthread_create(&app->threads[app->num_threads], NULL, thread_main, thread_ctx)) == 0) {
            app->num_threads++;
        } else {
            fprintf(stderr, "Failed to create receiver thread: %s\n", strerror(err));
            goto fail;
        }
    }

    if (scheduled_task_init(&app->reporter_task, &REPORTER_INTERVAL, reporter_callback, app) != 0) {
        goto fail;
    }

    *result = app;
    return 0;

fail:
    app_destroy(app);
    return 1;
}


int main(int argc, char * const argv[]) {
    int i, err, sig, status;
    sigset_t mask;
    struct app *app;
    struct options *options;
    struct mode available_modes[] = {
        {"recv",    recv_thread_main},
        {"recvmsg", recvmsg_thread_main},
#ifdef HAVE_RECVMMSG
        {"recvmmsg", recvmmsg_thread_main},
#endif
    };
    void *(*thread_main)(void *) = NULL;

    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);

    if ((err = pthread_sigmask(SIG_BLOCK, &mask, NULL)) != 0) {
        fprintf(stderr, "Problem installing sigmask: %s\n", strerror(err));
        status = EXIT_FAILURE;
        goto done1;
    }

    if (options_init(&options, "recv", 1, 1, 0, 0, 1) != 0) {
        status = EXIT_FAILURE;
        goto done1;
    }

    if (options_parse_args(options, argc, argv) != 0) {
        status = EXIT_FAILURE;
        goto done2;
    }

    for (i = 0; i < sizeof(available_modes) / sizeof(available_modes[0]); i++) {
        if (strcmp(available_modes[i].name, options->mode) == 0) {
            thread_main = available_modes[i].thread_main;
            break;
        }
    }

    if (thread_main == NULL) {
        fprintf(stderr, "Unsupported mode: %s\n", options->mode);
        status = EXIT_FAILURE;
        goto done2;
    }

    if (options->num_addrinfos <= 0) {
        fprintf(stderr, "You must specify at least one address!\n");
        status = EXIT_FAILURE;
        goto done2;
    }

    if (options->packet_size <= 0) {
        fprintf(stderr, "Packet size must be a positive integer!\n");
        status = EXIT_FAILURE;
        goto done2;
    }

    if (options->packets_per_burst <= 0) {
        fprintf(stderr, "Packets per burst must be a positive integer!\n");
        status = EXIT_FAILURE;
        goto done2;
    }

    if (options->num_threads <= 0) {
        fprintf(stderr, "Number of threads must be a positive integer!\n");
        status = EXIT_FAILURE;
        goto done2;
    }

    if (app_init(&app, options, thread_main) != 0) {
        status = EXIT_FAILURE;
        goto done2;
    }

    if ((err = sigwait(&mask, &sig)) != 0) {
        fprintf(stderr, "Problem waiting for signal: %s\n", strerror(err));
        status = EXIT_FAILURE;
        goto done3;
    }

    status = EXIT_SUCCESS;

done3:
    app_destroy(app);
done2:
    options_destroy(options);
done1:
    return status;
}
