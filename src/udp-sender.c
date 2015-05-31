#define _GNU_SOURCE

#include <errno.h>
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
    size_t num_addrinfos;
    struct addrinfo **addrinfos;
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
        printf("Sending: %.3lf kpacket/s, %.3lf MB/s\n",
            delta_kilopackets / delta_seconds,
            delta_megabytes / delta_seconds);
        fflush(stdout);
    }
    app->last_report = now;
}


static void * send_thread_main(void *ptr) {
    struct thread_ctx *thread_ctx = ptr;
    int i, j, *fds = NULL;
    ssize_t bytes_sent;
    size_t iter_bytes_sent, iter_packets_sent;
    char *desc = NULL, *data = NULL;
    struct addrinfo *addrinfo;

    if (format_desc(&desc, thread_ctx->id, thread_ctx->addrinfos, thread_ctx->num_addrinfos) != 0) {
        goto done;
    }

    printf("%s", desc);
    if (thread_ctx->num_addrinfos > 0) {
        data = calloc(thread_ctx->packet_size, 1);
        if (data == NULL) {
            fprintf(stderr, "Failed to allocate data array: %s\n", strerror(errno));
            goto done;
        }

        fds = calloc(thread_ctx->num_addrinfos, sizeof(fds[0]));
        if (fds == NULL) {
            fprintf(stderr, "Failed to allocate fd array: %s\n", strerror(errno));
            goto done;
        }
        for (i = 0; i < thread_ctx->num_addrinfos; i++) {
            fds[i] = -1;
        }
        for (i = 0; i < thread_ctx->num_addrinfos; i++) {
            addrinfo = thread_ctx->addrinfos[i];
            fds[i] = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
            if (fds[i] == -1) {
                fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
                goto done;
            }
            if (connect(fds[i], addrinfo->ai_addr, addrinfo->ai_addrlen) != 0) {
                fprintf(stderr, "Failed to connect socket: %s\n", strerror(errno));
                goto done;
            }
        }

        while (!*thread_ctx->stopping) {
            iter_bytes_sent = 0;
            iter_packets_sent = 0;

            for (i = 0; i < thread_ctx->num_addrinfos; i++) {
                for (j = 0; j < thread_ctx->packets_per_burst; j++) {
                    bytes_sent = send(fds[i], data, thread_ctx->packet_size, 0);
                    if (bytes_sent == -1) {
                        fprintf(stderr, "Problem sending packet: %s\n", strerror(errno));
                        goto done;
                    } else if (bytes_sent > 0) {
                        iter_bytes_sent += bytes_sent;
                        iter_packets_sent++;
                    }
                }
            }

            pthread_mutex_lock(thread_ctx->stats_mutex);
            *thread_ctx->bytes_processed_since_last_report += iter_bytes_sent;
            *thread_ctx->packets_processed_since_last_report += iter_packets_sent;
            pthread_mutex_unlock(thread_ctx->stats_mutex);
        }
    }

done:
    if (fds != NULL) {
        for (i = 0; i < thread_ctx->num_addrinfos; i++) {
            if (fds[i] != -1) {
                close(fds[i]);
            }
        }
        free(fds);
    }
    if (data != NULL) {
        free(data);
    }
    if (desc != NULL) {
        free(desc);
    }
    return NULL;
}


static void * sendmsg_thread_main(void *ptr) {
    struct thread_ctx *thread_ctx = ptr;
    int i, j, fd = -1;
    ssize_t bytes_sent;
    size_t iter_bytes_sent, iter_packets_sent;
    char *desc = NULL, *data = NULL;
    struct addrinfo *addrinfo;
    struct msghdr msg;
    struct iovec iov;

    if (format_desc(&desc, thread_ctx->id, thread_ctx->addrinfos, thread_ctx->num_addrinfos) != 0) {
        goto done;
    }

    printf("%s", desc);
    if (thread_ctx->num_addrinfos > 0) {
        data = calloc(thread_ctx->packet_size, 1);
        if (data == NULL) {
            fprintf(stderr, "Failed to allocate data array: %s\n", strerror(errno));
            goto done;
        }

        fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (fd == -1) {
            fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
            goto done;
        }

        iov.iov_base = data;
        iov.iov_len = thread_ctx->packet_size;

        memset(&msg, 0, sizeof(msg));
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;

        while (!*thread_ctx->stopping) {
            iter_bytes_sent = 0;
            iter_packets_sent = 0;

            for (i = 0; i < thread_ctx->num_addrinfos; i++) {
                addrinfo = thread_ctx->addrinfos[i];
                msg.msg_name = addrinfo->ai_addr;
                msg.msg_namelen = addrinfo->ai_addrlen;

                for (j = 0; j < thread_ctx->packets_per_burst; j++) {
                    bytes_sent = sendmsg(fd, &msg, 0);
                    if (bytes_sent == -1) {
                        fprintf(stderr, "Problem sending packet: %s\n", strerror(errno));
                        goto done;
                    } else if (bytes_sent > 0) {
                        iter_bytes_sent += bytes_sent;
                        iter_packets_sent++;
                    }
                }
            }

            pthread_mutex_lock(thread_ctx->stats_mutex);
            *thread_ctx->bytes_processed_since_last_report += iter_bytes_sent;
            *thread_ctx->packets_processed_since_last_report += iter_packets_sent;
            pthread_mutex_unlock(thread_ctx->stats_mutex);
        }
    }

done:
    if (fd != -1) {
        close(fd);
    }
    if (data != NULL) {
        free(data);
    }
    if (desc != NULL) {
        free(desc);
    }
    return NULL;
}


static void * sendmmsg_thread_main(void *ptr) {
    struct thread_ctx *thread_ctx = ptr;
    int i, j, packets_sent, fd = -1;
    size_t iter_bytes_sent, iter_packets_sent;
    char *desc = NULL, *data = NULL;
    struct addrinfo *addrinfo;
    struct mmsghdr *msgs = NULL;
    struct iovec *iovs = NULL;

    if (format_desc(&desc, thread_ctx->id, thread_ctx->addrinfos, thread_ctx->num_addrinfos) != 0) {
        goto done;
    }

    printf("%s", desc);
    if (thread_ctx->num_addrinfos > 0) {
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

        fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (fd == -1) {
            fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
            goto done;
        }

        while (!*thread_ctx->stopping) {
            iter_bytes_sent = 0;
            iter_packets_sent = 0;

            for (i = 0; i < thread_ctx->num_addrinfos; i++) {
                addrinfo = thread_ctx->addrinfos[i];
                for (j = 0; j < thread_ctx->packets_per_burst; j++) {
                    msgs[j].msg_hdr.msg_name = addrinfo->ai_addr;
                    msgs[j].msg_hdr.msg_namelen = addrinfo->ai_addrlen;
                }

                packets_sent = sendmmsg(fd, msgs, thread_ctx->packets_per_burst, 0);
                if (packets_sent == -1) {
                    if (errno == EAGAIN) {
                        continue;
                    } else {
                        fprintf(stderr, "Problem sending packets: %s\n", strerror(errno));
                        goto done;
                    }
                } else if (packets_sent > 0) {
                    for (j = 0; j < packets_sent; j++) {
                        iter_bytes_sent += msgs[j].msg_len;
                    }
                    iter_packets_sent += packets_sent;
                }
            }

            pthread_mutex_lock(thread_ctx->stats_mutex);
            *thread_ctx->bytes_processed_since_last_report += iter_bytes_sent;
            *thread_ctx->packets_processed_since_last_report += iter_packets_sent;
            pthread_mutex_unlock(thread_ctx->stats_mutex);
        }
    }

done:
    if (fd != -1) {
        close(fd);
    }
    if (data != NULL) {
        free(data);
    }
    if (iovs != NULL) {
        free(iovs);
    }
    if (msgs != NULL) {
        free(msgs);
    }
    if (desc != NULL) {
        free(desc);
    }
    return NULL;
}


static void app_destroy(struct app *app) {
    size_t i;

    if (app != NULL) {
        app->stopping = 1;
        for (i = 0; i < app->num_threads; i++) {
            pthread_join(app->threads[i], NULL);
        }

        if (app->reporter_task != NULL) {
            scheduled_task_destroy(app->reporter_task);
        }

        if (app->stats_mutex_initialized) {
            pthread_mutex_destroy(&app->stats_mutex);
        }

        free(app->threads);
        free(app->thread_ctx);
        free(app);
    }
}


static int app_init(struct app **result, struct options *options, void *(*thread_main)(void *)) {
    int i, err;
    size_t thread_addr_start, thread_addr_end;
    struct app *app = NULL;
    struct thread_ctx *thread_ctx;

    app = calloc(1, sizeof(app[0]));
    if (app == NULL) {
        fprintf(stderr, "Failed to malloc space for app struct: %s\n", strerror(errno));
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

    while (app->num_threads < options->num_threads) {
        thread_ctx = &app->thread_ctx[app->num_threads];
        thread_ctx->stopping = &app->stopping;
        thread_ctx->id = app->num_threads;
        thread_ctx->packet_size = options->packet_size;
        thread_ctx->packets_per_burst = options->packets_per_burst;
        thread_ctx->packets_processed_since_last_report = &app->packets_processed_since_last_report;
        thread_ctx->bytes_processed_since_last_report = &app->bytes_processed_since_last_report;
        thread_ctx->stats_mutex = &app->stats_mutex;

        if (options->shared_sockets) {
            thread_addr_start = 0;
            thread_addr_end = options->num_addrinfos;
        } else {
            thread_addr_start = BLOCK_LOW(app->num_threads, options->num_threads, options->num_addrinfos);
            thread_addr_end = BLOCK_LOW(app->num_threads + 1, options->num_threads, options->num_addrinfos);
        }

        thread_ctx->num_addrinfos = thread_addr_end - thread_addr_start;
        thread_ctx->addrinfos = calloc(thread_ctx->num_addrinfos, sizeof(thread_ctx->addrinfos[0]));
        if (thread_ctx->addrinfos == NULL) {
            fprintf(stderr, "Failed to malloc space for thread addrinfo array: %s\n", strerror(errno));
            goto fail;
        }

        for (i = 0; i < thread_ctx->num_addrinfos; i++) {
            thread_ctx->addrinfos[i] = options->addrinfos[i + thread_addr_start];
        }

        if ((err = pthread_create(&app->threads[app->num_threads], NULL, thread_main, thread_ctx)) == 0) {
            app->num_threads++;
        } else {
            fprintf(stderr, "Failed to create sender thread: %s\n", strerror(err));
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
        {"send",    send_thread_main},
        {"sendmsg", sendmsg_thread_main},
#ifdef HAVE_SENDMMSG
        {"sendmmsg", sendmmsg_thread_main},
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

    if (options_init(&options, "send", 1, 1, 0, 0, 1) != 0) {
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
