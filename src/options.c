#include <errno.h>
#include <getopt.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include "options.h"
#include "utils.h"


static void addrinfos_destroy(struct addrinfo **addrinfos, size_t num_addrinfos) {
    size_t i;

    if (addrinfos != NULL) {
        for (i = 0; i < num_addrinfos; i++) {
            freeaddrinfo(addrinfos[i]);
        }
        free(addrinfos);
    }
}


static int addrinfos_init(struct addrinfo ***result_addrinfos, size_t *result_num_addrinfos, int argc, char * const argv[]) {
    int i, err;
    char *colon_idx, *node, *service;
    size_t num_addrinfos = 0, capacity = 0;
    struct addrinfo hints, *addrinfo, **addrinfos = NULL, **tmp;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    for (i = 0; i < argc; i++) {
        node = argv[i];

        colon_idx = strchr(node, ':');
        if (colon_idx == NULL) {
            fprintf(stderr, "Address must include a port: %s\n", node);
            goto fail;
        }

        colon_idx[0] = '\0';
        service = colon_idx + 1;

        err = getaddrinfo(node, service, &hints, &addrinfo);
        if (err != 0) {
            fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
            goto fail;
        }

        if (addrinfo != NULL) {
            if (num_addrinfos == capacity) {
                capacity = MAX(capacity + 1, capacity * 2);
                tmp = realloc(addrinfos, capacity * sizeof(addrinfos[0]));
                if (tmp == NULL) {
                    fprintf(stderr, "Failed to malloc space for addr lists array: %s\n", strerror(errno));
                    goto fail;
                }
                addrinfos = tmp;
            }
            addrinfos[num_addrinfos++] = addrinfo;
        }
    }

    *result_addrinfos = addrinfos;
    *result_num_addrinfos = num_addrinfos;
    return 0;

fail:
    addrinfos_destroy(addrinfos, num_addrinfos);
    return 1;
}


static void print_usage(const char *program_name) {
    fprintf(stderr, "usage: %s [options] address...\n"
                    "options:\n"
                    "  --shared-sockets,-a:     share sockets between threads\n"
                    "  --packets-per-burst=,-b: number of packets to send at a time\n"
                    "  --help,-h:               prints this message\n"
                    "  --packet-size=,-l:       size of each packet, excluding headers\n"
                    "  --mode=,-m:              (recv|recvmsg|recvmmsg|send|sendmsg|sendmmsg)\n"
                    "  --spin,-s:               use spinloop that calls nonblocking recv*\n"
                    "  --threads=,-t:            number of IO threads\n",
                    program_name);
}


int options_init(
        struct options **result,
        const char *default_mode,
        int default_packet_size,
        int default_packets_per_burst,
        int default_shared_sockets,
        int default_spin,
        int default_num_threads) {

    struct options *options = NULL;

    options = calloc(1, sizeof(options[0]));
    if (options == NULL) {
        fprintf(stderr, "Failed to malloc space for options struct: %s\n", strerror(errno));
        goto fail;
    }

    if (str_dup(&options->mode, default_mode) != 0) {
        fprintf(stderr, "Failed to copy string: %s\n", strerror(errno));
        goto fail;
    }

    options->packet_size = default_packet_size;
    options->packets_per_burst = default_packets_per_burst;
    options->shared_sockets = default_shared_sockets;
    options->spin = default_spin;
    options->num_threads = default_num_threads;

    *result = options;
    return 0;

fail:
    options_destroy(options);
    return 1;
}


int options_parse_args(struct options *options, int argc, char * const argv[]) {
    int c;
    struct option longopts[] = {
        {"shared-sockets",    no_argument,       NULL, 'a'},
        {"packets-per-burst", required_argument, NULL, 'b'},
        {"help",              no_argument,       NULL, 'h'},
        {"packet-size",       required_argument, NULL, 'l'},
        {"mode",              required_argument, NULL, 'm'},
        {"spin",              no_argument,       NULL, 's'},
        {"threads",           required_argument, NULL, 't'},
        {NULL,                0,                 NULL, 0},
    };

    for (;;) {
        c = getopt_long(argc, argv, "ab:hl:m:st:", longopts, NULL);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'a':
                options->shared_sockets = 1;
                break;

            case 'b':
                if (str_to_int(&options->packets_per_burst, optarg) != 0) {
                    fprintf(stderr, "packets per burst must be a positive integer!\n");
                    goto fail;
                }
                break;

            case 'h':
                print_usage(argc > 0 ? argv[0] : NULL);
                goto fail;

            case 'l':
                if (str_to_int(&options->packet_size, optarg) != 0) {
                    fprintf(stderr, "packet size must be a positive integer!\n");
                    goto fail;
                }
                break;

            case 'm':
                if (options->mode != NULL) {
                    free(options->mode);
                    options->mode = NULL;
                }
                if (str_dup(&options->mode, optarg) != 0) {
                    fprintf(stderr, "Failed to copy string: %s\n", strerror(errno));
                    goto fail;
                }
                break;

            case 's':
                options->spin = 1;
                break;

            case 't':
                if (str_to_int(&options->num_threads, optarg) != 0) {
                    fprintf(stderr, "threads must be a positive integer!\n");
                    goto fail;
                }
                break;

            default:
                print_usage(argc > 0 ? argv[0] : NULL);
                goto fail;
        }
    }

    if (addrinfos_init(&options->addrinfos, &options->num_addrinfos, argc - optind, &argv[optind]) != 0) {
        goto fail;
    }

    return 0;

fail:
    return 1;
}


void options_destroy(struct options *options) {
    if (options != NULL) {
        addrinfos_destroy(options->addrinfos, options->num_addrinfos);
        free(options->mode);
        free(options);
    }
}
