#ifndef __OPTIONS_H
#define __OPTIONS_H

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

struct options {
    int packet_size;
    int packets_per_burst;
    int shared_sockets;
    int spin;
    int num_threads;
    size_t num_addrinfos;
    struct addrinfo **addrinfos;
    char *mode;
};

int options_init(
        struct options **result,
        const char *mode,
        int default_packet_size,
        int default_packets_per_burst,
        int default_shared_sockets,
        int default_spin,
        int default_num_threads);
int options_parse_args(struct options *options, int argc, char * const argv[]);
void options_destroy(struct options *options);

#endif
