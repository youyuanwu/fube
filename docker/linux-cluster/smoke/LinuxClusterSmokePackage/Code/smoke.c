#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    char *end = NULL;
    long port = argc == 2 ? strtol(argv[1], &end, 10) : 0;
    if (argc != 2 || end == argv[1] || *end != '\0' || port < 1 || port > 65535) {
        fprintf(stderr, "Usage: smoke PORT (1-65535)\n");
        return 1;
    }
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        perror("socket");
        return 1;
    }
    int enabled = 1;
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons((unsigned short)port),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };
    if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0 ||
        bind(listener, (struct sockaddr *)&address, sizeof(address)) < 0 ||
        listen(listener, 8) < 0) {
        perror("listener setup");
        close(listener);
        return 1;
    }
    printf("G1 native smoke listening on port %ld; pid=%ld\n", port, (long)getpid());
    fflush(stdout);
    const char response[] = "HTTP/1.0 200 OK\r\nContent-Length: 19\r\n"
                            "Connection: close\r\n\r\nG1 native smoke OK\n";
    for (;;) {
        int client = accept(listener, NULL, NULL);
        if (client < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            close(listener);
            return 1;
        }
        char request[1024];
        if (recv(client, request, sizeof(request), 0) <= 0) {
            fprintf(stderr, "Unable to read smoke request\n");
            close(client);
            continue;
        }
        size_t offset = 0;
        while (offset < sizeof(response) - 1) {
            ssize_t sent = send(client, response + offset, sizeof(response) - 1 - offset,
                                MSG_NOSIGNAL);
            if (sent < 0 && errno == EINTR) {
                continue;
            }
            if (sent <= 0) {
                perror("send");
                break;
            }
            offset += (size_t)sent;
        }
        close(client);
    }
}
