#include <stdlib.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "smq.h"

int main(int argc, const char* argv[])
{
	const char* progname = argv[0];

    /* Initialize smq */
    if (!smq_init()) return 1;

    // /* Advertise the topic */
    if (!smq_advertise_hash("MARC")) return 1;
    smq_wait_for(1000);

    int sock;
    /* Create a best-effort datagram socket using UDP */
    if ((sock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0)
    {
        perror("socket() failed");
        exit(-1);
    }

    struct sockaddr_in broadcastAddr;
    memset(&broadcastAddr, 0, sizeof(broadcastAddr));
    broadcastAddr.sin_family = AF_INET;
    broadcastAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    broadcastAddr.sin_port = htons(2001);
    if (bind(sock, (struct sockaddr *) &broadcastAddr, sizeof(broadcastAddr)) < 0)
    {
        perror("bind() failed");
        exit(-1);
    }
    int ret = 0;
    while (0 < (ret = smq_spin_once(10)))
    {
        int cmdLen;
        char cmdBuffer[255];
        /* Receive a single datagram from the server */
        if ((ret = recvfrom(sock, cmdBuffer, sizeof(cmdBuffer), MSG_DONTWAIT, NULL, 0)) < 0)
        {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                perror("recvfrom() failed");
                break;
            }
        }
        else if ((cmdLen = ret) > 0)
        {
            char jsonBuffer[300];
            snprintf(jsonBuffer, sizeof(jsonBuffer), "{ \"cmd\": \"%s\" }", cmdBuffer);
            printf("publish %s\n", jsonBuffer);
            smq_publish_hash("MARC", (const uint8_t*)jsonBuffer, strlen(jsonBuffer));
        }
    }
    return ret;
}
