#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include "smq.h"

int main(int argc, const char* argv[])
{
    /* Initialize smq */
    if (!smq_init()) return 1;

    int trycount = 0;
	const char* sport = (argc >= 2) ? argv[1] : "/dev/ttyUSB0";
	int baud = (argc == 3) ? atoi(argv[2]) : 115200;
	int fd = -1;
    while (fd == -1)
    {
        fd = smq_subscribe_serial(sport, baud);
        if (fd == -1)
        {
            sleep(5);
            if (trycount++ < 10)
                printf("Trying again ...\n");
        }
    }
	smq_wait();
	return smq_unsubscribe_serial(fd);
}
