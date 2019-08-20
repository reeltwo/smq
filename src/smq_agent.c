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

	const char* sport = (argc >= 2) ? argv[1] : "/dev/ttyUSB0";
	int baud = (argc == 3) ? atoi(argv[2]) : 115200;
	int fd = smq_subscribe_serial(sport, 0);
	smq_spin();
	return smq_unsubscribe_serial(fd);
}
