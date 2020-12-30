#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <json-c/json.h>
#include "smq.h"

static int fd = -1;

static void MARC_callback(const char* topic_name, const uint8_t* msg, size_t len, void* arg)
{
    json_tokener* tok = json_tokener_new();
    json_object* jobj = json_tokener_parse_ex(tok, (const char*)msg, len);
    if (jobj != NULL)
    {
        json_object* jcmd = json_object_object_get(jobj, "cmd");
        if (jcmd != NULL)
        {
            const char* marccmd = json_object_get_string(jcmd);
            if (*marccmd == '@' && fd != -1)
            {
                char cr = '\r';
                write(fd, marccmd, strlen(marccmd));
                write(fd, &cr, 1);
            }
        }
        json_object_put(jobj);
    }
    json_tokener_free(tok);
}

int main(int argc, const char* argv[])
{
    /* Initialize smq */
    if (!smq_init()) return 1;

    int trycount = 0;
	const char* sport = (argc >= 2) ? argv[1] : "/dev/ttyUSB0";
	int baud = (argc == 3) ? atoi(argv[2]) : 2400;

    if (!smq_advertise_hash("MARC")) return 1;
    if (!smq_subscribe_hash("MARC", MARC_callback, NULL)) printf("failed to subscribe to MARC\n");

    while (fd == -1)
    {
        fd = smq_open_serial(sport, baud);
        if (fd == -1)
        {
            sleep(5);
            if (trycount++ < 10)
                printf("Trying again ...\n");
        }
    }
	smq_wait();
	return smq_close_serial(fd);
}
