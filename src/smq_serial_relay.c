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
            printf("MARC: %s\n", marccmd);
            if ((*marccmd == '@' || *marccmd == '#') && fd != -1)
            {
                char cr = '\r';
                if (*marccmd == '@')
                    marccmd += 1;
                printf("GOT \"%s\"\n", marccmd);
                write(fd, marccmd, strlen(marccmd));
                write(fd, &cr, 1);
            }
        }
        json_object_put(jobj);
    }
    json_tokener_free(tok);
}

static char buildCommand(char ch, char* output_str, size_t output_size)
{
    static uint8_t sPos = 0;
    if (ch == '\n')
    {
        output_str[sPos]='\0';
        sPos = 0;
        printf("%s\n", output_str);
        return 0;
    }
    if (ch == '\r')
    {
        output_str[sPos]='\0';
        sPos = 0;
        return 1;
    }
    output_str[sPos] = ch;
    if (sPos <= output_size-1)
        sPos++;
    return 0;
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
        fd = smq_open_serial(sport, baud, 0);
        if (fd == -1)
        {
            sleep(5);
            if (trycount++ < 10)
                printf("Trying again ...\n");
        }
    }
    /* eat any garbage */
    char ch;
    while (read(fd, &ch, 1) == 1)
        ;
    smq_register_fd(fd);
    char cmdBuffer[2048];

    while (smq_spin_once(-1) > 0)
    {
        if (smq_available(fd))
        {
            while (read(fd, &ch, 1) == 1)
            {
                if (buildCommand(ch, cmdBuffer, sizeof(cmdBuffer)))
                {
                    char jsonBuffer[2148];
                    snprintf(jsonBuffer, sizeof(jsonBuffer), "{ \"cmd\": \"%s\" }", cmdBuffer);
                    printf("publish \"%s\"\n", jsonBuffer);
                    smq_publish_hash("MARC", (const uint8_t*)jsonBuffer, strlen(jsonBuffer));
                }
            }
        }
    }
    return smq_close_serial(fd);
}
