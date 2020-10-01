#include <stdlib.h>
#include <json-c/json.h>
#include "smq.h"

static void message_callback(const char* topic_name, const uint8_t* msg, size_t len, void* arg)
{
    printf("%s : %s\n", topic_name, msg);
}

int main(int argc, const char* argv[])
{
    const char* progname = argv[0];
    const char* topic = argv[1];

    if (argc != 2)
    {
        fprintf(stderr, "%s: <topic>\n", progname);
        return 1;
    }
    /* Initialize dzmq */
    if (!smq_init()) return 1;

    /* Subscribe */
    if (!smq_advertise(topic)) return 1;
    if (!smq_advertise_hash(topic)) return 1;
    if (!smq_subscribe(topic, message_callback, NULL)) printf("failed to subscribe to %s\n", topic);
    if (!smq_subscribe_hash(topic, message_callback, NULL)) printf("failed to subscribe to %s\n", topic);

    /* Spin */
    if(!smq_spin()) return 1;

    return 0;
}
