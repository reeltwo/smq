#include <stdlib.h>
#include "smq.h"

static void messageCallback(const char* topic_name, const uint8_t* msg, size_t len, void* arg)
{
    printf("%s:%.*s\n", topic_name, (int)len, msg);
    fflush(stdout);
}

int main(int argc, const char* argv[])
{
    int i;
    /* Initialize dzmq */
    if (!smq_init()) return 1;

    /* Subscribe */
    for (i = 1; i < argc; i++)
    {
        const char* event = argv[i];
        if (!smq_subscribe(event, messageCallback, NULL)) printf("failed to subscribe to '%s'\n", event);
        if (!smq_subscribe_hash(event, messageCallback, NULL)) printf("failed to subscribe to '%s'\n", event);
    }
    /* Spin */
    fflush(stdout);
    if(!smq_spin()) return 1;

    return 0;
}
