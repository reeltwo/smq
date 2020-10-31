#include <stdlib.h>
#include <json-c/json.h>
#include "smq.h"

int main(int argc, const char* argv[])
{
	const char* progname = argv[0];
	const char* topic = argv[1];
	const char* msg = argv[2];
	json_object* jobj;

	if (argc != 3)
	{
		fprintf(stderr, "%s: <topic> <json>\n", progname);
		return 1;
	}
    /* Initialize dzmq */
    if (!smq_init()) return 1;

    if ((jobj = json_tokener_parse(msg)) == NULL)
    {
    	fprintf(stderr, "%s: invalid json : \"%s\"\n", progname, msg);
    	return 1;
    }
    json_object_put(jobj);

    // /* Advertise the topic - can take up to 10 seconds for topic to be subscribed to*/
    if (!smq_advertise_hash(topic)) return 1;
    smq_wait_for(10000);

    smq_publish_hash(topic, (const uint8_t*)msg, strlen(msg));
    smq_wait_for(10);
    return 0;
}
