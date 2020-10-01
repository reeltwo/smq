#include <stdlib.h>
#include <json-c/json.h>
#include "smq.h"

static void message_callback(const char* topic_name, const uint8_t* msg, size_t len, void* arg)
{
    json_tokener* tok = json_tokener_new();
    json_object* jobj = json_tokener_parse_ex(tok, (const char*)msg, len);
    if (jobj != NULL)
    {
        json_object_object_foreach(jobj, key, val)
        {
            int val_type = json_object_get_type(val);
            switch (val_type)
            {
                case json_type_null:
                    printf("%s:NULL\n", key);
                    break;
                case json_type_boolean:
                    printf("%s:%s\n", key, json_object_get_boolean(val) ? "true": "false");
                    break;
                case json_type_double:
                    printf("%s:%g\n", key, json_object_get_double(val));
                    break;
                case json_type_int:
                    printf("%s:%d\n", key, json_object_get_int(val));
                    break;
                case json_type_string:
                    printf("%s:\"%s\"\n", key, json_object_get_string(val));
                    break;
                case json_type_object:
                    break;
                case json_type_array:
                    // Support int array as byte-buffer
                break;
            }
        }
    }
    json_tokener_free(tok);
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
    setvbuf(stdout, NULL, _IONBF, 0);
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
