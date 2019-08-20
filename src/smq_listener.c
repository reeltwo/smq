#include <stdlib.h>
#include <json-c/json.h>
#include "smq.h"

static size_t sReceiveCount_head;
// static size_t sReceiveCount_body;
static size_t sReceiveCount_DomeState;
static float sReceiveCount_HZ;

// static float sReceiveCount_State;
static unsigned long long sStartTime;

static void Refresh_callback(const char* topic_name, const uint8_t* msg, size_t len, void* arg)
{
    unsigned long long curTime = smq_current_time();
    long numSeconds = (long)(curTime - sStartTime) / 1000;
    if (numSeconds > 0)
        sReceiveCount_HZ = (sReceiveCount_head / (float)numSeconds);
    sReceiveCount_DomeState++;
}

static void DomeState_callback(const char* topic_name, const uint8_t* msg, size_t len, void* arg)
{
    printf("DomeState : %s\n", msg);
}

static void PWM_callback(const char* topic_name, const uint8_t* msg, size_t len, void* arg)
{
    printf("PWM : %s\n", msg);
}

// void crc1F30_callback(const char* topic_name, const uint8_t* msg, size_t len)
// {
//     smsg_t smsg;
//     if (smsg_init(&smsg, msg, len) == 0)
//     {
//         // printf("Received message %s[%d]\n", topic_name, len);
//         uint32_t u32 = smsg_read_uint32(&smsg);
//         smsg_complete(&smsg);
//         // printf("u32: 0x%08X\n", u32);
//         sReceiveCount_crc1F30++;
//     }
//     else
//     {
//         printf("Decoding failed : [%s] %d\n", topic_name, decoded_line);
//     }
// }

#if 0
void publish_callback()
{
    // char msg[255];
    // static size_t seq;
    long elapsedTime = (long)(smq_current_time() - sStartTime);
    // sprintf(msg, "Hello World: %lu [%lu:%lu] [heading=0x%04X]", seq++, sReceiveCount_head, sReceiveCount_body, headingsaved);
    printf("Elapsed : %dms %.2fHZ [%d]\n", (int)elapsedTime, sReceiveCount_HZ, sReceiveCount_DomeState);
{
    static char sState;
    sState = !sState;
    json_object* jobj = json_object_new_object();
    json_object_object_add(jobj, "seq", json_object_new_integer(sState));
    json_object_object_add(jobj, "color", json_object_new_integer(sState));
    json_object_object_add(jobj, "scale", json_object_new_integer(sState));
    json_object_object_add(jobj, "sec", json_object_new_integer(4));
    const char* msg = json_object_to_json_string(jobj);
    printf("msg : %s\n", msg);
    smq_publish_hash("logiceffect", msg, strlen(msg));
    json_object_put(jobj);
}
// {
//     static char sState;
//     sState = !sState;
//     json_object* jobj = json_object_new_object();
//     json_object_object_add(jobj, "state", json_object_new_boolean(sState));
//     const char* msg = json_object_to_json_string(jobj);
//     printf("msg : %s\n", msg);
//     smq_publish_hash("led2", msg, strlen(msg));
//     json_object_put(jobj);
// }
// {
//     static char sState;
//     sState = !sState;
//     json_object* jobj = json_object_new_object();
//     json_object_object_add(jobj, "state", json_object_new_boolean(sState));
//     const char* msg = json_object_to_json_string(jobj);
//     printf("msg : %s\n", msg);
//     smq_publish_hash("led3", msg, strlen(msg));
//     json_object_put(jobj);
// }
}
#endif

int main(int argc, const char* argv[])
{
    /* Initialize dzmq */
    if (!smq_init()) return 1;

    // /* Advertise the topic */
    // if (!smq_advertise_hash("logiceffect")) return 1;
    // if (!smq_advertise_hash("led2")) return 1;
    // if (!smq_advertise_hash("led3")) return 1;

    sStartTime = smq_current_time();

    /* Subscribe */
    if (!smq_subscribe("DomeState", DomeState_callback, NULL)) printf("failed to subscribe to DomeState\n");
    if (!smq_subscribe("PWM", PWM_callback, NULL)) printf("failed to subscribe to PWM\n");

    /* Subscribe */
    //if (!smq_subscribe("BodyOrientation", body_orientation_callback)) printf("failed to subscribe to bla\n");

    // /* Subscribe */
    // if (!smq_subscribe("$crc1F30", crc1F30_callback)) printf("failed to subscribe to $crc1F30\n");

    // /* Setup timer for publish every 10 seconds */
    // if (!smq_timer(publish_callback, 10000)) return 1;

    /* Spin */
    if(!smq_spin()) return 1;

    return 0;
}
