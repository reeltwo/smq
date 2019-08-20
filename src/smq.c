#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <sys/file.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>

#include <ifaddrs.h>
#include <arpa/inet.h>

#include <zmq.h>
#include "smq.h"

#include <json-c/json.h>

#include <uuid/uuid.h>
#ifdef __MACH__
#include <mach/mach.h>
#include <mach/clock.h>
#endif

// ---------------------------------------

/* Defaults and overrides */
#define SMQ_DISC_PORT 11312
#define SMQ_INPROC_ADDR "inproc://topics"

/* Constants */
#define SMQ_OP_ADV 0x01
#define SMQ_OP_SUB 0x02
#define SMQ_OP_PUB 0x03

#define SMQ_UDP_MAX_SIZE 512
#define SMQ_ADV_REPEAT_PERIOD 1.0
#define SMQ_MAX_TOPIC_LENGTH 193 + 1
#define SMQ_MAX_ADDR_LENGTH 267 + 1
#define SMQ_FLAGS_LENGTH 16
#define SMQ_MAX_POLL_ITEMS 1024

// ---------------------------------------

#define GUID_LEN sizeof(uuid_t)
#define GUID_STR_LEN (sizeof(uuid_t) * 2) + 4 + 1

// ---------------------------------------

typedef struct
{
    uint16_t version;
    uuid_t guid;
    char topic[SMQ_MAX_TOPIC_LENGTH];
    uint8_t type;
    uint8_t flags[SMQ_FLAGS_LENGTH];
} smq_msg_header_t;

typedef struct
{
    smq_msg_header_t header;
    char addr[SMQ_MAX_ADDR_LENGTH];
} smq_adv_msg_t;

typedef struct smq_connection_t
{
    int fd;
    char addr[SMQ_MAX_ADDR_LENGTH];
    struct smq_connection_t* next;
    struct smq_connection_t* prev;
} smq_connection_t;

typedef struct
{
    struct smq_connection_t* first;
    struct smq_connection_t* last;
} smq_connection_list_t;

typedef struct smq_topic_t
{
    char name[SMQ_MAX_TOPIC_LENGTH];
    smq_msg_callback_t* callback;
    smq_msg_callback_t* scallback;
    struct smq_topic_t* next;
    struct smq_topic_t* prev;
    void* arg;
} smq_topic_t;

typedef struct
{
    struct smq_topic_t* first;
    struct smq_topic_t* last;
} smq_topic_list_t;

// ---------------------------------------

static uuid_t GUID;

static int init_called;

static int bcast_fd;
static struct sockaddr_in dst_addr, rcv_addr;
static struct ip_mreq mreq;

static char ip_address[INET_ADDRSTRLEN];
static char bcast_address[INET_ADDRSTRLEN];
static char tcp_address[SMQ_MAX_ADDR_LENGTH];

static void* zmq_context;
static void* zmq_publish_sock;
static void* zmq_subscribe_sock;
static zmq_pollitem_t poll_items[SMQ_MAX_POLL_ITEMS];
static size_t poll_items_count;

static smq_topic_list_t published_topics;
static smq_topic_list_t subscribed_topics;
static smq_connection_list_t connections;

static smq_timer_callback_t* timer_callback;
static void* timer_arg;
static long timer_period;
static struct timespec last_timer;

static int register_file_descriptor(int fd)
{
    if (poll_items_count >= SMQ_MAX_POLL_ITEMS)
    {
        fprintf(stderr, "Too many devices to poll\n");
        return 0;
    }
    poll_items[poll_items_count].socket = 0;
    poll_items[poll_items_count].fd = fd;
    poll_items[poll_items_count].events = ZMQ_POLLIN;
    poll_items_count += 1;
    return poll_items_count;
}

static int register_socket(void* socket)
{
    if (poll_items_count >= SMQ_MAX_POLL_ITEMS)
    {
        fprintf(stderr, "Too many devices to poll\n");
        return 0;
    }
    poll_items[poll_items_count].socket = socket;
    poll_items[poll_items_count].events = ZMQ_POLLIN;
    poll_items_count += 1;
    return poll_items_count;
}

// ---------------------------------------

static int smq_connection_list_append(smq_connection_list_t* list, int fd, const char* addr)
{
    smq_connection_t* new_connection = (struct smq_connection_t *) malloc(sizeof(struct smq_connection_t));
    if (0 == new_connection)
    {
        fprintf(stderr, "Error appending connection to list\n");
        return 0;
    }
    new_connection->next = 0;
    new_connection->fd = fd;
    strncpy(new_connection->addr, addr, SMQ_MAX_ADDR_LENGTH);
    if (0 == list->last && 0 == list->first)
    {
        new_connection->prev = 0;
        list->last = new_connection;
        list->first = new_connection;
    }
    else
    {
        new_connection->prev = list->last;
        list->last->next = new_connection;
        list->last = new_connection;
    }
    return 1;
}

static int smq_connection_list_remove(smq_connection_t* connection)
{
    if (connection->next)
    {
        connection->next->prev = connection->prev;
    }
    if (connection->prev)
    {
        connection->prev->next = connection->next;
    }
    free(connection);
    return 1;
}

static smq_connection_t* smq_fd_in_list(smq_connection_list_t* list, int fd)
{
    smq_connection_t* connection = list->first;
    while (connection != 0)
    {
        if (connection->fd == fd)
        {
            break;
        }
        connection = connection->next;
    }
    return connection;
}

static smq_connection_t* smq_addr_in_list(smq_connection_list_t* list, const char* addr)
{
    smq_connection_t* connection = list->first;
    while (connection != 0)
    {
        if (0 == strcmp(connection->addr, addr))
        {
            break;
        }
        connection = connection->next;
    }
    return connection;
}

static int smq_connection_list_destroy(smq_connection_list_t* list)
{
    smq_connection_t* connection = list->first;
    while (1)
    {
        if (0 == connection->next)
        {
            free(connection);
            return 1;
        }
        connection = connection->next;
        free(connection->prev);
    }
}

// ---------------------------------------

static void smq_guid_to_str(uuid_t guid, char* guid_str, int guid_str_len)
{
    for (size_t i = 0; i < sizeof(uuid_t) && i != guid_str_len; i ++)
    {
         snprintf(guid_str, guid_str_len,
                  "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                  guid[0], guid[1], guid[2], guid[3], guid[4], guid[5], guid[6],
                  guid[7], guid[8], guid[9], guid[10], guid[11], guid[12],
                  guid[13], guid[14], guid[15]
        );
    }
}

static int smq_guid_compare(uuid_t guid, uuid_t other)
{
    for (size_t i = 0; i < sizeof(uuid_t); i ++)
    {
        if (guid[i] != other[i])
        {
            return 0;
        }
    }
    return 1;
}

// ---------------------------------------

static int smq_broadcast_ip_from_address_ip(char* ip_addr, char* bcast_addr)
{
    char temp_ip_addr[INET_ADDRSTRLEN];
    strcpy(temp_ip_addr, ip_addr);
    char* tok = strtok(temp_ip_addr, ".");
    int toks = 0;
    while (0 != tok) {
        toks++;
        char* next_tok = strtok(0, ".");
        if (0 == next_tok) {
            strcat(bcast_addr, "255");
            break;
        }
        strcat(bcast_addr, tok);
        strcat(bcast_addr, ".");
        tok = next_tok;
    }
    return 1 ? 4 == toks : 0;
}

/*
 * References:
 * http://stackoverflow.com/questions/212528/get-the-ip-address-of-the-machine
 * http://man7.org/linux/man-pages/man3/getifaddrs.3.html
 */
static int smq_get_address_ipv4(char* address)
{
    struct ifaddrs* ifAddrStruct = NULL;
    struct ifaddrs* ifa = NULL;
    void* tmpAddrPtr = NULL;

    getifaddrs(&ifAddrStruct);
    int address_found = 0;

    for (ifa = ifAddrStruct; NULL != ifa; ifa = ifa->ifa_next)
    {
        /* If IPv4 */
        if (AF_INET == ifa->ifa_addr->sa_family)
        {
            tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            inet_ntop(AF_INET, tmpAddrPtr, address, INET_ADDRSTRLEN);
        }
        else
        {
            continue;
        }
        /* stop at the first non 127.0.0.1 address */
        if (address[0] && strncmp(address, "127.0.0.1", 9))
        {
            address_found = 1;
            break;
        }
    }
    /* Free address structure */
    if (NULL != ifAddrStruct)
    {
        freeifaddrs(ifAddrStruct);
    }
    /* If address not found, zero it */
    if (!address_found)
    {
        address[0] = 0;
    }
    return address_found;
}

// ---------------------------------------

uint64_t smq_current_time()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void smq_get_time_now (struct timespec* time)
{
# if 0//def __MACH__ // OS X does not have clock_gettime, use clock_get_time
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  time->tv_sec = mts.tv_sec;
  time->tv_nsec = mts.tv_nsec;
# else
  clock_gettime(CLOCK_MONOTONIC, time);
# endif
}

static long convert_timespec_to_ms(struct timespec* time_spec)
{
    return (time_spec->tv_sec * 1e3) + (time_spec->tv_nsec / 1e6);
}

static long smq_time_till(struct timespec* last_time, long timer_period)
{
    struct timespec now;
    smq_get_time_now(&now);
    long time_since_last = (unsigned long)convert_timespec_to_ms(&now) - (unsigned long)convert_timespec_to_ms(last_time);
    return timer_period - time_since_last;
}

// ---------------------------------------

int smq_init()
{
    if (init_called)
    {
        fprintf(stderr, "smq_init called more than once\n");
        return 0;
    }
    init_called = 1;
    /* Generate uuid */
    uuid_generate(GUID);
    /* Get the IPv4 address */
    if (0 >= smq_get_address_ipv4(ip_address))
    {
        fprintf(stderr, "Error getting IPv4 address.\n");
        return 0;
    }
    /* Get the broadcast address from the IPv4 address */
    if (0 >= smq_broadcast_ip_from_address_ip(ip_address, bcast_address))
    {
        fprintf(stderr, "Error computing broadcast ip address\n");
        return 0;
    }
    /* Setup broadcast socket */
    bcast_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (0 >= bcast_fd)
    {
        fprintf(stderr, "Error opening socket\n");
        return 0;
    }
    /* Make socket non-blocking */
    if (fcntl(bcast_fd, F_SETFL, O_NONBLOCK, 1) < 0)
    {
        fprintf(stderr, "Error setting socket to non-blocking\n");
        return 0;
    }
    /* Allow multiple sockets to use the same PORT number */
    u_int yes = 1;
    if (setsockopt(bcast_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    {
        fprintf(stderr, "Reusing ADDR failed\n");
        return 0;
    }
    if (setsockopt(bcast_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes)) < 0)
    {
        fprintf(stderr, "setsockopt (SOL_SOCKET, SO_REUSEPORT)\n");
        return 0;
    }
    /* Allow socket to use broadcast */
    if (setsockopt(bcast_fd, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes)) < 0)
    {
        fprintf(stderr, "setsockopt (SOL_SOCKET, SO_BROADCAST)\n");
        return 0;
    }
    /* Set up destination address */
    memset(&dst_addr, 0, sizeof(dst_addr));
    dst_addr.sin_family = AF_INET;
    dst_addr.sin_addr.s_addr = inet_addr(bcast_address);
    dst_addr.sin_port = htons(SMQ_DISC_PORT);
    /* Set up receiver address */
    memset(&rcv_addr, 0, sizeof(rcv_addr));
    rcv_addr.sin_family = AF_INET;
    rcv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    rcv_addr.sin_port = htons(SMQ_DISC_PORT);
    /* Bind to receive address */
    if (bind(bcast_fd, (struct sockaddr *) &rcv_addr, sizeof(rcv_addr)) < 0)
    {
        fprintf(stderr, "Error binding broadcast socket\n");
        return 0;
    }
    /* Add the bcast socket to the zmq poller */
    register_file_descriptor(bcast_fd);
    /* Setup zmq context */
    zmq_context = zmq_ctx_new();
    /* Setup publisher zmq socket */
    zmq_publish_sock = zmq_socket(zmq_context, ZMQ_PUB);
    /* Bind publisher to tcp transport */
    char publish_addr[SMQ_MAX_ADDR_LENGTH];
    sprintf(publish_addr, "tcp://%s:*", ip_address);
    if (0 > zmq_bind(zmq_publish_sock, publish_addr))
    {
        fprintf(stderr, "Error binding zmq socket to tcp\n");
        return 0;
    }
    size_t size_of_tcp_address = sizeof(tcp_address);
    memset(tcp_address, 0, size_of_tcp_address);
    if (0 > zmq_getsockopt(zmq_publish_sock, ZMQ_LAST_ENDPOINT, tcp_address, &size_of_tcp_address))
    {
        fprintf(stderr, "Error getting endpoint address of publisher socket\n");
        return 0;
    }
    /* Bind publisher to inproc transport */
    if (0 > zmq_bind(zmq_publish_sock, SMQ_INPROC_ADDR))
    {
        fprintf(stderr, "Error binding zmq socket to inproc\n");
        return 0;
    }
    /* Setup subscriber socket */
    zmq_subscribe_sock = zmq_socket(zmq_context, ZMQ_SUB);
    zmq_connect(zmq_subscribe_sock, SMQ_INPROC_ADDR);
    register_socket(zmq_subscribe_sock);
    /* Report the state of the node */
    char guid_str[GUID_STR_LEN];
    smq_guid_to_str(GUID, guid_str, GUID_STR_LEN);
    printf("GUID:          %s\n", guid_str);
    printf("IPv4 Address:  %s\n", ip_address);
    printf("Bcast Address: %s\n", bcast_address);
    printf("TCP Endpoint:  %s\n", tcp_address);
    /* Return the handle */
    return 1;
}

static int sendto_bcast(unsigned char* buffer, size_t buffer_len)
{
    return sendto(bcast_fd, buffer, buffer_len, 0, (struct sockaddr *) &dst_addr, sizeof(dst_addr));
}

static size_t serialize_msg_header(uint8_t* buffer, smq_msg_header_t* header)
{
    size_t index = 0;
    memcpy(buffer, &header->version, 2);
    index += 2;
    memcpy(buffer + index, &header->guid, GUID_LEN);
    index += GUID_LEN;
    uint8_t topic_length = strlen(header->topic);
    memcpy(buffer + index, &topic_length, 1);
    index += 1;
    memcpy(buffer + index, header->topic, topic_length);
    index += topic_length;
    memcpy(buffer + index, &header->type, 1);
    index += 1;
    memcpy(buffer + index, header->flags, 16);
    index += 16;
    return index;
}

static size_t deserialize_msg_header(smq_msg_header_t* header, uint8_t* buffer, size_t len)
{
    size_t header_length = 0, available_bytes = len;
    assert (available_bytes > 2);
    memcpy(&header->version, buffer, 2);
    header_length += 2;
    available_bytes -= 2;
    assert (available_bytes > GUID_LEN);
    memcpy(&header->guid, buffer + header_length, GUID_LEN);
    header_length += GUID_LEN;
    available_bytes -= GUID_LEN;
    assert (available_bytes > 1);
    uint8_t topic_len;
    memcpy(&topic_len, buffer + header_length, 1);
    header_length += 1;
    available_bytes -= 1;
    assert (available_bytes > topic_len);
    memcpy(&header->topic, buffer + header_length, topic_len);
    header_length += topic_len;
    available_bytes -= topic_len;
    assert (available_bytes > 1);
    memcpy(&header->type, buffer + header_length, 1);
    header_length += 1;
    available_bytes -= 1;
    assert (available_bytes >= 16);
    memcpy(&header->flags, buffer + header_length, 16);
    header_length += 16;
    header->topic[topic_len] = '\0';
    return header_length;
}

static void smq_print_header(smq_msg_header_t* header)
{
    char guid_str[GUID_STR_LEN];
    smq_guid_to_str(header->guid, guid_str, GUID_STR_LEN);
    printf("---------------------------------\n");
    printf("header.version: 0x%02x\n", header->version);
    printf("header.guid:    %s\n", guid_str);
    printf("header.topic:   %s\n", header->topic);
    printf("header.type:    0x%02x\n", header->type);
    printf("---------------------------------\n");
}

static size_t serialize_adv_msg(uint8_t* buffer, smq_adv_msg_t* adv_msg)
{
    size_t bytes_written = serialize_msg_header(buffer, &adv_msg->header);
    uint16_t addr_len = strlen(adv_msg->addr);
    memcpy(buffer + bytes_written, &addr_len, sizeof(addr_len));
    bytes_written += sizeof(addr_len);
    memcpy(buffer + bytes_written, adv_msg->addr, addr_len);
    bytes_written += addr_len;
    return bytes_written;
}

static size_t deserialize_adv_msg(smq_adv_msg_t* adv_msg, uint8_t* buffer, size_t len)
{
    size_t msg_len = 0, available_bytes = len;
    uint16_t addr_len;
    assert (available_bytes > sizeof(addr_len));
    memcpy(&addr_len, buffer, sizeof(addr_len));
    msg_len += sizeof(addr_len);
    available_bytes -= sizeof(addr_len);
    assert (available_bytes >= addr_len);
    memcpy(&adv_msg->addr, buffer + msg_len, addr_len);
    adv_msg->addr[addr_len] = 0;
    msg_len += addr_len;
    return msg_len;
}

static int send_adv(const char* topic_name)
{
    /* Build an adv_msg.header */
    smq_adv_msg_t adv_msg;
    adv_msg.header.version = 0x01;
    memcpy(adv_msg.header.guid, GUID, GUID_LEN);
    strcpy(adv_msg.header.topic, topic_name);
    adv_msg.header.type = SMQ_OP_ADV;
    memset(adv_msg.header.flags, 0, SMQ_FLAGS_LENGTH);
    /* Copy in adv_msg.addr */
    strcpy(adv_msg.addr, tcp_address);
    uint8_t buffer[SMQ_UDP_MAX_SIZE];
    size_t adv_msg_len = serialize_adv_msg(buffer, &adv_msg);
    if (0 >= sendto_bcast(buffer, adv_msg_len))
    {
        fprintf(stderr, "Error sending ADV message to broadcast\n");
        return 0;
    }
    return 1;
}

static int smq_topic_list_append(smq_topic_list_t* topic_list, const char* topic, smq_msg_callback_t* callback, void* arg)
{
    smq_topic_t* new_topic = (struct smq_topic_t*) malloc(sizeof(struct smq_topic_t));
    if (0 == new_topic)
    {
        fprintf(stderr, "Error appending topic to list\n");
        return 0;
    }
    new_topic->next = 0;
    strncpy(new_topic->name, topic, SMQ_MAX_TOPIC_LENGTH);
    new_topic->callback = callback;
    new_topic->scallback = NULL;
    new_topic->arg = arg;
    if (0 == topic_list->last && 0 == topic_list->first)
    {
        new_topic->prev = 0;
        topic_list->last = new_topic;
        topic_list->first = new_topic;
    }
    else
    {
        new_topic->prev = topic_list->last;
        topic_list->last->next = new_topic;
        topic_list->last = new_topic;
    }
    return 1;
}

static int smq_topic_list_remove(smq_topic_t* topic)
{
    if (topic->next)
    {
        topic->next->prev = topic->prev;
    }
    if (topic->prev)
    {
        topic->prev->next = topic->next;
    }
    free(topic);
    return 1;
}

static smq_topic_t* smq_topic_in_list(smq_topic_list_t* topic_list, const char* topic_name)
{
    smq_topic_t* topic = topic_list->first;
    while (topic != 0)
    {
        if (0 == strcmp(topic->name, topic_name))
        {
            break;
        }
        topic = topic->next;
    }
    return topic;
}

static int smq_topic_list_destroy(smq_topic_list_t* topic_list)
{
    smq_topic_t* topic = topic_list->first;
    while (1)
    {
        if (0 == topic->next)
        {
            free(topic);
            return 1;
        }
        topic = topic->next;
        free(topic->prev);
    }
}

int smq_is_advertised(const char* topic_name)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_is_advertised) smq_init must be called first\n");
        return 0;
    }
    return (smq_topic_in_list(&published_topics, topic_name) != NULL);
}

int smq_advertise(const char* topic_name)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_advertise) smq_init must be called first\n");
        return 0;
    }
    if (smq_topic_in_list(&published_topics, topic_name))
    {
        fprintf(stderr, "Cannot advertise the topic '%s', which has already been advertised\n", topic_name);
        return 0;
    }
    printf("Advertising topic '%s'\n", topic_name);
    /* Add topic to publisher list */
    if (!smq_topic_list_append(&published_topics, topic_name, 0, NULL))
    {
        return 0;
    }
    return send_adv(topic_name);
}

int smq_advertise_hash(const char* topic_name)
{
    char buf[32];
    sprintf(buf, "$crc%04X", smq_string_hash(topic_name));
    return smq_advertise(buf);
}

static int send_sub(const char* topic_name)
{
    /* Build a sub_msg */
    smq_msg_header_t header;
    header.version = 0x01;
    memcpy(header.guid, GUID, GUID_LEN);
    strcpy(header.topic, topic_name);
    header.type = SMQ_OP_SUB;
    memset(header.flags, 0, SMQ_FLAGS_LENGTH);
    uint8_t buffer[SMQ_UDP_MAX_SIZE];
    size_t header_len = serialize_msg_header(buffer, &header);
    if (0 >= sendto_bcast(buffer, header_len))
    {
        fprintf(stderr, "Error sending SUB message to broadcast\n");
        return 0;
    }
    return 1;
}

int smq_is_subscribed(const char* topic_name)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_is_advertised) smq_init must be called first\n");
        return 0;
    }
    return (smq_topic_in_list(&subscribed_topics, topic_name) != NULL);
}

static int smq_is_subscribed_serial(const char* topic_name)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_is_advertised) smq_init must be called first\n");
        return 0;
    }
    smq_topic_t* topic = smq_topic_in_list(&subscribed_topics, topic_name);
    return (topic != NULL)? (topic->scallback != NULL) : 0;
}

int smq_subscribe(const char* topic_name, smq_msg_callback_t* callback, void* arg)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_subscribe) smq_init must be called first\n");
        return 0;
    }
    smq_topic_t* topic = smq_topic_in_list(&subscribed_topics, topic_name);
    if (topic != NULL)
    {
        if (topic->callback != NULL)
        {
            fprintf(stderr, "Cannot subscribe to the topic '%s', which has already been subscribed\n", topic_name);
            return 0;
        }
        topic->callback = callback;
        topic->arg = arg;
        return 1;
    }
    printf("Subscribing to topic '%s'\n", topic_name);
    /* Add topic to subscriber list */
    if (!smq_topic_list_append(&subscribed_topics, topic_name, callback, arg))
    {
        return 0;
    }
    /* Add subscription filter to inproc */
    if (0 != zmq_setsockopt(zmq_subscribe_sock, ZMQ_SUBSCRIBE, topic_name, strlen(topic_name)))
    {
        fprintf(stderr, "Error subscribing to topic '%s'\n", topic_name);
    }
    return send_sub(topic_name);
}

static int smq_subscribe_ser(const char* topic_name, smq_msg_callback_t* callback)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_subscribe_serial) smq_init must be called first\n");
        return 0;
    }
    smq_topic_t* topic = smq_topic_in_list(&subscribed_topics, topic_name);
    if (topic != NULL)
    {
        if (topic->scallback != NULL)
        {
            fprintf(stderr, "Cannot subscribe to the topic '%s', which has already been subscribed\n", topic_name);
            return 0;
        }
        topic->scallback = callback;
        return 1;
    }
    printf("Subscribing to topic '%s'\n", topic_name);
    /* Add topic to subscriber list */
    if (!smq_topic_list_append(&subscribed_topics, topic_name, NULL, NULL))
    {
        return 0;
    }
    topic = smq_topic_in_list(&subscribed_topics, topic_name);
    if (topic != NULL)
        topic->scallback = callback;

    /* Add subscription filter to inproc */
    if (0 != zmq_setsockopt(zmq_subscribe_sock, ZMQ_SUBSCRIBE, topic_name, strlen(topic_name)))
    {
        fprintf(stderr, "Error subscribing to topic '%s'\n", topic_name);
    }
    return send_sub(topic_name);
}

int smq_subscribe_hash(const char* topic_name, smq_msg_callback_t* callback, void* arg)
{
    char buf[32];
    sprintf(buf, "$crc%04X", smq_string_hash(topic_name));
    return smq_subscribe(buf, callback, arg);
}

int smq_publish(const char* topic_name, const uint8_t* msg, size_t len)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_publish) smq_init must be called first\n");
        return 0;
    }
    if (!smq_topic_in_list(&published_topics, topic_name))
    {
        fprintf(stderr, "Cannot publish to topic '%s' which is unadvertised\n", topic_name);
        return 0;
    }
    // printf("smq_publish %s\n", topic_name);

    /* Construct a header for the message */
    smq_msg_header_t header;
    header.version = 0x01;
    memcpy(header.guid, GUID, GUID_LEN);
    strcpy(header.topic, topic_name);
    header.type = SMQ_OP_PUB;
    memset(header.flags, 0, SMQ_FLAGS_LENGTH);
    uint8_t buffer[SMQ_UDP_MAX_SIZE];
    size_t header_len = serialize_msg_header(buffer, &header);
    /* Send the topic as the first part of a three part message */
    zmq_msg_t topic_msg;
    assert(0 == zmq_msg_init_size(&topic_msg, strlen(topic_name)));
    memcpy(zmq_msg_data(&topic_msg), topic_name, strlen(topic_name));
    assert(strlen(topic_name) == zmq_msg_send(&topic_msg, zmq_publish_sock, ZMQ_SNDMORE));
    zmq_msg_close(&topic_msg);
    /* Send the header the next part */
    zmq_msg_t header_msg;
    assert(0 == zmq_msg_init_size(&header_msg, header_len));
    memcpy(zmq_msg_data(&header_msg), buffer, header_len);
    assert(header_len == zmq_msg_send(&header_msg, zmq_publish_sock, ZMQ_SNDMORE));
    zmq_msg_close(&header_msg);
    /* Finally send the data */
    zmq_msg_t data_msg;
    assert(0 == zmq_msg_init_size(&data_msg, len));
    memcpy(zmq_msg_data(&data_msg), msg, len);
    assert(len == zmq_msg_send(&data_msg, zmq_publish_sock, 0));
    zmq_msg_close(&data_msg);
    return 1;
}

int smq_publish_hash(const char* topicName, const uint8_t *msg, size_t len)
{
    char buf[32];
    sprintf(buf, "$crc%04X", smq_string_hash(topicName));
    return smq_publish(buf, msg, len);
}

int smq_timer(smq_timer_callback_t* callback, long timer_period_ms, void* arg)
{
    if (timer_period_ms < 0)
    {
        fprintf(stderr, "Cannot set a timer with period less than 0\n");
        return 0;
    }
    if (callback != 0 && timer_period_ms == 0)
    {
        fprintf(stderr, "Cannot set a timer with period 0\n");
        return 0;
    }
    timer_callback = callback;
    timer_arg = arg;
    timer_period = timer_period_ms;
    smq_get_time_now(&last_timer);
    return 1;
}

int smq_clear_timer()
{
    return smq_timer(0, 0, NULL);
}

static int handle_bcast_msg(uint8_t* buffer, int length)
{
    smq_msg_header_t header;
    size_t header_size = deserialize_msg_header(&header, buffer, length);
    printf("handle_bcast_msg type=%d\n", header.type);
    if (header.type == SMQ_OP_ADV)
    {
        smq_adv_msg_t adv_msg;
        deserialize_adv_msg(&adv_msg, buffer + header_size, length - header_size);
        memcpy(&adv_msg.header, &header, sizeof(header));
        if (smq_guid_compare(GUID, adv_msg.header.guid))
        {
            /* Ignore self messages */
            return 1;
        }
        if (0 == strncmp("tcp://", adv_msg.addr, 6))
        {
            printf("I should connect to tcp address: %s\n", adv_msg.addr);
            if (0 != smq_addr_in_list(&connections, adv_msg.addr))
            {
                printf("Skipping connection to address '%s', because it has already been made\n", adv_msg.addr);
                return 1;
            }
            if (0 != zmq_connect(zmq_subscribe_sock, adv_msg.addr))
            {
                fprintf(stderr, "Error connecting to addr '%s'\n", adv_msg.addr);
                return 0;
            }
            if (!smq_connection_list_append(&connections, -1, adv_msg.addr))
            {
                fprintf(stderr, "Failed to add connection to list\n");
                return 0;
            }
            return 1;
        }
        else
        {
            fprintf(stderr, "Unkown protocol type for address: %s\n", adv_msg.addr);
            return 0;
        }
    }
    else if (header.type == SMQ_OP_SUB)
    {
        printf("header.topic : %s\n", header.topic);
        if (0 != smq_topic_in_list(&published_topics, header.topic))
        {
            /* Resend the ADV message */
            printf("Resending ADV for topic '%s'\n", header.topic);
            return send_adv(header.topic);
        }
        else
        {
            printf("NOT ADVERTISED?\n");
        }
    }
    else
    {
        fprintf(stderr, "Unknown type: %i\n", header.type);
    }
    return 1;
}

int smq_spin_once(long timeout)
{
    if (!init_called)
    {
        fprintf(stderr, "(smq_spin_once) smq_init must be called first\n");
        return 0;
    }
    /* If there is a timer set */
    long time_till_timer = -1;
    if (0 != timer_callback)
    {
        /* Determine the time until the next firing */
        time_till_timer = smq_time_till(&last_timer, timer_period);
        /* If we have no time left, run the callback now */
        if (time_till_timer <= 0)
        {
            smq_get_time_now(&last_timer);
            timer_callback(timer_arg);
            return 1;
        }
    }
    /* Poll for either the given timeout, or the time until the next timer, which ever is shorter */
    long zmq_timeout = (-1 != time_till_timer && time_till_timer < timeout) ? time_till_timer : timeout;
    int rc = zmq_poll(poll_items, poll_items_count, zmq_timeout);
    if (rc < 0)
    {
        switch (errno)
        {
            case EINTR:
                return 1;
            case EFAULT:
                perror("Invalid poll items");
                return 0;
            case ETERM:
                perror("Socket terminated");
                return 0;
            default:
                perror("Unknown error in zmq_poll");
                return 0;
        }
    }
    /* Check for serial messages */
    if (poll_items_count > 1 && poll_items[2].revents & ZMQ_POLLIN)
    {
        smq_process_serial(poll_items[2].fd, 0xFF);
    }

    /* Timeout */
    if (rc == 0)
    {
        /* TODO: check timer again */
        return 1;
    }

    /* Check for incoming broadcast messages */
    if (0 < poll_items_count && poll_items[0].revents & ZMQ_POLLIN)
    {
        uint8_t buffer[SMQ_UDP_MAX_SIZE];
        socklen_t len_rcv_addr = sizeof(rcv_addr);
        int ret = recvfrom(bcast_fd, buffer, SMQ_UDP_MAX_SIZE, 0, (struct sockaddr *) &rcv_addr, &len_rcv_addr);
        if (ret < 0)
        {
            perror("Error in recvfrom on broadcast socket");
            return 0;
        }
        return handle_bcast_msg(buffer, ret);
    }
    /* Check for incoming ZMQ messages */
    if (poll_items[1].revents & ZMQ_POLLIN)
    {
        char topic[SMQ_MAX_TOPIC_LENGTH];
        smq_msg_header_t header;
        /* Get the topic msg */
        zmq_msg_t topic_msg;
        assert(0 == zmq_msg_init(&topic_msg));
        assert(-1 != zmq_msg_recv(&topic_msg, zmq_subscribe_sock, 0));
        memcpy(topic, zmq_msg_data(&topic_msg), zmq_msg_size(&topic_msg));
        topic[zmq_msg_size(&topic_msg)] = 0;
        assert(zmq_msg_more(&topic_msg));
        zmq_msg_close(&topic_msg);
        /* Get the header msg */
        zmq_msg_t header_msg;
        assert(0 == zmq_msg_init(&header_msg));
        assert(-1 != zmq_msg_recv(&header_msg, zmq_subscribe_sock, 0));
        deserialize_msg_header(&header, (uint8_t *) zmq_msg_data(&header_msg), zmq_msg_size(&header_msg));
        int more = zmq_msg_more(&header_msg);
        zmq_msg_close(&header_msg);
        printf("header.type = %d\n", header.type);
        if (header.type == SMQ_OP_PUB)
        {
            /* Find subscriber */
            smq_topic_t* subscriber = smq_topic_in_list(&subscribed_topics, topic);
            if (!subscriber)
            {
                fprintf(stderr, "Could not find subscriber for topic '%s'\n", topic);
                return 0;
            }
            /* Receive final data msg */
            assert(more);
            zmq_msg_t data_msg;
            assert(0 == zmq_msg_init(&data_msg));
            assert(-1 != zmq_msg_recv(&data_msg, zmq_subscribe_sock, 0));
            size_t data_len = zmq_msg_size(&data_msg);
            uint8_t* data = (uint8_t *) zmq_msg_data(&data_msg);
            if (subscriber->scallback != NULL)
                subscriber->scallback(topic, data, data_len, subscriber->arg);
            if (subscriber->callback != NULL)
                subscriber->callback(topic, data, data_len, subscriber->arg);
        }
        return 1;
    }
    return 1;
}

int smq_spin()
{
    int ret = 0;
    while (0 < (ret = smq_spin_once(10))) {}
    return ret;
}

// ----------------------------------------------

static inline uint16_t update_crc(uint16_t crc, const uint8_t data)
{
    // crc-16 poly 0x8005 (x^16 + x^15 + x^2 + 1)
    static const uint16_t crc16_table[256] =
    {
        0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241, 0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
        0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40, 0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
        0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40, 0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
        0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641, 0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
        0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240, 0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
        0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41, 0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
        0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41, 0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
        0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640, 0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
        0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240, 0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
        0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41, 0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
        0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41, 0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
        0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640, 0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
        0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241, 0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
        0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40, 0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
        0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40, 0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
        0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641, 0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040
    };
    return (crc >> 8) ^ crc16_table[(crc ^ data) & 0xFF];
}

uint16_t smq_calc_crc(const void* buf, size_t len, uint16_t crc)
{
    const uint8_t* b = (uint8_t*)buf;
    const uint8_t* buf_end = (uint8_t*)buf + len;
    while (b < buf_end)
    {
        crc = update_crc(crc, *b++);
    }
    return crc;
}

uint16_t smq_string_hash(const char* str)
{
    return ~smq_calc_crc(str, strlen(str), ~0);
}

// ------------------------------------------------------

static void smq_send_raw_bytes(int fd, const void* buf, size_t len)
{
    write(fd, buf, len);
#if 0
    printf("==>%d:[0x", len);
    for (int i = 0; i < len; i++)
    {
        printf("%02X ", ((uint8_t*)buf)[i]);
    }
    printf("]\n");
#endif
}

static void smq_send_data(int fd, const void* buf, uint16_t len)
{
    uint16_t crc = 0;
    const uint8_t* b = (uint8_t*)buf;
    const uint8_t* buf_end = (uint8_t*)buf + len;
    while (b < buf_end)
    {
        crc = update_crc(crc, *b++);
    }
    smq_send_raw_bytes(fd, &crc, sizeof(crc));
    smq_send_raw_bytes(fd, buf, len);
}

void smq_send_string(int fd, const char* str)
{
    uint8_t delim = 0x00;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));

    uint16_t len = strlen(str);
    smq_send_data(fd, &len, sizeof(len));
    smq_send_data(fd, str, len);
}

void smq_send_string_hash(int fd, const char* str)
{
    uint8_t delim = 0x01;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));

    uint16_t crc = 0xFFFF;
    size_t len = strlen(str);
    const uint8_t* b = (uint8_t*)str;
    const uint8_t* buf_end = (uint8_t*)str + len;
    while (b < buf_end)
    {
        crc = update_crc(crc, *b++);
    }
    crc = ~crc;
    smq_send_raw_bytes(fd, &crc, sizeof(crc));
}

void smq_send_int8(int fd, int8_t val)
{
    uint8_t delim = 0x02;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &val, sizeof(val));
}

void smq_send_int16(int fd, int16_t val)
{
    uint8_t delim = 0x03;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &val, sizeof(val));
}

void smq_send_int32(int fd, int32_t val)
{
    uint8_t delim = 0x04;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &val, sizeof(val));
}

void smq_send_uint8(int fd, uint8_t val)
{
    uint8_t delim = 0x05;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &val, sizeof(val));
}

void smq_send_uint16(int fd, uint16_t val)
{
    uint8_t delim = 0x06;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &val, sizeof(val));
}

void smq_send_uint32(int fd, uint32_t val)
{
    uint8_t delim = 0x07;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &val, sizeof(val));
}

void smq_send_float(int fd, float val)
{
    uint8_t delim = 0x08;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &val, sizeof(val));
}

void smq_send_boolean(int fd, char val)
{
    uint8_t delim = (val) ? 0x0A : 0x0B;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
}

void smq_send_null(int fd)
{
    uint8_t delim = 0x0C;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
}

void smq_send_buffer(int fd, const void* buf, uint16_t len)
{
    uint8_t delim = 0x0D;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
    smq_send_data(fd, &len, sizeof(len));
    smq_send_data(fd, buf, len);
}

void smq_end(int fd)
{
    uint8_t delim = 0xFF;
    smq_send_raw_bytes(fd, &delim, sizeof(delim));
}

// ------------------------------------------------------

static int smsg_read(int fd, void* buf, size_t len)
{
    char* b = (char*)buf;
    char* b_end = b + len;
    while (b < b_end)
    {
        int cnt = b_end - b;
        int n = read(fd, b, cnt);
        if (n < 0)
        {
            cnt = b - (char*)buf;
            return (cnt) ? cnt : -1;
        }
        b = b + n;
    }
    int cnt = b - (char*)buf;
#if 1
    if (cnt > 0)
    {
        printf("<==%d:[0x", cnt);
        for (int i = 0; i < cnt; i++)
        {
            printf("%02X %c", ((uint8_t*)buf)[i], ((uint8_t*)buf)[i]);
        }
        printf("]\n");
    }
#endif
    return cnt;
}

static int strcmp_hash(const char* topicNameHash, const char* topicName)
{
    char buf[32];
    sprintf(buf, "$crc%04X", smq_string_hash(topicName));
    return strcmp(topicNameHash, buf);
}

static const char* sDataType[] =
{
    "STRING",
    "HASH",
    "INT8",
    "INT16",
    "INT32",
    "UINT8",
    "UINT16",
    "UINT32",
    "FLOAT",
    "BUFFER"
};

#if 0
#define REPORT_TYPE(b) \
{   if (b < sizeof(sDataType)/sizeof(sDataType[0])) printf("[%s]\n", sDataType[b]); }
#else
#define REPORT_TYPE(b)
#endif
#define REPORT_MEM_ERROR() \
{   printf("ERROR : %d\n", __LINE__); \
    exit(1); \
    return -1; }
#define REPORT_READ_ERROR() \
{   printf("ERROR : %d\n", __LINE__); \
    exit(1); \
    return -1; }
#define REPORT_BAD_CRC(crc, recrc) \
{   printf("CRC BAD GOT 0x%04X EXPECTED 0x%04X @%d\n", crc, recrc, __LINE__); \
    exit(1);\
    return -1; }\

static int smsg_callback_fd;
static void smsg_callback(const char * topic_name, const uint8_t * msg, size_t len, void* arg);

int smq_process_serial(int fd, uint8_t id)
{
    uint8_t buffer[8];
    char* jkey = NULL;
    char* topicName = NULL;
    json_object* jobj = NULL;
    //printf("============smq_process_serial\n");
    buffer[0] = id;
    char needACK = (id == 0xFF); 
    for (;;)
    {
        if (id == 0xFF)
            smsg_read(fd, buffer, 1);
        id = 0xFF;
        REPORT_TYPE(buffer[0]);
        printf("[0x%02X]\n", buffer[0]);
        switch (buffer[0])
        {
            case 0x00:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                printf("    CRC : 0x%04X\n", crc);
                uint16_t len;
                if (smsg_read(fd, &len, sizeof(len)) != sizeof(len))
                    REPORT_READ_ERROR();
                printf("    LEN : %d\n", len);
                uint16_t recrc = smq_calc_crc(&len, sizeof(len), 0);
                if (crc == recrc)
                {
                    if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                        REPORT_READ_ERROR();

                    printf("    CRC : 0x%04X\n", crc);
                    char* buffer = (char*)malloc(len+1);
                    if (buffer == NULL)
                        REPORT_MEM_ERROR();
                    ssize_t readlen = smsg_read(fd, buffer, len);
                    if (readlen != len)
                    {
                        printf("read : %d wanted : %d\n", (int)readlen, (int)len);
                        REPORT_READ_ERROR();
                    }
                    buffer[len] = '\0';
                    recrc = smq_calc_crc(buffer, len, 0);
                    if (crc == recrc)
                    {
                        // printf("[STRING] %s\n", buffer);
                        if (jobj == NULL)
                        {
                            topicName = buffer;
                            jobj = json_object_new_object();
                        }
                        else if (jkey == NULL)
                        {
                            jkey = buffer;
                        }
                        else
                        {
                            json_object_object_add(jobj, jkey, json_object_new_string(buffer));
                            free(buffer);
                            free(jkey);
                            jkey = NULL;
                        }
                    }
                    else
                    {
                        free(buffer);
                        REPORT_BAD_CRC(crc, recrc);
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x01:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                printf("[CRC_STRING] : 0x%04X\n", crc);
                char buf[32];
                sprintf(buf, "$crc%04X", crc);
                if (strcmp_hash(buf, "subscribers") == 0)
                {
                    uint16_t count;
                    if (smsg_read(fd, &count, sizeof(count)) != sizeof(count))
                        REPORT_READ_ERROR();
                    printf("count : %d\n", count);
                    for (unsigned i = 0; i < count; i++)
                    {
                        uint16_t crcsub;
                        if (smsg_read(fd, &crcsub, sizeof(crcsub)) != sizeof(crcsub))
                            REPORT_READ_ERROR();
                        sprintf(buf, "$crc%04X", crcsub);
                        if (!smq_is_subscribed_serial(buf))
                        {
                            smsg_callback_fd = fd;
                            if (!smq_subscribe_ser(buf, smsg_callback))
                            {
                                printf("FAILED TO SUBSCRIBE %s\n", buf);
                            }
                            else
                            {
                                printf("subscribe %s\n", buf);
                            }
                        }
                    }
                }
                else if (jobj == NULL)
                {
                    topicName = strdup((const char*)buffer);
                    jobj = json_object_new_object();
                }
                else if (jkey == NULL)
                {
                    jkey = strdup((const char*)buffer);
                }
                else
                {
                    json_object_object_add(jobj, jkey, json_object_new_string((const char*)buffer));
                    free(jkey);
                    jkey = NULL;
                }
                break;
            }
            case 0x02:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                int8_t val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_int(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x03:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                int16_t val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_int(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x04:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                int32_t val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_int(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x05:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                uint8_t val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_int(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x06:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                uint16_t val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_int(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x07:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                uint32_t val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_int(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x08:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                float val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_double(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x09:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                double val;
                if (smsg_read(fd, &val, sizeof(val)) != sizeof(val))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&val, sizeof(val), 0);
                if (crc == recrc)
                {
                    if (jkey != NULL)
                    {
                        json_object_object_add(jobj, jkey, json_object_new_double(val));
                        free(jkey);
                        jkey = NULL;
                    }
                    else
                    {
                        // INVALID
                    }
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0x0A:
            {
                if (jkey != NULL)
                {
                    json_object_object_add(jobj, jkey, json_object_new_boolean(1));
                    free(jkey);
                    jkey = NULL;
                }
                else
                {
                    // INVALID
                }
                break;
            }
            case 0x0B:
            {
                if (jkey != NULL)
                {
                    json_object_object_add(jobj, jkey, json_object_new_boolean(0));
                    free(jkey);
                    jkey = NULL;
                }
                else
                {
                    // INVALID
                }
                break;
            }
            case 0x0C:
            {
                if (jkey != NULL)
                {
                    json_object_object_add(jobj, jkey, NULL);
                    free(jkey);
                    jkey = NULL;
                }
                else
                {
                    // INVALID
                }
                break;
            }
            case 0x0D:
            {
                uint16_t crc;
                if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                    REPORT_READ_ERROR();
                uint16_t len;
                if (smsg_read(fd, &len, sizeof(len)) != sizeof(len))
                    REPORT_READ_ERROR();
                uint16_t recrc = smq_calc_crc(&len, sizeof(len), 0);
                if (crc == recrc)
                {
                    if (smsg_read(fd, &crc, sizeof(crc)) != sizeof(crc))
                        REPORT_READ_ERROR();

                    char* buffer = (char*)malloc(len);
                    if (buffer == NULL)
                        REPORT_MEM_ERROR();
                    if (smsg_read(fd, buffer, len) != len)
                        REPORT_READ_ERROR();
                    recrc = smq_calc_crc(buffer, len, 0);
                    if (crc == recrc)
                    {
                        if (jkey != NULL)
                        {
                            json_object* jarr = json_object_new_array();
                            for (unsigned i = 0; i < len; i++)
                            {
                                json_object_array_add(jarr, json_object_new_int(buffer[i]));
                            }
                            json_object_object_add(jobj, jkey, jarr);
                            free(jkey);
                            jkey = NULL;
                        }
                        else
                        {
                            // INVALID
                        }
                    }
                    else
                    {
                        free(buffer);
                        REPORT_BAD_CRC(crc, recrc);
                    }
                    free(buffer);
                }
                else
                {
                    REPORT_BAD_CRC(crc, recrc);
                }
                break;
            }
            case 0xDB:
            {
                uint8_t ch;
                if (smsg_read(fd, &ch, sizeof(ch)) != sizeof(ch))
                    REPORT_READ_ERROR();
                printf("%c", ch);
                break;
            }
            case 0xDD:
            {
                printf("HMM\n");
                uint8_t len;
                if (smsg_read(fd, &len, sizeof(len)) != sizeof(len))
                    REPORT_READ_ERROR();
                printf("len : %d\n", len);
                char* buffer = (char*)malloc(len+1);
                if (buffer == NULL)
                    REPORT_MEM_ERROR();
                if (smsg_read(fd, buffer, len) != len)
                    REPORT_READ_ERROR();
                buffer[len] = '\0';
                printf("%s", buffer);
                free(buffer);
                break;
            }
            case 0xDE:
            {
                printf("HMM2\n");
                uint8_t lenhi;
                uint8_t lenlo;
                if (smsg_read(fd, &lenhi, sizeof(lenhi)) != sizeof(lenhi))
                    REPORT_READ_ERROR();
                if (smsg_read(fd, &lenlo, sizeof(lenlo)) != sizeof(lenlo))
                    REPORT_READ_ERROR();
                uint16_t len = (lenhi << 8) | lenlo;
                printf("len : %d\n", len);
                char* buffer = (char*)malloc(len+1);
                if (buffer == NULL)
                    REPORT_MEM_ERROR();
                if (smsg_read(fd, buffer, len) != len)
                    REPORT_READ_ERROR();
                buffer[len] = '\0';
                printf("%s", buffer);
                free(buffer);
                break;
            }
            case 0xFF:
            {
                if (jobj != NULL)
                {
                    if (!smq_is_advertised(topicName))
                    {
                        if (!smq_advertise(topicName))
                        {
                            printf("FAILED TO ADVERTISE %s\n", topicName);
                        }
                        else
                        {
                            printf("advertise %s\n", topicName);
                        }
                    }
                    if (smq_is_advertised(topicName))
                    {
                        const char* msg = json_object_to_json_string(jobj);
                        smq_publish(topicName, (const uint8_t*)msg, strlen(msg));
                    }
                    json_object_put(jobj);
                    free(topicName);
                    topicName = NULL;
                    jobj = NULL;
                }
                if (needACK)
                {
                    char ready = 'A';
                    if (write(fd, &ready, 1) != 1)
                    {
                        printf("FAIL\n");
                    }
                    // static uint32_t sCount;
                    // printf("============smq_process_serial DONE ACK=%d\n", sCount);
                    // sCount++;
                }
                return 0;
            }
        }
    }
}

static void smsg_callback(const char * topic_name, const uint8_t * msg, size_t len, void* arg)
{
    int fd = smsg_callback_fd;
    uint16_t crc;
    printf("======callback : topic_name=%s\n", topic_name);
    if (strncmp(topic_name, "$crc", 4) == 0)
    {
        int crcint;
        sscanf(topic_name+4, "%x", &crcint);
        crc = (uint16_t)crcint;
    }
    else
    {
        crc = smq_string_hash(topic_name);
    }
    printf("msg : %s\n", msg);
    json_tokener* tok = json_tokener_new();
    json_object* jobj = json_tokener_parse_ex(tok, (const char*)msg, len);
    if (jobj != NULL)
    {
        char delim = 'D';
        smq_send_raw_bytes(fd, &delim, 1);
        for (;;)
        {
            int cnt = smsg_read(fd, &delim, 1);
            // printf("smsg_read ");
            if (delim == 'R')
            {
                printf("send CRC : 0x%04X\n", crc);
                smq_send_raw_bytes(fd, &crc, sizeof(crc));

                json_object_object_foreach(jobj, key, val)
                {
                    int val_type = json_object_get_type(val);
                    switch (val_type)
                    {
                        case json_type_null:
                            smq_send_string_hash(fd, key);
                            smq_send_null(fd);
                            break;
                        case json_type_boolean:
                            smq_send_string_hash(fd, key);
                            smq_send_boolean(fd, json_object_get_boolean(val));
                            break;
                        case json_type_double:
                            smq_send_string_hash(fd, key);
                            smq_send_float(fd, json_object_get_double(val));
                            break;
                        case json_type_int:
                            smq_send_string_hash(fd, key);
                            smq_send_int32(fd, json_object_get_int(val));
                            break;
                        case json_type_string:
                            smq_send_string_hash(fd, key);
                            smq_send_string(fd, json_object_get_string(val));
                            break;
                        case json_type_object:
                            break;
                        case json_type_array:
                            // Support int array as byte-buffer
                            break;
                    }
                }
                printf("============== smq_end1\n");
                smq_end(fd);
                printf("============== smq_end2\n");
                char ready = 'A';
                if (write(fd, &ready, 1) != 1)
                {
                    printf("FAIL\n");
                }
                printf("============== END\n");
                break;
            }
            else if (delim == 0x00 || delim == 0x01)
            {
                smq_process_serial(fd, delim);
            }
        }
    }
    json_tokener_free(tok);
    printf("======callback DONE\n");
}

/// -----------------------------------------

int smq_reset_serial(int fd)
{
    for (int is_on = 0; is_on < 2; is_on++)
    {
        unsigned int ctl;
        if (ioctl(fd, TIOCMGET, &ctl) == 0)
        {
            if (is_on)
            {
                /* Set DTR and RTS */
                ctl |= (TIOCM_DTR | TIOCM_RTS);
            }
            else
            {
                /* Clear DTR and RTS */
                ctl &= ~(TIOCM_DTR | TIOCM_RTS);
            }
            if (ioctl(fd, TIOCMSET, &ctl) != 0)
                printf("ioctl1 failed\n");
              usleep(50*1000);
        }
        else
        {
            printf("ioctl failed\n");
        }
    }
    sleep(1);
    struct timeval timeout;
    fd_set rfds;
    int nfds;
    int rc;
    unsigned char buf;

    timeout.tv_sec = 0;
    timeout.tv_usec = 250000;

    while (1)
    {
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);

    reselect:
        nfds = select(fd + 1, &rfds, NULL, NULL, &timeout);
        if (nfds == 0)
        {
            break;
        }
        else if (nfds == -1)
        {
            if (errno == EINTR)
            {
                goto reselect;
            }
            else
            {
                return 1;
            }
        }
        rc = read(fd, &buf, 1);
        if (rc < 0)
        {
            return 1;
        }
    }
    return 0;
}

static speed_t serial_baud_lookup(long baud)
{
    struct baud_mapping
    {
        long baud;
        speed_t speed;
    };
    static struct baud_mapping baud_lookup_table [] =
    {
        { 1200,   B1200 },
        { 2400,   B2400 },
        { 4800,   B4800 },
        { 9600,   B9600 },
        { 19200,  B19200 },
        { 38400,  B38400 },
        { 57600,  B57600 },
        { 115200, B115200 },
        { 230400, B230400 },
        { 0,      0 }                 /* Terminator. */
    };
    for (struct baud_mapping *map = baud_lookup_table; map->baud; map++)
    {
        if (map->baud == baud)
            return map->speed;
    }
    return baud;
}

int smq_open_serial(const char* serial_port, unsigned baud)
{
    int fd;
    int rc;
    struct termios termios;
    const char* sport = (serial_port != NULL) ? serial_port : "/dev/ttyUSB0";
    speed_t speed = serial_baud_lookup((baud != 0) ? baud : 115200);
    if (speed == 0)
    {
        // Non-standard speed
    }

#ifdef __APPLE__
    fd = open(sport, O_RDWR | O_NOCTTY | O_NONBLOCK);
#else
    fd = open(sport, O_RDWR | O_NOCTTY);
#endif
    if (fd < 0)
        return -1;

    /* Make sure device is of tty type */
    if (!isatty(fd))
    {   
        close(fd);
        return -1;
    }
    /* Lock device file */
    if ((flock(fd, LOCK_EX | LOCK_NB) == -1) && (errno == EWOULDBLOCK))
    {
        close(fd);
        return -1;
    }

    rc = tcgetattr(fd, &termios);
    if (rc < 0)
    {
        close(fd);
        return -1;
    }

    termios.c_iflag = IGNBRK;
    termios.c_oflag = 0;
    termios.c_lflag = 0;
    termios.c_cflag = (CS8 | CREAD | CLOCAL);
    termios.c_cc[VMIN]  = 1;
    termios.c_cc[VTIME] = 0;

    rc = cfsetospeed(&termios, speed);
    if (rc == -1)
    {
        close(fd);
        return -1;
    }
    rc = cfsetispeed(&termios, speed);
    if (rc == -1)
    {
        close(fd);
        return -1;
    }
    // 8 data bits
    termios.c_cflag &= ~CSIZE;
    termios.c_cflag |= CS8;

    // No flow control
    termios.c_cflag &= ~CRTSCTS;
    termios.c_iflag &= ~(IXON | IXOFF | IXANY);

    // 1 stop bit
    termios.c_cflag &= ~CSTOPB;

    // No parity
    termios.c_cflag &= ~PARENB;

    /* Control, input, output, local modes for tty device */
    termios.c_cflag |= CLOCAL | CREAD;
    termios.c_oflag = 0;
    termios.c_lflag = 0;

    /* Control characters */
    termios.c_cc[VTIME] = 0; // Inter-character timer unused
    termios.c_cc[VMIN]  = 1; // Blocking read until 1 character received

    rc = tcsetattr(fd, TCSANOW, &termios);
    if (rc == -1)
    {
        close(fd);
        return -1;
    }
    return fd;
}

int smq_subscribe_serial(const char* serial_port, unsigned baud)
{
    int fd = smq_open_serial(serial_port, baud);
    if (fd == -1)
    {
        fprintf(stderr, "Failed to initialize SMQ serial port : %s\n", serial_port);
        return -1;
    }
    smq_reset_serial(fd);
    sleep(1);

    register_file_descriptor(fd);
    char ready = 'A';
    if (write(fd, &ready, 1) == 1)
    {
        printf("Ready\n");
    }
    sleep(1);
    return fd;
}

int smq_close_serial(int fd)
{
    return (fd != -1) ? close(fd) : -1;
}

int smq_unsubscribe_serial(int fd)
{
    return smq_close_serial(fd);
}
