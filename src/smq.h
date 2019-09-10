#ifndef SMQ_CLIENT_H
#define SMQ_CLIENT_H
#include <stdio.h>
#include <string.h>
#include <setjmp.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef signed char int8_t;
typedef signed short int16_t;
typedef signed int int32_t;
#if __LP64__
#ifndef _INT64_T
typedef signed long int64_t;
#endif
#ifndef _UINT64_T
typedef unsigned long uint64_t;
#endif
#else
#ifndef _INT64_T
typedef signed long long int64_t;
#endif
#ifndef _UINT64_T
typedef unsigned long long uint64_t;
#endif
#endif
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned int uint32_t;

typedef struct smsg_t
{
    const uint8_t* msg;
    const uint8_t* msgend;
    size_t len;
    jmp_buf jmp;
} smsg_t;

// --------------------------------------------------

typedef void (smq_msg_callback_t)(const char* topic_name, const uint8_t* msg, size_t len, void* arg);
typedef void (smq_timer_callback_t)(void* arg);

// --------------------------------------------------
// smsg - serial message: messages to and from serial

void smsg_failed(smsg_t* smsg);

void smsg_complete(smsg_t* smsg);

int8_t smsg_read_int8(smsg_t* smsg);

int16_t smsg_read_int16(smsg_t* smsg);

int32_t smsg_read_int32(smsg_t* smsg);

uint8_t smsg_read_uint8(smsg_t* smsg);

uint16_t smsg_read_uint16(smsg_t* smsg);

uint32_t smsg_read_uint32(smsg_t* smsg);

// ----------------------------------------

uint16_t smq_calc_crc(const void* buf, size_t len, uint16_t crc);

uint16_t smq_string_hash(const char* str);

uint64_t smq_current_time();

// ----------------------------------------

int smq_init();

int smq_is_advertised(const char* topic_name);

int smq_is_subscribed(const char* topic_name);

int smq_advertise(const char* topic_name);

int smq_advertise_hash(const char* topic_name);

int smq_subscribe(const char* topic_name, smq_msg_callback_t* callback, void* arg);

int smq_subscribe_hash(const char* topic_name, smq_msg_callback_t* callback, void* arg);

int smq_publish(const char* topic_name, const uint8_t * msg, size_t len);

int smq_publish_hash(const char* topicName, const uint8_t *msg, size_t len);

int smq_timer(smq_timer_callback_t* callback, long period_ms, void* arg);

int smq_clear_timer();

int smq_spin_once(long timeout_ms);

int smq_spin();

// ----------------------------------------

int smq_open_serial(const char* serial_port, unsigned speed);

int smq_close_serial(int fd);

int smq_reset_serial(int fd);

int smq_subscribe_serial(const char* serial_port, unsigned speed);

int smq_process_serial(int fd, uint8_t);

int smq_unsubscribe_serial(int fd);

// ----------------------------------------

#ifdef __cplusplus
}
#endif

#endif
