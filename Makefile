AR ?= ar
CC ?= gcc
CFLAGS := $(CFLAGS) -g
LIBRARIES = -lsmq -lzmq -luuid -ljson-c

all:
	mkdir -p bin lib
	$(CC) -c $(CFLAGS) src/smq.c -o src/smq.o
	$(AR) rcs lib/libsmq.a src/smq.o
	$(CC) src/smq_agent.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_agent
	$(CC) src/smq_listener.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_listener
	$(CC) src/smq_publish.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_publish
	$(CC) src/smq_marcduino.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_marcduino
	$(CC) src/smq_serial_relay.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_serial_relay

clean:
	rm -rf bin lib src/*.o
