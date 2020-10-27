AR ?= ar
CC ?= gcc
CFLAGS := $(CFLAGS) -g
LIBRARIES = -lsmq -lzmq -luuid -ljson-c

lib:
	mkdir -p lib
	$(CC) -c $(CFLAGS) src/smq.c -o src/smq.o
	$(AR) rcs lib/libsmq.a src/smq.o

example: lib
	$(CC) src/smq_agent.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_agent
	$(CC) src/smq_listener.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_listener
	$(CC) src/smq_publish.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_publish
	$(CC) src/smq_marcduino.c $(CFLAGS) -Llib $(LIBRARIES) -o bin/smq_marcduino

clean:
	rm -rf bin
