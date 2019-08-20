AR = ar
CC = gcc
CFLAGS = -g
LIBRARIES = -lzmq -luuid -ljson-c -lsmq

all:
	mkdir -p bin lib
	${CC} -c ${CFLAGS} src/smq.c -o src/smq.o
	${AR} r lib/libsmq.a src/smq.o
	${CC} -o bin/smq_agent src/smq_agent.c ${CFLAGS} -Llib ${LIBRARIES}
	${CC} -o bin/smq_listener src/smq_listener.c ${CFLAGS} -Llib ${LIBRARIES}
	${CC} -o bin/smq_publish src/smq_publish.c ${CFLAGS} -Llib ${LIBRARIES}
	${CC} -o bin/smq_marcduino src/smq_marcduino.c ${CFLAGS} -Llib ${LIBRARIES}

clean:
	rm -rf bin
