#!/bin/sh
./bin/smq_listener MARC | grep --line-buffered '\$crc32C4:{ "cmd":' | awk -F "\"" '{print $4}'
