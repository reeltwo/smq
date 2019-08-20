#!/bin/sh
./bin/smq_publish LD "{ \"state\": $1 }"
