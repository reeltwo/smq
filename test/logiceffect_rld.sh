#!/bin/sh
./bin/smq_publish RLD "{ \"state\": $1 }"
