#!/bin/sh
./bin/smq_publish FLD "{ \"state\": $1 }"
