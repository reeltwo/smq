#!/bin/sh
./bin/smq_publish ServoDispatch "{ \"num\": $1, \"startDelay\": $2, \"moveTime\": $3, \"startPos\": $4, \"endPos\": $5, \"relPos\": $6 }"
