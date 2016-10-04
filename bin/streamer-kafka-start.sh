#!/usr/bin/env bash

if [ $# -lt 1 ];
then
        echo "USAGE: $0 config.json"
        exit 1
fi

CURRENT=`pwd` && cd `dirname $0` && SOURCE=`pwd` && cd ${CURRENT} && PARENT=`dirname ${SOURCE}`

exec $SOURCE/streamer-kafka.sh localhost:9092 myapp $1