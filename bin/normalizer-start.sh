#!/usr/bin/env bash

if [ $# -lt 1 ];
then
        echo "USAGE: $0 config.json"
        exit 1
fi

CURRENT=`pwd` && cd `dirname $0` && SOURCE=`pwd` && cd ${CURRENT} && PARENT=`dirname ${SOURCE}`

for file in ${PARENT}/lib/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

java -cp ${CLASSPATH} rb.ks.Normalizer $1