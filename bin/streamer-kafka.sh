#!/usr/bin/env bash

for file in ${NORMALIZER_HOME}/lib/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

if [ $# -eq 2 ]; then
  java -cp ${CLASSPATH} rb.ks.utils.StreamerKafkaConfig $0 $1
elif [ $# -eq 3 ]; then
  java -cp ${CLASSPATH} rb.ks.utils.StreamerKafkaConfig $0 $1 $2
else
  echo -n 'streamer-kafka.sh <bootstrap_kafka_server> <app_id> [stream_config_path]'
fi