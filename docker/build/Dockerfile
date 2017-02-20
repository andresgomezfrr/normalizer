FROM java:openjdk-8-jre

MAINTAINER agomez@wizzie.io

RUN apt-get update
RUN apt-get install -y gettext-base

RUN mkdir -p /var/log/normalizer

ADD normalizer-dist.tar.gz /opt/
RUN rm -f /opt/normalizer/config/*

ADD log4j.xml /opt/normalizer/config/
ADD config_env.json /opt/normalizer/config/
ADD normalizer-start.sh /bin

ENV METRIC_ENABLE true
ENV METRIC_INTERVAL 60000
ENV MULTI_ID false
ENV NUM_STREAM_THREADS 1
ENV MAX_OUTPUT_KAFKA_TOPICS 500

ENV JVM_OPTIONS -Xmx512m -Xms512m

CMD /bin/normalizer-start.sh


