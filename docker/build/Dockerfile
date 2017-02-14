FROM java:openjdk-8-jre

MAINTAINER agomez@wizzie.io

RUN apt-get update
RUN apt-get install -y gettext-base

RUN mkdir -p /alloc/var/ /alloc/log/ks-normalizer /app

ADD normalizer-dist.tar.gz /alloc/var/
RUN rm -f /alloc/var/normalizer/config/*

ADD log4j.xml /alloc/var/normalizer/config/
ADD config_env.json /alloc/var/normalizer/config/
ADD normalizer-start.sh /app

CMD /app/normalizer-start.sh


