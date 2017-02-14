---
layout: page
title: "Listeners"
category: metrics
date: 2017-02-14 13:21:52
order: 2
---

The listeners are the process that listen the reported metrics and do something with them. You can have multiple listeners at the same time. Currently, the Normalizer service supports two listener metrics:

#### ConsoleMetricListener

This listener `io.wizzie.ks.metrics.ConsoleMetricListener` send the transform the metrics to JSON and prints them into the log file using the log4j. The metric format is:
```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345}
```

#### KafkaMetricListener
This listener `io.wizzie.ks.normalizer.metrics.KafkaMetricListener` send the transform the metrics to JSON and sends them into the Kafka topic. The metric format is:
```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345, "app_id":"MY_KAFKA_STREAMS_APP_ID"}
```

This listener adds a new property to specify the metrics Kafka topic `metric.kafka.topic`, by default is `__normalizer_metrics`

#### Custom Listeners
You can made new listeners to do this you need to implement the [MetricListener Class](https://github.com/wizzie-io/normalizer/blob/master/service/src/main/java/io/wizzie/ks/normalizer/metrics/MetricListener.java). On this class you receive the metric on the method `void updateMetric(String metricName, Object metricValue);`
