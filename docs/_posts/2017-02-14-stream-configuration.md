---
layout: page
title: "Stream Configuration"
category: conf
date: 2017-02-14 12:25:04
order: 2
---

The stream configuration is the execution plan of the normalizer. Normalizer uses the stream configuration to build the Kafka Streams topology using DSL API. You can configure how the normalizer gets the stream conf to do it you need to use the [Bootstrappers](https://github.com/wizzie-io/normalizer/wiki/Bootstrapper).

Stream configuration has two main sections:

### inputs

This section is used to define the mapper between streams and Kafka topic.

```json
{
  "inputs": {
    "topic1": [
      "stream1",
      "stream2"
    ],
    "topic2": [
      "stream3"
    ]
  }
}
```

This example defines a three streams `stream1`and `stream2` is reading Kafka topic `topic1`, and `stream3` that is reading Kafka topic `topic2`. Later, you can use the defined streams inside the stream section.

### streams

The `streams` section is made by streams (json objects):

```json
{
  "streams":{
    "stream1":{
        "funcs":[
              {
                "name":"myMapper",
                "className":"io.wizzie.ks.normalizer.funcs.impl.SimpleMapper",
                "properties": {
                  "maps": [
                    {"dimPath":["timestamp"]}
                  ]
                }
              }
        ],
        "sinks":[
            {"topic":"output1", "type": "kafka"}
        ]
    }
  }
}
```

The streams objects are made by a key and a body. The key is the stream's name that you define previously on `input` section and the body is made by `funcs` and `sinks`.

#### funcs
The `funcs` is a JSON Array where you define different [functions](https://github.com/wizzie-io/normalizer/wiki/Functions) to transforms the stream. The **transformations are sequential** so the order when you are defining the functions is so important!!

#### sinks
The `funcs` is a JSON Array where you define sinks. You can define two types of sinks:

* `kafka` : kafka sink is used to send the data into kafka topic.
* `stream` :  stream sink is used to create a new stream that you can use later on the `streams` section.

You can indicate what sink type do you want to use using the field `type`, by default the type is `kafka`.

Sinks also have the option to configure a filter that is apply to the stream before send the messages through the sink, to define a filter you need to use the field `filter` and use some [filter function](https://github.com/wizzie-io/normalizer/wiki/Functions#filter-functions).

```json
      "sinks": [
        {"topic": "mapper-stream", "type": "stream", "filter": {
        "name": "myFilter",
        "className": "io.wizzie.ks.normalizer.funcs.impl.ContainsDimensionFilter",
        "properties": {
          "dimensions": ["A"]
        }
      }},
        {"topic": "diff-splitter-stream", "type": "stream", "filter": {
          "name": "myFilter",
          "className": "io.wizzie.ks.normalizer.funcs.impl.ContainsDimensionFilter",
          "properties": {
            "dimensions": ["A"],
            "__MATCH": false
          }}}
      ]
```

Finally, when you define a sink you can repartitioned the stream using different key, to do this process you can use the field `partitionBy`.

```json
        "sinks":[
            {"topic":"output", "partitionBy":"X"},
            {"topic":"output1"}
        ]
```

By default the partitioned is done using the Kafka key message, that is indicated using the reserved value `__KEY`.

These examples are the same:

```json
        "sinks":[
            {"topic":"output", "partitionBy":"__KEY"}
        ]

        "sinks":[
            {"topic":"output1"}
        ]
```

## Other Notes
* If you build an invalid plan, the [PlanBuilderException](https://github.com/wizzie-io/normalizer/blob/master/service/src/main/java/zz/ks/exceptions/PlanBuilderException.java) will be throw.
* You can't do loop on the stream topology, if you define a loop the [TryToDoLoopException](https://github.com/wizzie-io/normalizer/blob/master/service/src/main/java/zz/ks/exceptions/TryToDoLoopException.java) will be throw.
