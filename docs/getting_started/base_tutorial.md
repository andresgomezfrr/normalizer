---
title: Base Tutorial
layout: single
toc: true
---

On this page, we can try a stream example using a real Kafka cluster and the normalizer jar artifact. We are going to suppose that you have built the normalizer distribution how we explain on the [Building](https://github.com/wizzie-io/normalizer#compiling-sources) section.

### Explication
First of all, we need define a stream for launch a normalizer application.

#### Stream Config Json (my-stream-tutorial.json)

```json
{
    "inputs":{
      "mytopic":["mystream"]
    },
    "streams":{
      "mystream":{
          "funcs":[
                {
                  "name":"firstMapper",
                  "className":"io.wizzie.normalizer.funcs.impl.SimpleMapper",
                  "properties": {
                    "maps": [
                      {"dimPath":["body", "messages"]},
                      {"dimPath":["header", "mac"], "as":"id"}
                    ]
                  }
                }
          ],
          "sinks":[
              {"topic":"partitionedStream", "type":"stream", "partitionBy":"id"}
          ]
      },
      "partitionedStream":{
          "funcs":[
                {
                  "name":"flattenMessages",
                  "className":"io.wizzie.normalizer.funcs.impl.ArrayFlattenMapper",
                  "properties": {
                    "flat_dimension": "messages"
                  }
                }
          ],
          "sinks":[
              {"topic":"output", "type":"kafka"}
          ]
      }
    }
}
```
At this point we have defined our stream where we inject messages
#### Phase 0: Input messages
```json
{
  "header": {
    "mac": "00:00:00:00:00",
    "version": 2
  },
  "body": {
    "messages": [
      {
        "type": "rssi",
        "value": -78
      },
      {
        "type": "cpu",
        "value": 80
      }
    ],
    "someData": "otherData"
  }
}
```

On this example we read the Kafka topic `mytopic` and mapped it to the stream `mystream`. On the stream `mystream` we use one function that is called `firstMapper`, using this function we select messages specific fields and rename them. We select the field `messages` that is inside the field `body` and we also select the field `mac` that is inside `header` and rename it to `id`. The processed stream is partitioned by the field `id` and it is sent to the `partitionedStream` that is created at runtime.

#### Phase 1: Partitioned stream messages

```json
{
  "id": "00:00:00:00:00",
  "messages": [
    {
      "type": "rssi",
      "value": -78
    },
    {
      "type": "cpu",
      "value": 80
    }
  ]
}
```

Later, we process the `partitionedStream` using another function that is called `flattenMessages` on this case we do a flatten on the messages that is an JSON ARRAY.

#### Phase 2: Output messages
```json
{"id": "00:00:00:00:00", "type": "rssi", "value": -78}
{"id": "00:00:00:00:00", "type": "cpu",  "value":  80}
```

Finally the result will be sent to Kafka again into a topic that is called `output`.

### Execution

On the first time we need to have a running Kafka cluster and the decompressed normalizer distribution.

#### Config file

We need to modify the config file that is inside the folder `config/sample_config.json`, we can change it or destroy it and create a new one with this content.

```json
{
  "application.id": "my-first-normalizer-app",
  "bootstrap.servers": "localhost:9092",
  "num.stream.threads": 1,
  "bootstrapper.classname": "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper",
  "file.bootstrapper.path": "/etc/normalizer/my-stream-tutorial.json",
  "metric.enable": true,
  "metric.listeners": ["io.wizzie.metrics.ConsoleMetricListener"],
  "metric.interval": 60000
}
```

On this config file we indicate the `application.id` that will identify our instances group and some Kafka Broker. On the example we are going to use the `FileBootstrapper` so we read the config using a local file. We also need to set the property `file.bootstrapper.path` to the path where we have the stream config file.

Now we can start the normalizer service to do that we can uses the init script that is inside the folder bin:

```
normalizer/bin/normalizer-start.sh normalizer/config/sample_config.json
```

When the normalizer is running you can check it on the log file that is on directory `/var/log/ks-normalizer/normalizer.log` by default. If it started correctly you can see something like this:

```
2016-10-26 13:18:27      StreamTask [INFO] task [0_0] Initializing state stores
2016-10-26 13:18:27      StreamTask [INFO] task [0_0] Initializing processor nodes of the topology
2016-10-26 13:18:27    StreamThread [INFO] stream-thread [StreamThread-1] Creating active task 1_0 with assigned partitions [[__my-first-normalizer-app_normalizer_mystream_to_partitionedStream-0]]
2016-10-26 13:18:27      StreamTask [INFO] task [1_0] Initializing state stores
2016-10-26 13:18:27      StreamTask [INFO] task [1_0] Initializing processor nodes of the topology
```

Now you can produce some input message into `input` Kafka topic, but first you could open a Kafka consumer to check the output messages.

* Consumer
```
kafka_dist/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --property print.key=true --topic output --new-consumer
```

* Producer
```
kafka_dist/bin/kafka-console-producer.sh --broker-list localhost:9092 --property parse.key=true --property key.separator=, --topic mytopic
```

You can write some message into console-producer:

```
key1,{"header":{"mac":"00:00:00:00:00","version":2},"body":{"messages":[{"type":"rssi","value":-78},{"type":"cpu","value":80}],"someData":"otherData"}}
```

and you must see the output message on the console-consumer:

```
00:00:00:00:00	{"type":"rssi","value":-78,"id":"00:00:00:00:00"}
00:00:00:00:00	{"type":"cpu","value":80,"id":"00:00:00:00:00"}
```

This is the end of the tutorial!! Congratulations! ;)
