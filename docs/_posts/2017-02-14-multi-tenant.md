---
layout: page
title: "Definition"
category: multi-tenant
date: 2017-02-14 13:24:30
---

The Normalize service has a multi tenant mode, on this mode it prefix the `application.id` automatically on all the Kafka topic, except the bootstraper and metric topic. 

On this mode when you define a stream definition, for example:

```json
{
  "inputs":{
    "topic1":["stream1", "stream2"]
  },
  "streams":{
    "stream1":{
      "funcs":[
        {
          "name":"myFilter",
          "className":"io.wizzie.ks.normalizer.funcs.impl.FieldFilter",
          "properties": {
              "dimension":"FILTER-DIMENSION",
              "value":"FILTER-VALUE"
          }
        },
        {
          "name":"myFilterKey",
          "className":"io.wizzie.ks.normalizer.funcs.impl.FieldFilter",
          "properties": {
            "dimension":"__KEY",
            "value":"FILTER-kEY"
          }
        }
      ],
      "sinks":[
        {"topic":"output"}
      ]
    }
  }
}
```

You define the topic `topic1` but really you read from Kafka topic `${APP_ID}_topic1` and when you produce to the `output` topic, you really send data to the topic `${APP_ID}_output`. On this mode we define the `APP_ID == TENANT_ID`. To enable this mode you can configure the property `multi.id` to `true`.
