---
layout: page
title: "FlatMapper Functions"
category: funcs
date: 2017-02-14 12:58:16
order: 2
---

The flat mapper functions transforms stream one message to zero or many messages `1 to zero OR 1 to many`.

### JqFlatMapper

The JqFlatMapper is a special mapper that allow us to use [Jq Syntax](https://stedolan.github.io/jq/), internally the mapper uses [eiiches/jackson-jq](https://github.com/eiiches/jackson-jq) library to build the jq query.

```json
{
  "name":"jqFlatMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.JqFlatMapper",
  "properties": { 
    "jqQuery": "{ids:[.ids|split(\",\")[]|tonumber|.+100],name}"
  }
}
```

You only need to set he property `jqQuery` where you must to define a new JSON. The above function transform this input message into this output message.

**Input:**

```json
{"ids": "12,15,23", "name": "jackson", "timestamp": 1418785331123}
```

**Output:**

```json
{"ids": [112, 115, 123], "name": "jackson"}
```

### SplitterFlatMapper

The SplitterFlatMapper is a flatMapper that process a single message and produce one or more messages. This flatMapper split a message into multiples messages based on the difference of time.

```json
{
  "name":"mySplitterFlatMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.SplitterFlatMapper",
  "properties": {
    "dimensions": ["bytes", "pkts"],
    "timestampDim": "timestamp",
    "firstTimestampDim": "first_switch"
  }
}
```

On this flatMapper you have three properties:

 * `dimensions`: The dimension that the mapper should split.
 * `timestampDim`: The dimension that indicate the current time.
 * `first_switch`: The dimension that indicate the previous time.

This mapper process a message and divide the `bytes` and `pkts` counters across multiple output messages based on the minutes difference between `timestamp` and `first_switch`.

**Input:**

```json
{"timestamp": 1477379967, "first_switch": 1477379847, "bytes": 120, "pkts": 60}
```

**Output:**

```json
{"timestamp": 1477379847, "bytes": 60, "pkts": 30}
{"timestamp": 1477379967, "bytes": 60, "pkts": 30}
```

### ArrayFlattenMapper

This flatMapper allow us do a flatten array using all the other message fields. 

```json
{
  "name":"myArrayFlatMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.ArrayFlattenMapper",
  "properties": {
    "flat_dimension": "my_array_dim"
  }
}
```

On this flatMapper, you only need to specify the property `flat_dimension`, **this dimension must be an Json Array**.

**Input:**

```json
{"timestamp": 1477379967, "my_array_dim": [{"my_first": 1, "my_second": 2}, {"other_first": 1, "other_second": 2}], "outside_field":"outside_value"}
```

**Output:**

```json
{"timestamp": 1477379967, "outside_field":"outside_value", "my_first": 1, "my_second": 2}
{"timestamp": 1477379967, "outside_field":"outside_value", "other_first": 1, "other_second": 2}
```

### FormatterFlatMapper

The formatterFlatMapper allow us generate one or more message from a unique message. It allows us to select the different dimensions to build the new output messages.

On this flatMapper you have three properties:

 * `commonFields`: The dimension that the mapper must to select on the input message and put on all output messages.
 * `filters`: This mapper allows us to apply filter to create multiple stream branches into the function.
 * `generators`: The generators apply on each input message transforming it on a new output message. Currently, there are two generators:
    * **constant**: The `constant` generator is used to add constant value to the output message.
    * **fieldValue**: The `fieldValue` generator select a specific field form the input message and put it on the output message.

```json
{
  "inputs": {
    "topic1": [
      "stream1"
    ]
  },
  "streams": {
    "stream1": {
      "funcs": [
        {
          "name": "myFormatterFlatMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.FormatterFlatMapper",
          "properties": {
            "commonFields": ["timestamp", "user_id"],
            "filters": [
              {"name": "inside", "className": "io.wizzie.ks.normalizer.funcs.impl.FieldFilter", "properties": {"dimension": "location", "value": "inside"}},
              {"name": "outside", "className": "io.wizzie.ks.normalizer.funcs.impl.FieldFilter", "properties": {"dimension": "location", "value": "outside"}}
            ],
            "generators": [
              {
                "filter": "outside",
                "definitions": [
                  {
                    "apply": [
                      {"field": "location", "content": {"type": "constant", "value": "garden"}},
                      {"field": "light", "content": {"type": "fieldValue", "value": "dev_1"}}
                    ]
                  },
                  {
                    "apply": [
                      {"field": "location", "content": {"type": "constant", "value": "swimming pool"}},
                      {"field": "cleaner_engine", "content": {"type": "fieldValue", "value": "dev_2"}}
                    ]
                  }
                ]
              },
              {
                "filter": "inside",
                "definitions": [
                  {
                    "apply": [
                      {"field": "location", "content": {"type": "constant", "value": "kitchen"}},
                      {"field": "light", "content": {"type": "fieldValue", "value": "light_1"}}
                    ]
                  },
                  {
                    "apply": [
                      {"field": "location", "content": {"type": "constant", "value": "bedroom"}},
                      {"field": "light", "content": {"type": "fieldValue", "value": "light_2"}}
                    ]
                  }
                ]
              }
            ]
          }
        }
      ],
      "sinks": [
        {
          "topic": "output"
        }
      ]
    }
  }
}
```

**Input**:

```json
{"timestamp": 1477379967, "user_id": "MY_USER_ID", "location": "outside", "dev_1": "ON", "dev_2": "OFF"}
{"timestamp": 1477379967, "user_id": "MY_USER_ID", "location": "inside", "light_1": "ON", "light_2": "ON"}

```

**Output:**

```json
{"timestamp": 1477379967, "user_id": "MY_USER_ID", "location": "garden", "light":"ON"}
{"timestamp": 1477379967, "user_id": "MY_USER_ID", "location": "swimming pool", "cleaner_engine":"OFF"}
{"timestamp": 1477379967, "user_id": "MY_USER_ID", "location": "kitchen", "light":"ON"}
{"timestamp": 1477379967, "user_id": "MY_USER_ID", "location": "bedroom", "light":"OFF"}
```
