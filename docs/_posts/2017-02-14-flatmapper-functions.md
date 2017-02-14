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
TODO

