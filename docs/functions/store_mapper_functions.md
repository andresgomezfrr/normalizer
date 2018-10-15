---
title: Store Mapper Functions
layout: single
toc: true
---

The Store Mappers are the same way to work that the Mappers `1 event to 1 event`, but on this case the Store Mappers have a cache on you can compute value based on a previous events.
You can add store to a function using the key `stores`, this key is a JSON Array with the store's names.

### DiffCounterStoreMapper

The DiffCounterStoreMapper allows us to compute the difference on one or more values based on specific group of keys.

This mapper has 4 properties:
    * `counters`: The dimensions where you want to compute the difference.
    * `timestamp`: The dimension that contains the time of the message, this is used to save the timestamp and put on the message the current timestamp and the old timestamp (the timestamp when the last event arrived). Default: `timestamp`
    * `keys`: The group of dimensions that is used to compute the difference beetween the values.
    * `firsttimeview`: Discard or not the first event. Default: `true`
    * `sendIfZero`: Send or not a message if the difference is zero. Default `true`

Also, you must add the `"stores":["counter-store"]` because this store is used by DiffCounterStoreMapper function to compute the differences.

```json
{
  "inputs":{
    "topic1":["stream1"]
  },
  "streams":{
    "stream1":{
      "funcs":[
        {
          "name":"diffCounterMapper",
          "className":"io.wizzie.normalizer.funcs.impl.DiffCounterStoreMapper",
          "properties": {
            "counters": ["value"],
            "timestamp": "timestamp",
            "keys": ["user_id", "sensor_id"],
            "firsttimeview": false
          },
          "stores":["counter-store"]
        }
      ],
      "sinks":[
        {"topic":"output"}
      ]
    }
  }
}
```

**Input**:

```json
{"timestamp": 1000000000, "user_id": "AAAAA", "sensor_id": "sensor_1111", "value": 10000}
{"timestamp": 1555550000, "user_id": "AAAAA", "sensor_id": "sensor_1111", "value": 22000}

{"timestamp": 1666660000, "user_id": "AAAAA", "sensor_id": "sensor_2222", "value": 22000}
{"timestamp": 1777770000, "user_id": "AAAAA", "sensor_id": "sensor_2222", "value": 25000}

{"timestamp": 1666660000, "user_id": "BBBBB", "sensor_id": "sensor_2222", "value": 30000}
{"timestamp": 1777770000, "user_id": "BBBBB", "sensor_id": "sensor_2222", "value": 40000}

```

**Output:**

```json
{"timestamp": 1555550000, "last_timestamp": 1000000000, "user_id": "AAAAA", "sensor_id": "sensor_1111", "value": 12000}
{"timestamp": 1777770000, "last_timestamp": 1666660000, "user_id": "AAAAA", "sensor_id": "sensor_2222", "value": 3000}
{"timestamp": 1777770000, "last_timestamp": 1666660000, "user_id": "BBBBB", "sensor_id": "sensor_2222", "value": 10000}

```
