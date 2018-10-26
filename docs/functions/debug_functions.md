---
title: Debug Functions
layout: single
toc: true
---

Currently, the normalizer has two debug functions, both mappers. You can add these mappers into the stream processing to debug.

### LogMapper

The `LogMapper` prints all the message that pass by the function.
The logger uses this format `KEY: %s - VALUE: %s`

```json
{
  "name": "myLogMapper",
  "className": "io.wizzie.normalizer.funcs.impl.debug.LogMapper",
  "properties": { }
}
```

### MessagesMeanRateMapper

The `MessagesMeanRateMapper` tells us how many messages per second are processed by the normalizer.

This mapper has one property:
* `print_foreach`: The number of messages each the mapper reports the metrics.

The output log has this format `Messages rate mean: %.2f (Total: %d)`. The first value is the mean of messages per second and the second value is the total messages.

```json
{
  "name": "myMeanRateMapper",
  "className": "io.wizzie.normalizer.funcs.impl.debug.MessagesMeanRateMapper",
  "properties": {
    "print_foreach": 1000
  }
}
```