---
layout: page
title: "Debug Functions"
category: customize
date: 2017-02-22 11:08:45
order: 2
---

Currently, the normalizer has two debug functions, both mappers. You can add these mappers into the stream processing to debug.

### LogMapper

The `LogMapper` prints all the message into folder `/var/log/ks-normalizer/debug/.`
The logger uses this format `KEY: %s - VALUE: %s`

### MessagesMeanRateMapper

The `MessagesMeanRateMapper` tells us how many messages per second are processed by the normalizer.

This mapper has one property:
* `foreach`: The number of messages each the mapper reports the metrics.

The output log has this format `Messages rate mean: %.2f (Total: %d)`. The first value is the mean of messages per second and the second value is the total messages.

