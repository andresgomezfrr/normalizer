---
layout: page
title: "Definition"
category: bootstrapper
date: 2017-02-14 12:42:07
---

The bootstrapper is the mechanism that is used to load stream config into normalizer. You can configure the boostrapper class on the config file, using `bootstrapper.classname` property. Currently, the normalizer has two bootstrappers:

* [FileBootstrapper](https://wizzie.io/normalizer/bootstrapper/file-boostrapper.html)
* [KafkaBootstrapper](https://wizzie.io/normalizer/bootstrapper/kafka-boostrapper.html)

If you want you can extend the bootstrappers using the [ThreadBootstrapper](https://github.com/wizzie-io/normalizer/blob/master/service/src/main/java/io/wizzie/ks/normalizer/builder/bootstrap/ThreadBootstrapper.java) abstract class. 

On this class you need to implement two methods:

```void init(Builder builder, Config config, MetricsManager metricsManager) throws Exception;```

* To initialize update the stream config you need to use the method `updateStreamConfig(...)` from Builder class.

```public void run()```
 
* You only need implemented it, if you want to update the stream config continuously. 

