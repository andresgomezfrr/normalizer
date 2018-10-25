

<p align="center">
    <img src="docs/assets/images/logo.svg" alt="Normalizer" title="Normalizer" width="250%"/>
</p>

[![CircleCI](https://circleci.com/gh/wizzie-io/normalizer/tree/master.svg?style=svg)](https://circleci.com/gh/wizzie-io/normalizer/tree/master)
[![Docs](https://img.shields.io/badge/docs-current-brightgreen.svg)](https://wizzie-io.github.io/normalizer/) 
[![GitHub release](https://img.shields.io/github/release/wizzie-io/normalizer.svg)](https://github.com/wizzie-io/normalizer/releases/latest) 
[![wizzie-io](https://img.shields.io/badge/powered%20by-wizzie.io-F68D2E.svg)](https://github.com/wizzie-io/)


Normalizer is a stream processing engine based of Kafka Streams. Normalizer does maps (staless and stafull), flatmaps and filters by us. You only need to define a JSON stream where you specify the process logic and how the message are transformed.

Normalizer offers us: scalability, fault tolerance, back-preassure, KV states (RocksDB) and full Kafka integration ... [Try it now!!](https://wizzie-io.github.io/normalizer/getting/getting-started.html)

## Documentation

You can find the docs on the [Docs Website](https://wizzie-io.github.io/normalizer/)

## Getting Started

You can get started with Normalizer with this [tutorial](https://wizzie-io.github.io/normalizer/getting/getting-started.html).

## Compiling Sources

To build this project you can use `maven` tool. 

If you want to build the JAR of the project you can do:

```
mvn clean package
```

If you want to check if it passes all the test:

```
mvn test
```

If you want to build the distribution tar.gz:

```
mvn clean package -P dist
```

If you want to build the docker image, make sure that you have the docker service running:

```
mvn clean package -P docker
```

## Contributing

1. [Fork it](https://github.com/wizzie-io/normalizer/fork)
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Create a new Pull Request
