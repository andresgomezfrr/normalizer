---
layout: page
title: "File Bootstrapper"
category: bootstrapper
date: 2017-02-14 12:36:14
order: 2
---

`io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper`

This bootstrapper read the stream config from local file system, and build a KS topology using this file. You need to add the properties on the configuration file.

| Property     | Description     | 
| :------------- | :-------------  | 
| `file.bootstrapper.path`      | Stream config file path      |

Library: https://github.com/wizzie-io/config-bootstrapper