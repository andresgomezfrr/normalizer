---
layout: page
title: "Filter Functions"
category: funcs
date: 2017-02-14 12:58:42
order: 4
---

The filter functions are used to filter out messages from the stream.

All the filters has a common property that is called `__MATCH`. The `__MATCH` property allows us to filter out the messages that either matches the filter or does not.
 * `__MATCH: true` The filter filters out the messages that satisfy the filter.
 * `__MATCH: false` The filter filters out the messages that don't satisfy the filter.

### FieldFilter

The FieldFilter is a filter that allows us to filter if a specific dimension contains a specific value match.

```json
{
  "name": "myFieldFilter",
  "className": "io.wizzie.ks.normalizer.funcs.impl.FieldFilter",
  "properties": {
    "dimension": "FILTER_DIMENSION",
    "value": "FILTER_VALUE"
  }
}
```

The FieldFilter has two properties that are called `dimension` and `value`:
- `dimension`: The dimension to get the value.
- `value`: Value to compare with obtained value.
If we have next JSON message:

```json
{
  "DIM-A":"VALUE-A",
  "DIM-B":10,
  "FILTER_DIMENSION": "FILTER_VAL"
}
```

The FieldFilter doesn't match and return `false` value. Bu if we have next message:

```json
{
  "DIM-A": "VALUE-A",
  "DIM-B": 10,
  "FILTER_DIMENSION": "FILTER_VALUE"
}
```

The FieldFilter match and return `true` value

If you use `__KEY` dimension the filter checks the message key.

### MultiValueFieldFilter

The MultiValueFieldFilter is a filter that allows us to filter if a specific dimension contains some value from a list.

```json
{
  "name": "myFieldFilter",
  "className": "io.wizzie.ks.normalizer.funcs.impl.MultiValueFieldfilter",
  "properties": {
    "dimension": "FILTER_DIMENSION",
    "values": ["FILTER_VALUE", "FILTER_VALUE_1"]
  }
}
```

The FieldFilter has two properties that are called `dimension` and `value`:
- `dimension`: The dimension to get the value.
- `values`: Values to compare with obtained value.
If we have next JSON message:

```json
{
  "DIM-A":"VALUE-A",
  "DIM-B":10,
  "FILTER_DIMENSION": "FILTER_VAL"
}
```

The FieldFilter doesn't match and return `false` value. Bu if we have next message:

```json
{
  "DIM-A": "VALUE-A",
  "DIM-B": 10,
  "FILTER_DIMENSION": "FILTER_VALUE"
}
```

or

```json
{
  "DIM-A": "VALUE-A",
  "DIM-B": 10,
  "FILTER_DIMENSION": "FILTER_VALUE_1"
}
```

The FieldFilter match and return `true` value.

If you use `__KEY` dimension the filter checks the message key.


### ContainsDimensionFilter
The ContainsDimensionFilter is a filter that allows us to filter if a JSON contains specific dimensions defined by us.

```json
{
  "name": "myContainsDimensionFilter",
  "className": "io.wizzie.ks.normalizer.funcs.impl.ContainsDimensionFilter",
  "properties": {
    "dimensions": ["DIM-A", "DIM-B", "DIM-C"]
  }
}
```
The ContainsDimensionFilter has only one property named `dimensions`. The property indicates the dimensions that json message must have.

If we have next JSON message:

```json
{
  "DIM-A": "VALUE-A",
  "DIM-K": "VALUE-K",
  "DIM-C": "VALUE-C"
}
```

The ContainsDimensionFilter doesn't match. But if we have next JSON message:

```json
{
  "DIM-A": "VALUE-A",
  "DIM-B": "VALUE-B",
  "DIM-C": "VALUE-C"
}
```

The ContainsDimensionFilter match.

### StartWithFilter
The StartWithFilter is a filter that allows us to filter if a specific dimension contains a value that start with a specific string.

```json
{
  "inputs":{
    "topic":["stream"]
  },
  "streams":{
    "stream":{
      "funcs":[
        {
          "name":"myKeyStartWithFilter",
          "className":"io.wizzie.ks.normalizer.funcs.impl.StartWithFilter",
          "properties": {
            "dimension": "DIM-B",
            "start_with": "FILTER"
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

The StartWithFilter has two properties called `dimension` and `start_with`:
* `dimension`: Dimension of which we get the value to filter.
* `start_with`: String to apply to value. If match then return `true` else return `false`.

If we have next JSON message:

```json
{
  "DIM-A":"NOT-FILTER-A",
  "DIM-B":"NOT-FILTER-B",
  "DIM-C":"NOT-FILTER-C"
}
```

The StartWithFilter doesn't match. But if we have next JSON message:

```json
{
  "DIM-A":"NOT-FILTER-A",
  "DIM-B":"FILTER-B",
  "DIM-C":"NOT-FILTER-C"
}
```

The StartWithFilter match and return `true`.

### AndFilter

The AndFilter is a filter that allows us to apply as many filters as we want. If all filters match then AndFilter return `true` else return `false`.

```json
{
  "name":"myAndFilter",
  "className":"io.wizzie.ks.normalizer.funcs.impl.AndFilter",
  "properties": {
    "filters": [
      {
        "name": "myFieldFilter",
        "className": "io.wizzie.ks.normalizer.funcs.impl.FieldFilter",
        "properties": {
          "dimension": "FILTER-DIMENSION",
          "value": "FILTER-VALUE"
        }
      },
      {
        "name": "myContainsFilter",
        "className": "io.wizzie.ks.normalizer.funcs.impl.ContainsDimensionFilter",
        "properties": {
          "dimensions": ["A", "B", "C"]
        }
      }
    ]
  }
}
```

The AndFilter has been one property called `filters`.
* `filters`: Array of filters definition. Each filter have particular properties.

If we have this JSON message:

```json
{
  "dimension":"VALUE-1",
  "FILTER-DIMENSION":"FILTER-VALUE",
  "other":"currentValue"
}
```

The AndFilter doesn't match and return `false` because only contains `FILTER-DIMENSION`.

However if we have this JSON message:

```json
{
  "dimension":"VALUE-1",
  "FILTER-DIMENSION":"FILTER-VALUE",
  "A":"currentValue"
}
```

The AndFilter matchs and return `true` because all defined filters have been matched.

### OrFilter

The OrFilter is a filter that allows us to apply as many filters as we want. If any filter matches then OrFilter return `true` else return `false`.

```json
{
  "name":"myOrFilter",
  "className":"io.wizzie.ks.normalizer.funcs.impl.OrFilter",
  "properties": {
    "filters": [
      {
        "name": "myFieldFilter",
        "className": "io.wizzie.ks.normalizer.funcs.impl.FieldFilter",
        "properties": {
          "dimension": "FILTER-DIMENSION",
          "value": "FILTER-VALUE"
        }
      },
      {
        "name": "myContainsFilter",
        "className": "io.wizzie.ks.normalizer.funcs.impl.ContainsDimensionFilter",
        "properties": {
          "dimensions": ["A", "B", "C"]
        }
      }
    ]
  }
}
```

The OrFilter has been one property called `filters`.
* `filters`: Array of filters definition. Each filter have particular properties.

If we have this JSON message:

```json
{
  "dimension":"VALUE-1",
  "FILTER-DIMENSION":"FILTER-VALUE",
  "other":"currentValue"
}
```

The OrFilter match and return true because the dimension `FILTER-DIMENSION` contains value `FILTER-VALUE`

### IsStringFilter

The IsStringFilter is a filter that allows us to filter if a specific dimension is a String class.

```json
{
  "name":"isString",
  "className":"io.wizzie.ks.normalizer.funcs.impl.IsStringFilter",
  "properties": {
    "dimension":"string-dimension"
  }
}
```

The IsStringFilter has two properties that are called `dimension` and `value`:
- `dimension`: The dimension to check the value.

If we have next JSON message:

```json
{
  "DIM-A":"VALUE-A",
  "DIM-B":10,
  "string-dimension": 2
}
```

The IsStringFilter doesn't match and return `false` value. Bu if we have next message:

```json
{
  "DIM-A": "VALUE-A",
  "DIM-B": 10,
  "string-dimension": "FILTER_VALUE"
}
```

The IsStringFilter match and return `true` value

### IsListFilter

The IsListFilter is a filter that allows us to filter if a specific dimension is a List class.

```json
{
  "name":"isList",
  "className":"io.wizzie.ks.normalizer.funcs.impl.IsListFilter",
  "properties": {
      "dimension":"list-dimension"
  }
}
```

The IsListFilter has two properties that are called `dimension` and `value`:
- `dimension`: The dimension to check the value.

If we have next JSON message:

```json
{
  "DIM-A":"VALUE-A",
  "DIM-B":10,
  "list-dimension": 2
}
```

The IsListFilter doesn't match and return `false` value. Bu if we have next message:

```json
{
  "DIM-A": "VALUE-A",
  "DIM-B": 10,
  "list-dimension": ["FILTER_VALUE", "b"]
}
```

The IsListFilter match and return `true` value
