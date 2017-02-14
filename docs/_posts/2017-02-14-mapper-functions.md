---
layout: page
title: "Mapper Functions"
category: funcs
date: 2017-02-14 12:58:03
order: 1
---

The mapper functions transforms the stream one message to another message `1 to 1` .

### SimpleMapper
The SimpleMapper is a function that allow us to simplify the JSON Object into one level. it also selects different fields from JSON Object and rename it. 

```json
{
  "name":"myMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.SimpleMapper",
  "properties": { 
    "maps": [
      {"dimPath":["A","B","C"], "as":"X"},
      {"dimPath":["Y","W","Z"], "as":"Q"},
      {"dimPath":["Y","W","P"]},
      {"dimPath":["timestamp"]}
    ]
  }
}
```

The SimpleMapper has one property that is called `maps` on this property you define the fields that you want to select and if you want rename it. If we have this json message:

```json
{
  "A": {
    "B": {
      "C": "MyValue"
    },
    "D": "A"
  },
  "Y": {
    "W": {
      "P": 123456,
      "Z": "MyOtherValue"
    }
  },
  "timestamp": 123456788
}
```

If we use this message using the SimpleMapper that is defined on the above example, we get this output:

```json
{"X":"MyValue", "Q":"MyOtherValue", "P": 123456, "timestamp": 123456788}
```
### ReplaceMapper

The ReplaceMapper is a function that allow us replace current values of messages by others that define us. The replace value should exists.

```json
{
  "name":"myReplaceMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.ReplaceMapper",
  "properties": { 
    "dimension":"myDimension",
    "replacements": {
      "currentvalue1":"replaceValue1",
      "currentvalue2":"replaceValue2",
      "currentvalue3":"replaceValue3"
    }
  }
}
```

The ReplaceMapper has two properties that are called `dimension` and `replacements`:

 * `dimension`: The dimension that indicate the current value to replace
 * `replacements`: Key-Value pairs for replace the current value. `Key` must be lowercase text.
 
If we have this json message:

```json
{
  "dimension":"VALUE-1",
  "otherDimension":"VALUE-2",
  "myDimension":"currentValue2"
}
```

If we use this message using the ReplaceMapper that is defined on the above example, we get this output:

```json
{
  "dimension":"VALUE-1",
  "otherDimension":"VALUE-2",
  "myDimension":"replaceValue2"
}
```

### JoinMapper
The JoinMapper is a function that allow us join as many values as us want and assign them in other dimension.

```json
{
  "name":"myJoinMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.JoinMapper",
  "properties": { 
    "newDimension":"myNewDimension",
    "values": [
     {"fromDimension":"dimension1", "orDefault":"defaultValue1", "delete": false},
     {"fromDimension":"dimension2", "orDefault":"defaultValue2", "delete": true},
     {"fromDimension":"dimension3", "orDefault":"defaultValue3"}
    ],
    "delimitier": "-"
  }
}
```

The JoinMapper has three properties that are called `newDimension`, `values` and `delimitier`:

* `newDimension`: Name of new dimension where all join values will be assign. This parameter can't be null.
* `values`: Array of values, contains a sequence of items with two parameters `fromDimension` and `orDefault`.
 * `fromDimension`: Dimension where we get the value to join. This parameter can't be null.
 * `orDefault`: Default value if value of `fromDimension` doesn't exists. This parameter can't be null.
 * `delete`: Delete dimension defined in `fromDimension` parameter. Default value is `false`.
* `delimitier`: Separator for each dimension. By default is `-`.

If we have this JSON message:

```json
{
  "dimension1":"A",
  "dimension2":"B",
  "timestamp":123456789
}
```

If we use this message using the JoinMapper that is defined on the above example, we get this output:

```json
{
  "dimension1":"A",
  "myNewDimension":"A/B/defaultValue3",
  "timestamp":123456789
}
```
### MaxValueMapper

The MaxValueMapper is a function that allow us from an array of numbers which is greater. The max value is detected and store in other dimension

```json
{
  "name":"myMaxValueMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.MaxValueMapper",
  "properties": { 
    "dimension": "measures",
    "max_dimension_name": "max_measure"
  }
}
```

The MaxValueMapper have two properties named `dimension` and `max_dimension_name`:

* `dimension`: Dimension where the numbers are located.
* `max_dimension_name`: The dimension where to save the max value detected.

I we have next json message:

```json
{
  "type": "measures",
  "timestamp": 123456789,
  "measures": [1.70, 1.65, 1.72, 1.8, 1.8, 1.9, 1.86]
}
```

If we use this message using the MaxValueMapper that is defined on the above example, we get next output:

```json
{
  "type": "measures",
  "timestamp": 123456789,
  "measures": [1.70, 1.65, 1.72, 1.8, 1.8, 1.9, 1.86],
  "max_measure": 1.9
}
```

### MinValueMapper
The MinValueMapper is like MaxValueMapper function, except that this function locate the smaller number in a array number.

```json
{
  "name":"myMinValueMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.MinValueMapper",
  "properties": { 
    "dimension": "measures",
    "min_dimension_name": "min_measure"
  }
}
```

The MinValueMapper like MaxValueMapper also has two properties `dimension` and `min_dimension_name`:
* `dimension`:  Dimension where the numbers are located.
* `min_dimension_name`: The dimension where to save the min value detected.

If we have next json message:

```json
{
  "type": "measures",
  "timestamp": 123456789,
  "measures": [1.70, 1.65, 1.72, 1.8, 1.8, 1.9, 1.86]
}
```

If we use this message using the MinValueMapper that is defined on the above example, we get next output:

```json
{
  "type": "measures",
  "timestamp": 123456789,
  "measures": [1.70, 1.65, 1.72, 1.8, 1.8, 1.9, 1.86],
  "min_measure": 1.65
}
```

### ClassificationMapper

The ClassficationMapper allow us classify a numeric value. 

```json
{
  "name":"myClassificationMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.ClassificationMapper",
  "properties": {
    "dimension": "mark",
    "new_dimension": "classification",
    "classification": ["F", "D", "C", "B", "A"],
    "intervals": [49, 60, 71, 85],
    "unknown_value": -1
  }
}
```

The ClassificationMapper has five properties:

* `dimension`: The dimension that indicate the numeric value to classify.
* `new_dimension`: Dimension where put classification value.
* `classification`: Array of strings where put the classification names.
* `intervals`: Array of limit for each classification.
* `unknown_value`: Single value for `unknown` classification.

In the function definition we are classify exams of Alberta Senior High School. We assume next message:

```json
{
  "first_name": "John",
  "last_name": "Doe",
  "subject": "Chemistry",
  "mark": 75
}
```

If we use this message using the ClassificationMapper that is defined on the example above, we get next output:

```json
{
  "first_name": "John",
  "last_name": "Doe",
  "subject": "Chemistry",
  "mark": 75,
  "classification": "B"
}
```
