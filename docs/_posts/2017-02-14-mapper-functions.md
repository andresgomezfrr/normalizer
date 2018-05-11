---
layout: page
title: "Mapper Functions"
category: funcs
date: 2017-02-14 12:58:03
order: 1
---

The mapper functions transforms the stream one message to another message `1 to 1`.

### <a name="index"></a> Mapper functions
* [FieldMapper](#fieldMapper)
* [SimpleMapper](#simpleMapper)
* [ReplaceMapper](#replaceMapper)
* [JoinMapper](#joinMapper)
* [MaxValueMapper](#maxValueMapper)
* [MinValueMapper](#minValueMapper)
* [ClassificationMapper](#classificationMapper)
* [StringSplitterMapper](#stringSplitterMapper)
* [StringReplaceMapper](#stringReplaceMapper)
* [TimeMapper](#timeMapper)
* [FieldTypeConverterMapper](#fieldTypeConverterMapper)
* [ArithmeticMapper](#arithmeticMapper)
* [SimpleArrayMapper](#simpleArrayMapper)
* [RenameMapper](#renameMapper)
* [ArrayDecompositionMapper](#arrayDecompositionMapper)


### <a name="fieldMapper"></a> FieldMapper [ [Top](#index) ]

The FieldMapper is a function that allows us to add fields to one event.

```json
{
  "name": "myFieldMapper",
  "className": "io.wizzie.ks.normalizer.funcs.impl.FieldMapper",
  "properties": {
   "dimensions": [
     {
       "dimension": "dimension1",
       "value": "defaultValue1",
       "overwrite": false
      },
      {
        "dimension": "dimension2",
        "value": "defaultValue2",
        "overwrite": true
       },
       {
         "dimension": "dimension3",
         "value": "defaultValue3"
      }
    ]
  }
}
```

The FieldMapper has one property that is called `dimensions` on this property you define the fields that you want to add and if you want overwrite them if they exists. If we have this json message:

```json
{
  "dimension1":"value1",
  "dimension2":"value2",
  "dimension3": "value3",
  "timestamp": 123456789
}
```

If we use this message using the FieldMapper that is defined on the above example, we get this output:

```json
{"dimension1":"value1", "dimension2":"defaultValue2", "dimension3": "value3", "timestamp": 123456788}
```

By default the FieldMapper will not overwrite the values if you don't specify the overwrite property.


### <a name="simpleMapper"></a> SimpleMapper [ [Top](#index) ]

The SimpleMapper is a function that allows us to simplify the JSON Object into one level. It also selects different fields from JSON Object and renames it. The not selected fields will be removed.

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

### <a name="replaceMapper"></a> ReplaceMapper [ [Top](#index) ]

The ReplaceMapper is a function that allows us to replace current values of messages by others that define by us. The replace value should exists.

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

 * `dimension`: The dimension that indicate the current value to replace.
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

### <a name="joinMapper"></a> JoinMapper
The JoinMapper is a function that allows us to join as many values as we want and assign them in other dimension.

```json
{
  "name":"myJoinMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.JoinMapper",
  "properties": {
    "dimensionName":"myNewDimension",
    "values": [
     {"fromDimension":"dimension1", "orDefault":"defaultValue1", "delete": false},
     {"fromDimension":"dimension2", "orDefault":"defaultValue2", "delete": true},
     {"fromDimension":"dimension3", "orDefault":"defaultValue3"}
    ],
    "delimitier": "-"
  }
}
```

The JoinMapper has three properties that are called `dimensionName`, `values` and `delimitier`:

* `dimensionName`: Name of new dimension where all join values will be assign. This parameter can't be null.
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
  "myNewDimension":"A-B-defaultValue3",
  "timestamp":123456789
}
```

### <a name="maxValueMapper"></a> MaxValueMapper [ [Top](#index) ]

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

### <a name="minValueMapper"></a> MinValueMapper [ [Top](#index) ]
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

### <a name="classificationMapper"></a> ClassificationMapper [ [Top](#index) ]

The ClassficationMapper allows us to classify a numeric value.

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

### <a name="stringSplitterMapper"></a> StringSplitterMapper [ [Top](#index) ]

The StringSplitterMapper allows us to split one dimension into multiple dimension.

```json
        {
          "name":"myStringSplitterFunction",
          "className":"io.wizzie.ks.normalizer.funcs.impl.StringSplitterMapper",
          "properties": {
            "dimension": "DIM-H",
            "delimitier": ">",
            "fields": ["country", "province", "city"]
          }
        }
```

This mapper has some properties:

* `dimension`: The dimension field that you want to split.
* `delimitier`:  The character that the mapper uses to split.
* `fields`: The new fields to the splitter dimensions. This is a JSON Array.
* `delete_dimension`: This is a boolean to indicate if you want to delete the original dimension. Default: false

**Input**:

```json
{"timestamp": 1477379967, "DIM-H": "Spain>Andalucia>Sevilla"}
```

**Output:**

```json
{"timestamp": 1477379967, "country": "Spain", "province": "Andalucia", "city":"Sevilla", "DIM-H": "Spain>Andalucia>Sevilla"}
```

### <a name="stringReplaceMapper"></a> StringReplaceMapper [ [Top](#index) ]

The StringReplaceMapper replaces the dimension string value to another one.

```json
        {
          "name":"myStringReplacementFunction",
          "className":"io.wizzie.ks.normalizer.funcs.impl.StringReplaceMapper",
          "properties": {
            "dimension": "DIM-C",
            "target_string": "-",
            "replacement_string": ":"
          }
        }
```

This mapper has some properties:

* `dimension`: The dimension that you want to transform.
* `target_string`: The string sequence that you want to replace.
* `replacement_string`: The string sequence that you want to use on the change.

**Input**:

```json
{"timestamp": 1477379967, "DIM-C": "00-00-AA-FF-11-33"}
```

**Output:**

```json
{"timestamp": 1477379967, "DIM-C": "00:00:AA:FF:11:33"}
```

### <a name="timeMapper"></a> TimeMapper [ [Top](#index) ]

The TimeMapper converts different time formats to a specified format.

```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"millis",
            "toFormat":"secs",
            "forceStringOutput": "false"
          }
        }
```

This mapper has some properties:

* `dimension`: The dimension that you want to transform.
* `fromFormat`: The format that will be received by the function.
* `toFormat`: The format that you want at the function output.
* `forceStringOutput`: This property forces the output to be a string. i.e. if you want to have `{"timestamp": "1477379967"}` as time output instead of `{"timestamp": 1477379967}`` you have to set this property to true. Its default value is false.
* `forceTimestamp`: This property forces to output a timestamp if it doesn't exists at the event. i.e. if you want to have `{"timestamp": 1477379967,"a":"hello-world"}` as output when you have: `{"a": "hello-word"}`` you have to set this property to true. Its default value is true. When forcing timestamp, if you have a null timestamp value at input, current time will be sended at output.


Both `fromFormat` and `toFormat` must be: "ISO", "millis", "secs" or "pattern: ...".

If you choose "pattern: ..." as fromFormat or toFormat you have to specify a valid format. A valid format is a JDK date format (you can read more at: https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html and at JDK docs).


#### Examples:

##### Example 1: millis -> secs

```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"millis",
            "toFormat":"secs",
            "forceStringOutput": "false",
            "forceTimestamp": "true"
          }
        }
```


**Input**:

```json
{"timestamp": 1234567890000, "DIM-C": "00:00:AA:FF:11:33"}
```

**Output:**

```json
{"timestamp": 1234567890, "DIM-C": "00:00:AA:FF:11:33"}
```

##### Example 2: millis -> secs (force string)
```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"millis",
            "toFormat":"secs",
            "forceStringOutput": "true"
          }
        }
```


**Input**:

```json
{"timestamp": 1234567890000, "DIM-C": "00:00:AA:FF:11:33"}
```

**Output:**

```json
{"timestamp": "1234567890", "DIM-C": "00:00:AA:FF:11:33"}
```

##### Example 3: ISO -> secs
```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"ISO",
            "toFormat":"secs",
            "forceStringOutput": "false"
          }
        }
```


**Input**:

```json
{"timestamp": "2009-02-13T23:31:30.000Z", "DIM-C": "00:00:AA:FF:11:33"}
```

**Output:**

```json
{"timestamp": 1234567890, "DIM-C": "00:00:AA:FF:11:33"}
```

##### Example 4: secs -> pattern
```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"secs",
            "toFormat":"pattern: yyyyMMdd",
            "forceStringOutput": "false"
          }
        }
```


**Input**:

```json
{"timestamp": 1234567890, "DIM-C": "00:00:AA:FF:11:33"}
```

**Output:**

```json
{"timestamp": "20090213", "DIM-C": "00:00:AA:FF:11:33"}
```

##### Example 5: pattern -> pattern
```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"pattern: yyyy-MM-dd",
            "toFormat":"pattern: yyyyMMdd",
            "forceStringOutput": "false"
          }
        }
```


**Input**:

```json
{"timestamp": "2009-02-13", "DIM-C": "00:00:AA:FF:11:33"}
```

**Output:**

```json
{"timestamp": "20090213", "DIM-C": "00:00:AA:FF:11:33"}
```

##### Example 6: millis -> secs (no force timestamp)
```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"millis",
            "toFormat":"secs",
            "forceStringOutput": "false",
            "forceTimestamp": "false"
          }
        }
```


**Input**:

```json
{"DIM-C": "00:00:AA:FF:11:33"}
```

**Output:**

```json
{"DIM-C": "00:00:AA:FF:11:33"}
```

##### Example 7: millis -> secs (forcing timestamp)
```json
        {
          "name": "myTimeMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.TimeMapper",
          "properties": {
            "dimension":"timestamp",
            "fromFormat":"millis",
            "toFormat":"secs",
            "forceStringOutput": "false",
            "forceTimestamp": "true"
          }
        }
```


**Input**:

```json
{"DIM-C": "00:00:AA:FF:11:33"}
```

**Output:**

```json
{"timestamp": 1234567890, "DIM-C": "00:00:AA:FF:11:33"}
```

### <a name="fieldTypeConverterMapper"></a> FieldTypeConverterMapper [ [Top](#index) ]

The FieldTypeConverterMapper allows us to convert value types of several dimensions into other types.

```json
        {
          "name":"myFieldTypeConverterMapper",
          "className":"io.wizzie.ks.normalizer.funcs.impl.FieldTypeConverterMapper",
          "properties": {
			"conversions": [
				{
				  "dimension": "field-A",
                  "from": "string",
                  "to": "boolean"
				},
                {
                  "dimension": "timestamp",
                  "from": "string",
                  "to": "number"
                },
                {
                  "dimension": "field-B",
                  "from": "number",
                  "to": "boolean",
                  "newDimension": "isPositive"
                }
			]
          }
        }
```


**Input**:

```json
{"timestamp": "1503391561", "field-A": "true", "field-B": 1.35}
```

**Output:**

```json
{"timestamp": 1503391561, "field-A": true, "field-B": 1.35, "isPositive": true}
```

This mapper has some properties:

* `conversions`: Set of conversions that you want to apply.
	* `dimension`:  The dimension field which contains the value that you want to convert.
	* `from`: The source type: `NUMBER`, `STRING` or `BOOLEAN`.
	* `to`: The destiny type: `NUMBER`, `STRING` or `BOOLEAN`.
	* `newDimension`: If you set this property then the new value field will be renamed. Else `dimension` will be replace.

You can convert next types:

* `NUMBER` : This type represents `long`, `integer`, `short`, `byte`, `double` and `float`
* `STRING` : This type represents strings.
* `BOOLEAN` : This type represents boolean values: `true` or `false`

Next table shows available conversion between types:

| FROM\TO | NUMBER | STRING | BOOLEAN |
|:---------:|:--------:|:--------:|:---------:|
| **NUMBER**  | -      |&#x2713;|&#x2713;|
| **STRING**  |&#x2713;|-|&#x2713;|
| **BOOLEAN** |&#x2713;|&#x2713;|-|

Below you can found a description about conversion between types:

|Conversion|Description|
|--------|---------|
|NUMBER &#x2192; NUMBER|Without effect.|
|NUMBER &#x2192; STRING|Convert any number type into string type. If the number value is `1.35` then it will be converted into `"1.35"`|
|NUMBER &#x2192; BOOLEAN|Convert any number type into boolean type. If number is positive then the returned value will be `true`, else if numer is negative then the returned value will be `false`|
|STRING &#x2192; NUMBER|Convert any number string type into number type value. If the string value is `"1.35"` then it will be converted into `1.35`.|
|STRING &#x2192; STRING|Without effect.|
|STRING &#x2192; BOOLEAN|Convert literal boolean string type into boolean type. If string is "true" then the returned value will be `true`, else if string values is "false" then the returned value will be `false`. |
|BOOLEAN &#x2192; NUMBER|Convert any boolean type into integer number type. If boolean is `true` then the returned value will be `1` else if boolean values is `false` then the returned value will be `0`.|
|BOOLEAN &#x2192; STRING|Convert any boolean type into string type. If boolean value is `true ` then the returned value will be `"true"`, else if boolean value is `false` then the returned value will be `"false"`.|
|BOOLEAN &#x2192; BOOLEAN|Without effect.|


### <a name="arithmeticMapper"></a> ArithmeticMapper [ [Top](#index) ]

The Arithmetic mapper allows to evaluate math expressions over the json fields.

```json
         {
          "name": "myArithmeticMapper",
          "className": "io.wizzie.ks.normalizer.funcs.impl.ArithmeticMapper",
          "properties": {
            "equations": [
              {
                "dimensions": ["field1", "field2"],
                "equation": "field1+field2",
                "as": "sum"
              },
              {
                "dimensions": [
                  "field1",
                  "field3"
                ],
                "equation": "field1-field3",
                "as": "subtract"
              }
            ]
          }
        }
```

This mapper has one property:

* `equations`: The equations that are going to be evaluated. The equations are maps containing this three values:

    * `dimensions`: The dimensions that are going to be used at the equation. They are used to map the incoming json values to the solver.
    * `equation`: The equation that is going to be evaluated
    * `as`: The key of the field that will be used to insert the equation result.

For the function defined above, if we have:

**Input**:

```json
{"timestamp": 1477379967, "field1": 3, "field2": 4}
```

You will get as output:

**Output:**

```json
{"timestamp": 1477379967, "field1": 3, "field2": 4, "sum": 7.0}
```

The second equation `field1-field3` is not evaluated because the field `field3` is not contained at the input json.

If the input is:

```json
{"timestamp": 1477379967, "field1": 3, "field2": 4, "field3": 1}
```

The output would be:

```json
{"timestamp": 1477379967, "field1": 3, "field2": 4, "field3": 1, "sum": 7.0, "subtract": 2.0}
```

This function supports long, integer and double for input formats and it outputs double values.

#### Supported operations and library used

The library used for doing arithmetic operations is: [parsii](https://github.com/scireum/parsii) .
You can check the supported operations inspecting the java classes of this project.

Some of the operations supported are:

|Operation|Arguments|Example| Explanation |
|--------|---------|---------|---------|
|+| none | a+b | add values |
|-| none | a-b | subtract values |
|/| none | a/b | divide values |
|*| none | a*b | multiply values |
|sqrt| double a | sqrt(a) | square root value |
|floor| double a | floor(a) | select integer part of the number |
|sin| double a | sin(a) | sine of a |
|cos| double a | cos(a) | cosine of a |
|tan| double a | tan(a) | tangent of a |
|round| double a | round(a) | round to the closest integer |
|ceil| double a | ceil(a) | select next integer |
|pow| double a, double b | pow(a,b) | a^b |
|exp| double a | exp(a) | 10^a |
|log| double a | log(a) | ln(a) |
|log10| double a | log10(a) | log10(a) |
|min| double a, double b | min(a,b) | min value of a and b |
|max| double a, double b | max(a,b) | max value of a and b |
|random| double a | random(a) | a random number between 0 and a |
|toDegrees| double a | toDegrees(a) | a to degrees |
|toRadians| double a | toRadians(a) | a to radians |

### <a name="simpleArrayMapper"></a> SimpleArrayMapper [ [Top](#index) ]

The SimpleArrayMapper is a function that allows us to create new dimensions from an ArrayList.

```json
  {
    "name":"myArrayMapper",
    "className":"io.wizzie.ks.normalizer.funcs.impl.SimpleArrayMapper",
    "properties": {
      "dimension": "dimArray",
      "dimensionToIndex":{"a": 0, "b": 1, "c": 2},
      "deleteDimension": true
    }
  }
```

This mapper has some properties:

* `dimensionToIndex` : On this property you define the dimensions you want to create based on array index.
* `dimension` : The array list dimension to process.
* `deleteDimension` : If you want to remove the source dimension. Default: true.

Example:

* Input:

```json
{"timestamp": 123456789, "dimArray":["ABC", 123, "CDX"]}
```

* Output:

```json
{"timestamp": 123456789, "a":"ABC", "b":123, "c":"CDX"}
```

### <a name="renameMapper"></a> RenameMapper [ [Top](#index) ]

The RenameMapper is a function that allows us to rename fields from JSON Object.

```json
{
  "name":"myMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.RenameMapper",
  "properties": {
    "maps": [
      {"dimPath":["C"], "as":"X"},
      {"dimPath":["Z"], "as":"Q"}
    ]
  }
}
```

The RenameMapper has one property that is called `maps` on this property you define the fields that you want to rename:

```json
{
  "C": "MyValue",
  "D": "A",
  "Z": "MyOtherValue",
  "timestamp": 123456788
}
```

If we use this message using the RenameMapper that is defined on the above example, we get this output:

```json
{"X":"MyValue", "Q":"MyOtherValue", "D":"A", "timestamp": 123456788}
```

### <a name="arrayDecompositionMapper"></a> ArrayDecompositionMapper [ [Top](#index) ]

The ArrayDecompositionMapper is a function that allows us to map an array to a specific dimensions:

```json
{
  "name":"arrayDecompositionMapper",
  "className":"io.wizzie.ks.normalizer.funcs.impl.ArrayDecompositionMapper",
  "properties": {
    "dimensionToBuild": ["dim1", "dim2", "dim3"],
    "dimension": "array-dim",
    "delete_dimension": true
   }
}
```  

The ArrayDecompositionMapper has three properties:

 * `dimension`: The dimension that indicate the array that we want to map.
 * `dimensionToBuild`: The output dimension that the mapper generates using the dimension array.
 * `delete_dimension`: Option to delete the dimension or not.

If we have this json message:

```json
{
  "dimension":"VALUE-1",
  "otherDimension":"VALUE-2",
  "array-dim": ["A", "B", "C"]
}
```

If we use this message using the ArrayDecompositionMapper that is defined on the above example, we get this output:

```json
{
  "dimension":"VALUE-1",
  "otherDimension":"VALUE-2",
  "dim1":"A",
  "dim2":"B",
  "dim3":"C"
}
```
