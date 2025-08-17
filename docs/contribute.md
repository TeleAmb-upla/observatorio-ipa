# Contributing

The following conventions are used:

## Variable Naming Conventions

- All server side variables are prefixed with 'ee_'
- Image, ImageCollection and FeatureCollections are suffixed with '_img', '_ic' and '_fc' when possible

## Function Naming Conventions

- internal functions are prefixed with '_'
- server side functions are prefixed with 'ee_'. Server side functions are those that are meant to run on Google Earth Engine Servers and are restricted in the type of runtime interaction/behavior they can have. See GEE documentation for more details. Functions with _ee_ prefix are internal functions meant to run on server side

## Nested Functions

When using GEE  .map() function, prefer declaring functions outside of other functions and passing explicit arguments over scope variables. This is to clarify the function's dependencies and improve readability.

for example, instead of this:

```python
def main_function():
    ee_collection = ee.ImageCollection('collection_id')
    def _ee_function(arg1):
        return ee.collection.filter(ee.filter.Filter.eq('property', arg1))

    ee_list = ee.ee_list.List(['id1', 'id2'])
    result = ee_list.map(_ee_function)

    return result
```

do this:

```python
def _ee_function(arg1, ee_collection):
     return ee.collection.filter(ee.filter.Filter.eq('property', arg1))

def main_function():
    ee_collection = ee.ImageCollection('collection_id')
    ee_list = ee.ee_list.List(['id1', 'id2'])
    result = ee_list.map(lambda arg1: _ee_function(arg1, ee_collection))

    return result
```

## Linting & Formatting

For better readability and maintenance of the code the following conventions were used.

- Use `black` for formatting Python code.
- Use `Pylance` for linting and type checking.
- Use type hints in variables that are a result of a GEE function call that returns 'Any'. for example:

```python
ee_filtered_collection_ic: ee.ee_collection.ImageCollection = ee.ImageCollection('collection_id').filter(ee.Filter.eq('property', 'value'))
```

- Use full path for GEE functions for example, use ee.image.ImageCollection instead of ee.ImageCollection. As of earthengine-api v1.15.24 some exports in short format are not properly detected by Pylance. This also help keep the code readable indicating what output is expected from the functions.
