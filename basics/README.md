## This folder include same tasks, but developed using three approaches: RDD, DataFrames and Spark SQL

### [Basic example](./raiting_counter.py) with explanation

#### Find where spark is installed
To solve an error 'Python: No module named ‘pyspark’ Error' install findspark
https://sparkbyexamples.com/pyspark/python-no-module-named-pyspark-error/

```python
import findspark
findspark.init()
```

#### `Collections` module in Python

The collections module in Python is part of the standard library, and it provides specialized, high-performance, and more versatile alternatives to Python's built-in data structures like lists, dictionaries, and tuples. These enhanced data types are extremely useful when you need advanced functionality that is not available with basic Python data structures.
```python
import collections
```


#### Single thread, not a cluster
```python
     SparkConf().setMaster("local")
```

#### Set Application name for a Job
```python
    ... .setAppName("RatingsHistogram") 
```


#### Create RDD
```python
lines = sc.textFile('ml-100k/u.data')
```


#### Extract data (third field for rating) into a new RDD, initial RDD is not touched
```python
# 196	242	3	881250949
ratings = lines.map(lambda x: x.split()[2])
```

#### Group and count by rating (by unique values, we have them in number 5)
```python
result = ratings.countByValue()
```


#### Sort using OrderedDict (Dictionary that Remembers Insertion Order)
```python
sortedResults = collections.OrderedDict(sorted(result.items()))
```

TODO: add description here


### Run examples
#### Min / Max tempteratures
```
python -m rdd.max_min_temperatures

python -m dataframe.max_min_temperatures

python -m spark_sql.max_min_temperatures
```