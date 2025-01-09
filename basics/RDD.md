RDD = Resilient Distributed Dataset = abstraction for a giant set of data.
We can transform and apply actions

### The Spark context 
```python
sc = SparkContext(conf = conf) // starting point
```

We can create RDD object from  S3, JDBC, Cassandra, HBase, Elasticsearch, JSON, csv...

### Transforming RDDs
```python
ratings = lines.map(lambda x: x.split()[2])
```

Functions are transforming original RDD into another RDD
- `map` - input value -> output value, same amount of values, 1:1
- `flatmap` - input value -> multiple values (can be larger or smaller, e.g. 1:N or N:1)
- `filter` = filter in values by condition
- `distinct` - unique values
- `sample` - random sample (?)
- `union`, `intersections` - union/intersection of sets
- `substract`, `cartesian`


### Lambda function as a parameter
RDD methods accept function as a parameter, it can be lambda functions (just a shortcut)


### Actions
```
result = ratings.countByValue()
```

Soma basic actions:
- `collect` - is used to return all the elements of an RDD to the driver program as an array. This function should be used with caution as it can lead to out-of-memory errors if the size of the RDD is large
- `count`
- `countByValue` - how many times the value appears
- `take`
- `top`
- `reduce`

Some key/value RDD operations:
- `reduceByKey` - `rdd.reduceByKey(lambda x,y: y + x)` adds them up
- `mapValues`, `flatMapValues`

### Lazy evaluation
Nothing happened until action is called

### SQL-style joins
- `join`
- `rightOuter`
- `leftOuter`
- `cogroup`
- `substractByKey`