### Install all required packages

```bash
python3 -m venv venv

source venv/bin/activate

brew install openjdk@17
brew install scala
brew install apache-spark

spark-sumbit test_1.py (/usr/local/Cellar/apache-spark/3.5.3/bin/spark-submit test_1.py)

python test_1.py

```

### Spark Basics
[RDD](./docs/RDD.md)

[First exmaple + explanation](./docs/QuickStart.md)

### Run examples
#### Min / Max tempteratures
```
python -m rdd.max_min_temperatures

python -m dataframe.max_min_temperatures

python -m spark_sql.max_min_temperatures
```

```
+-----------+-------------+
|  stationID|c_temperature|
+-----------+-------------+
|ITE00100554|       -148.0|
|EZE00100082|       -135.0|
+-----------+-------------+

+-----------+-------------+
|  stationID|c_temperature|
+-----------+-------------+
|ITE00100554|        323.0|
|EZE00100082|        323.0|
+-----------+-------------+
```