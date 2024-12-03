##


People are using DataFrames instead of RDD.

DataFrames are like big SQK tables:
- have raw objects
- can run SQL queries
- can have a schema
- can read/write to JSON (?)
- communicate with JDBC/ODBC (outside Spark)


DataFrames create SQL table from data

DataSets are mostly for Scala

What Is ODBC/JDBC? - ODBC is an SQL-based Application Programming Interface (API).

Spark exposes ODBC/JDBC server on port 10000 (if Spark is builded on laptop using Hive).

How to understand that Spark is builded on laptop using Hive?

### User defined functions (udf)

```python
def square(x):
    return x*x

spark.udf.register("square", square, IntegerType())
df = spark.sql("SELECT square(some_numeric_field) FROM table_name")
```