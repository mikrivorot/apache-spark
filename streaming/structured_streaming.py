from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, when, col, concat, format_number, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark: SparkSession = SparkSession.builder \
    .appName("LogStreamProcessor") \
    .master("local[*]") \
    .config("spark.hadoop.fs.file.impl.ignore.hidden", "true") \
    .getOrCreate()

# each line looks like 
# 239.145.62.239 [2025-01-14T16:00:25] "POST /admin" 400 1508
schema: StructType = StructType([
    StructField("ip", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("method_url", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("content_size", IntegerType(), True)
])

# defining a separator helps us to parse the file without complex regex like
# LOG_PATTERN = r'(?P<ip>[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+) \[(?P<timestamp>[^\]]+)\] "(?P<method>[A-Z]+) (?P<url>[^ ]+) HTTP/1.[0-9]" (?P<status>[0-9]+) (?P<content_size>[0-9]+)'

log_stream: DataFrame = spark.readStream \
    .format("csv") \
    .option("pathGlobFilter", "logs_????-??-??T??-??-??*.txt") \
        .option("header", "false") \
        .option("sep", " ") \
        .schema(schema) \
        .load("./streaming/logs")

admin_logs: DataFrame = log_stream.filter(col("method_url").contains("admin"))

# .withColumn("failed", when(col("status") >= 400, 1).otherwise(0)):
# +-----------+-------------------+------+---------+------+------------+------+
# |ip         |timestamp          |method|url      |status|content_size|failed|
# +-----------+-------------------+------+---------+------+------------+------+
# |192.168.1.1|2025-01-14T10:00:00|POST  |/admin   |200   |3456        |0     |
# |10.0.0.2   |2025-01-14T10:00:01|POST  |/admin   |500   |1234        |1     |
# +-----------+-------------------+------+---------+------+------------+------+
security_results = admin_logs \
    .withColumn("failed", when(col("status") >= 400, 1).otherwise(0)) \
    .groupBy('method_url') \
    .agg(
        concat(
            format_number((expr("sum(failed)") / expr("count(*)") * 100), 1),
            lit("%")
        ) \
            .alias("failed")
    )
    
# TODO: add second example 'availability_result' and 'availability_query' to do the same for /books* 500 using SQL, not DF
    
security_query = ( security_results.writeStream.outputMode("complete").format("console").queryName("counts").start() )
security_query.awaitTermination()
spark.stop()
