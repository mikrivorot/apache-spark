from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, when, col, concat, format_number, lit, window, to_timestamp
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

security_results: DataFrame = log_stream \
    .filter(col("method_url").contains("admin")) \
    .withColumn("failed", when(col("status") >= 400, 1).otherwise(0)) \
    .groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds"),  # Time-based window
        col("method_url")  # Group by method_url
    ) \
    .agg(
        concat(
            format_number((expr("sum(failed)") / expr("count(*)") * 100), 1),
            lit("%")
        ) \
            .alias("failed")
    ) \
        .orderBy(col("window").desc())
    
    
security_query = ( security_results.writeStream.outputMode("complete").format("console").queryName("counts").start() )
security_query.awaitTermination()
spark.stop()
