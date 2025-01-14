from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark: SparkSession = SparkSession.builder \
    .appName("LogStreamProcessor") \
    .master("local[*]") \
    .config("spark.hadoop.fs.file.impl.ignore.hidden", "true") \
    .getOrCreate()

schema: StructType = StructType([
    StructField("ip", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("method_url", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("content_size", IntegerType(), True)
])

# 239.145.62.239 [2025-01-14T16:00:25] "POST /admin" 400 1508
log_stream: DataFrame = spark.readStream \
    .format("csv") \
    .option("pathGlobFilter", "logs_????-??-??T??-??-??*.txt") \
        .option("header", "false") \
        .option("sep", " ") \
        .schema(schema) \
        .load("./streaming/logs")

admin_logs: DataFrame = log_stream.filter(col("method_url").contains("admin"))

total_admin_requests: DataFrame = admin_logs.groupBy().count().alias("total_requests")
failed_admin_requests: DataFrame = admin_logs.filter(col("status") >= 400).groupBy().count().alias("failed_requests")
result = admin_logs \
    .withColumn("failed", when(col("status") >= 400, 1).otherwise(0)) \
    .groupBy() \
    .agg(
        expr("sum(failed)").alias("failed_requests"),
        expr("count(*)").alias("total_requests"),
        (expr("sum(failed)") / expr("count(*)") * 100).alias("failed_percentage")
    )
    
query = ( result.writeStream.outputMode("complete").format("console").queryName("counts").start() )
query.awaitTermination()
spark.stop()
