from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark: SparkSession = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema: StructType = StructType([ \
                    StructField("stationID", StringType(), True), \
                    StructField("date", IntegerType(), True), \
                    StructField("measure_type", StringType(), True), \
                    StructField("temperature", FloatType(), True)])

df: DataFrame = spark.read.schema(schema).csv("./data/1800.csv")

minTempsByStation: DataFrame = df \
    .filter(df.measure_type == "TMIN") \
    .select("stationID", "temperature") \
    .groupBy("stationID")\
    .min("temperature") \
    .withColumn("c_temperature", func.col("min(temperature)")) \
    .select("stationID", "c_temperature")\
    .sort("c_temperature")

maxTempsByStation: DataFrame = df \
    .filter(df.measure_type == "TMAX") \
    .select("stationID", "temperature") \
    .groupBy("stationID")\
    .max("temperature") \
    .withColumn("c_temperature", func.col("max(temperature)")) \
    .select("stationID", "c_temperature")\
    .sort("c_temperature")

minTempsByStation.show()
maxTempsByStation.show()

spark.stop()