from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# Set schema
schema = StructType([ \
                    StructField("stationID", StringType(), True), \
                    StructField("date", IntegerType(), True), \
                    StructField("measure_type", StringType(), True), \
                    StructField("temperature", FloatType(), True)])

# Read the file as dataframe
df = spark.read.schema(schema).csv("./data/1800.csv")

minTempsByStationF = df \
    .filter(df.measure_type == "TMIN") \
    .select("stationID", "temperature") \
    .groupBy("stationID")\
    .min("temperature") \
    .withColumn("f_temperature", func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2)) \
    .select("stationID", "f_temperature")\
    .sort("f_temperature")

minTempsByStationF.show()
spark.stop()