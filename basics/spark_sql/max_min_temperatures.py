from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinMaxTemperatures").getOrCreate()

schema = StructType([ \
                    StructField("stationID", StringType(), True), \
                    StructField("date", IntegerType(), True), \
                    StructField("measure_type", StringType(), True), \
                    StructField("temperature", FloatType(), True)])

dataFrame: DataFrame = spark.read.schema(schema).csv("./data/1800.csv")
dataFrame.createOrReplaceTempView("temperatures_view")

minTemps = spark.sql('SELECT stationID, ROUND(MIN(temperature)) AS min_c_temperature ' +
                    'FROM temperatures_view WHERE measure_type="TMIN" GROUP BY stationID SORT BY min_c_temperature')

maxTemps = spark.sql('SELECT stationID, ROUND(MAX(temperature)) AS max_c_temperature ' +
                    'FROM temperatures_view WHERE measure_type="TMAX" GROUP BY stationID SORT BY max_c_temperature')

minTemps.show()
maxTemps.show()

spark.stop()