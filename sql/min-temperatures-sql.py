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
df.createOrReplaceTempView("temperatures_view")

minTemps = spark.sql('SELECT stationID, ROUND(MIN(temperature) * 0.1 * (9.0 / 5.0) + 32.0, 2) AS min_f_temperature ' +
                    'FROM temperatures_view WHERE measure_type="TMIN" GROUP BY stationID SORT BY min_f_temperature')
minTemps.show()

spark.stop()