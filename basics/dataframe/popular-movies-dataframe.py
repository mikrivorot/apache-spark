from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark: SparkSession = SparkSession.builder.appName("PopularMovies").getOrCreate()

# 196	242	3	881250949
# 186	302	3	891717742
# 22	377	1	878887116
# 244	51	2	880606923
schema = StructType([ \
                    StructField("userID", IntegerType(), True), \
                    StructField("movieID", IntegerType(), True), \
                    StructField("rating", IntegerType(), True), \
                    StructField("timestamp", LongType(), True)])

moviesDF: DataFrame = spark.read.option("sep", "\t").schema(schema).csv("./data/ml-100k/u.data")

# Most popular movie = more views = more entries in the table = just count lines
topMovieIDs: DataFrame = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

topMovieIDs.show(10)

spark.stop()
