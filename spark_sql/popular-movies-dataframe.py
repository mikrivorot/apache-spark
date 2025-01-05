from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# 196	242	3	881250949
# 186	302	3	891717742
# 22	377	1	878887116
# 244	51	2	880606923
schema = StructType([ \
                    StructField("userID", IntegerType(), True), \
                    StructField("movieID", IntegerType(), True), \
                    StructField("rating", IntegerType(), True), \
                    StructField("timestamp", LongType(), True)])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("./data/ml-100k/u.data")
moviesDF.createOrReplaceTempView("movies_view")

# Most popular movie = more views = more entries in the table = just count lines
topMovieIDs = spark.sql("SELECT movieID as id, COUNT(*) as views FROM movies_view GROUP BY movieID ORDER BY views DESC")
topMovieIDs.show(10)
spark.stop()
