from pyspark import Broadcast
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from helpers.dict_movie_names import loadMovieNames, getLookupFunctionForDict

sparkSession: SparkSession = SparkSession.builder.appName("PopularMovies").getOrCreate()

# But where do we send it? To all nodes in a cluster?
broadcastedDictionary: Broadcast = sparkSession.sparkContext.broadcast(loadMovieNames())

schema: StructType = StructType([ \
                    StructField("userID", IntegerType(), True), \
                    StructField("movieID", IntegerType(), True), \
                    StructField("rating", IntegerType(), True), \
                    StructField("timestamp", LongType(), True)])

moviesDataFrame: DataFrame = sparkSession.read.option("sep", "\t").schema(schema).csv("./data/ml-100k/u.data")

movieCountsDataFrame: DataFrame = moviesDataFrame.groupBy("movieID").count()

lookupNameUdf = func.udf(getLookupFunctionForDict(broadcastedDictionary.value))

moviesWithNames: DataFrame = movieCountsDataFrame.withColumn("movieTitle", lookupNameUdf(func.col("movieID")))

sortedMoviesWithNames: DataFrame = moviesWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

sparkSession.stop()
