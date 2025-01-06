from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeEuclideanSimilarity(data: DataFrame) -> DataFrame:
    # Compute squared differences
    
    # +------+------+-------+-------+------------------+                              
    # |movie1|movie2|rating1|rating2|squared_difference|
    # +------+------+-------+-------+------------------+
    # |   242|   269|      3|      3|               0.0|
    # |   242|   845|      3|      4|               1.0|
    # |   242|  1022|      3|      4|               1.0|
    pairScores: DataFrame = data \
        .withColumn("squared_difference", (func.col("rating1") - func.col("rating2")) ** 2)

    # +------+------+-------------------+-----------------------+                     
    # |movie1|movie2|              score|numberOfPairOccurrences|
    # +------+------+-------------------+-----------------------+
    # |    51|   924|0.16952084719853724|                     15|
    # |   451|   529|0.08495938795016529|                     30|
    # |    86|   318| 0.0671481457783844|                     95|
    # |    40|   167| 0.1639607805437114|                     23|
    calculateSimilarity: DataFrame = pairScores \
        .groupBy("movie1", "movie2") \
        .agg(
            func.sum("squared_difference").alias("sum_squared_difference"),  # Sum of squared differences
            func.count("*").alias("numberOfPairOccurrences")  # Count of occurrences
        ) \
        .withColumn(
            "score", 
            1 / (1 + func.sqrt(func.col("sum_squared_difference")))  # Convert distance to similarity
        ) \
        .select("movie1", "movie2", "score", "numberOfPairOccurrences")  # Include occurrences

    return calculateSimilarity

def getMovieNameByMovieId(movieNames: DataFrame, movieId: int) -> DataFrame:
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark: SparkSession = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([ \
                            StructField("movieID", IntegerType(), True), \
                            StructField("movieTitle", StringType(), True) \
                            ])
    
moviesSchema = StructType([ \
                    StructField("userID", IntegerType(), True), \
                    StructField("movieID", IntegerType(), True), \
                    StructField("rating", IntegerType(), True), \
                    StructField("timestamp", LongType(), True)])
    
    
# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames: DataFrame = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("./data/ml-100k/u.item")

movies: DataFrame = spark.read \
    .option("sep", "\t") \
    .schema(moviesSchema) \
    .csv("./data/ml-100k/u.data")

ratings: DataFrame = movies.select("userId", "movieId", "rating") # dedicated variable required for alias later

moviePairs: DataFrame = ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
    .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))

moviePairSimilarities: DataFrame = computeEuclideanSimilarity(moviePairs) \
    .cache() # cache not really in use here, but useful when we request recommendations for multiple movies

if (len(sys.argv) > 1):
    scoreThreshold = 0.3
    movieID = int(sys.argv[1]) # e.g. 50

    filteredResults = moviePairSimilarities \
        .filter(
            ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
            (func.col("movie1") != func.col("movie2")) &  # Exclude self-pairs
            (func.col("score") >= scoreThreshold) &
            (func.col("numberOfPairOccurrences") >= 10) # Minimum 5 common raters 
        ) \
        .sort(func.col("score").desc()) \
        .take(10)

    print ("Top 10 similar movies for " + getMovieNameByMovieId(movieNames, movieID))

    for result in filteredResults:
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2
        
        print(getMovieNameByMovieId(movieNames, similarMovieID) + "\tscore: "  + str(result.score)  +  "\tnumber of shared ratings: " + str(result.numberOfPairOccurrences))
