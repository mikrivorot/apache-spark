from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeJaccardSimilarity(data, ratingThreshold):
    # only high ratings
    filteredData: DataFrame = data.filter((func.col("rating1") >= ratingThreshold) & (func.col("rating2") >= ratingThreshold))

    # Unique pairs, do not include [user,x,y] and [user,y,x]
    uniquePairs: DataFrame = filteredData.select("movie1", "movie2", "userId").distinct()

    # A∩B is the intersection (number of users who rated both movies) = count all pairs with  either movie1 or movie2
    intersectionCount: DataFrame = uniquePairs.groupBy("movie1", "movie2") \
        .agg(func.count("userId").alias("intersection"))

    # A∪B is the union (number of distinct users who rated either movie) = count only unique (both movie1 and movie2 presented in a pair)
    unionCount: DataFrame = uniquePairs.groupBy("movie1", "movie2") \
        .agg(func.countDistinct("userId").alias("union"))

    #  INNER JOIN two DataFrames
    jaccardScores: DataFrame = intersectionCount\
        .join(unionCount, on=["movie1", "movie2"], how="inner")\
        .withColumn("score", func.col("intersection") / func.col("union"))

    return jaccardScores


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
    .select(     
        func.col("ratings2.userId").alias("userId"), \
        func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))



ratingThreshold = 4
scoreThreshold = 1
minSharedRatings = 240

moviePairSimilarities: DataFrame = computeJaccardSimilarity(moviePairs, ratingThreshold) \
    .cache() # cache not really in use here, but useful when we request recommendations for multiple movies

if (len(sys.argv) > 1):
    movieID = int(sys.argv[1]) # e.g. 50 Star Wars (1977)     
    filteredResults = moviePairSimilarities \
        .filter(
            ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
            (func.col("movie1") != func.col("movie2")) &  # Exclude self-pairs
            (func.col("score") >= scoreThreshold) 
            & (func.col("intersection") >= minSharedRatings)
        ) \
        .sort(func.col("intersection").desc())
    filteredResults.show()

    print ("Top 10 similar movies for " + getMovieNameByMovieId(movieNames, movieID))

    for result in filteredResults.take(10):
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2
        
        print(getMovieNameByMovieId(movieNames, similarMovieID) + "\tscore: "  + str(result.score)  + "\tnumber of shared ratings: " + str(result.intersection) )

