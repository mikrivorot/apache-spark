from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computePearsonSimilarity(data):
    # Prepare values for Pearson formula
    # +------+------+-------+-------+---+---+---+---+---+                             
    # |movie1|movie2|rating1|rating2|  x|  y| xy| x2| y2|
    # +------+------+-------+-------+---+---+---+---+---+
    # |   242|   269|      3|      3|  3|  3|  9|  9|  9|
    # |   242|   845|      3|      4|  3|  4| 12|  9| 16|
    # |   242|  1022|      3|      4|  3|  4| 12|  9| 16|
    # |   242|   762|      3|      3|  3|  3|  9|  9|  9|
    pairScores: DataFrame = data \
        .withColumn("x", func.col("rating1")) \
        .withColumn("y", func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2")) \
        .withColumn("x2", func.col("rating1") * func.col("rating1")) \
        .withColumn("y2", func.col("rating2") * func.col("rating2"))
        
    # Compute sums and occurrences
    # +------+------+-----+-----+------+------+------+---+                            
    # |movie1|movie2|sum_x|sum_y|sum_xy|sum_x2|sum_y2|  n|
    # +------+------+-----+-----+------+------+------+---+
    # |    51|   924|   56|   50|   197|   228|   190| 15|
    # |   451|   529|   95|  115|   357|   353|   477| 30|
    # |    86|   318|  365|  434|  1669|  1499|  2032| 95|
    # |    40|   167|   74|   74|   241|   256|   252| 23|
    calculateSimilarity: DataFrame = pairScores \
        .groupBy("movie1", "movie2") \
        .agg(
            func.sum("x").alias("sum_x"),
            func.sum("y").alias("sum_y"),
            func.sum("xy").alias("sum_xy"),
            func.sum("x2").alias("sum_x2"),
            func.sum("y2").alias("sum_y2"),
            func.count("xy").alias("n")
        )

    # Calculate Pearson correlation
    # +------+------+--------------------+---+
    # |movie1|movie2|               score|  n|
    # +------+------+--------------------+---+
    # |    51|   924| 0.49163014676405214| 15|
    # |   451|   529|-0.16499334217557077| 30|
    # |    86|   318|0.022112518967021277| 95|
    # |    40|   167|  0.1845232968689283| 23|
    result: DataFrame = calculateSimilarity \
        .withColumn(
            "numerator", 
            func.col("n") * func.col("sum_xy") - func.col("sum_x") * func.col("sum_y")
        ) \
        .withColumn(
            "denominator",
            func.sqrt(
                (func.col("n") * func.col("sum_x2") - func.col("sum_x") ** 2) *
                (func.col("n") * func.col("sum_y2") - func.col("sum_y") ** 2)
            )
        ) \
        .withColumn(
            "score",
            func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator"))
            .otherwise(0)
        ) \
        .select("movie1", "movie2", "score", "n")

    return result

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

moviePairSimilarities: DataFrame = computePearsonSimilarity(moviePairs) \
    .cache() # cache not really in use here, but useful when we request recommendations for multiple movies

if (len(sys.argv) > 1):
    scoreThreshold = 0.3
    numberOfSharedRatings = 100
    movieID = int(sys.argv[1]) # e.g. 50 Star Wars (1977)     

    filteredResults = moviePairSimilarities \
        .filter(
            ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
            (func.col("movie1") != func.col("movie2")) &  # Exclude self-pairs
            (func.col("score") >= scoreThreshold) & (func.col("n") >= numberOfSharedRatings)
        ) \
        .sort(func.col("score").desc()) \
        .take(10)

    print ("Top 10 similar movies for " + getMovieNameByMovieId(movieNames, movieID))

    for result in filteredResults:
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2
        
        print(getMovieNameByMovieId(movieNames, similarMovieID) + "\tscore: "  + str(result.score) + "\tnumber of shared ratings: " + str(result.n))

