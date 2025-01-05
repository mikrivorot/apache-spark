# TODO: replace 'computeCosineSimilarity' with another method
# TODO: try to include more parameters (not only rating?)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(data):
    # pairScores  
    # rating -0.6153846153846154 is former rating 3 before 'normalization'
    # +------+------+------+-------------------+-------------------+-------------------+-------------------+--------------------+
    # |userId|movie1|movie2|            rating1|            rating2|      pow_rating1_2|      pow_rating2_2|rating1_mult_rating2|
    # +------+------+------+-------------------+-------------------+-------------------+-------------------+--------------------+
    # |   196|   242|   269|-0.6153846153846154|-0.6153846153846154|0.37869822485207105|0.37869822485207105| 0.37869822485207105|
    # |   196|   242|   845|-0.6153846153846154| 0.3846153846153846|0.37869822485207105|0.14792899408284022|-0.23668639053254437|
    # |   196|   242|  1022|-0.6153846153846154| 0.3846153846153846|0.37869822485207105|0.14792899408284022|-0.23668639053254437|
    pairScores: DataFrame = data \
      .withColumn("pow_rating1_2", func.col("rating1") * func.col("rating1")) \
      .withColumn("pow_rating2_2", func.col("rating2") * func.col("rating2")) \
      .withColumn("rating1_mult_rating2", func.col("rating1") * func.col("rating2")) 
    
    
    # calculateSimilarity
    # +------+------+-------------------+------------------+-----------------------+  
    # |movie1|movie2|          numerator|       denominator|numberOfPairOccurrences|
    # +------+------+-------------------+------------------+-----------------------+
    # |    51|   924|  8.206702703888233|20.054045769319636|                     15|
    # |   451|   529|-15.102263612317127| 41.98602996136441|                     30|
    # |    86|   318| 14.144888681199472|105.16076159426997|                     95|
    # |    40|   167|  2.134621524791088|15.026511911777025|                     23|
    calculateSimilarity: DataFrame = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("rating1_mult_rating2")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("pow_rating1_2"))) * func.sqrt(func.sum(func.col("pow_rating2_2")))).alias("denominator"), \
        func.count(func.col("rating1_mult_rating2")).alias("numberOfPairOccurrences")
      )    

    # result
    # +------+------+--------------------+-----------------------+                    
    # |movie1|movie2|               score|numberOfPairOccurrences|
    # +------+------+--------------------+-----------------------+
    # |    51|   924| 0.40922927963211975|                     15|
    # |   451|   529| -0.3596973475752351|                     30|
    # |    86|   318| 0.13450728643230178|                     95|
    result: DataFrame = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numberOfPairOccurrences")

    # result.show()
    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

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
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("./data/ml-100k/u.item")

# Load up movie data as dataset
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("./data/ml-100k/u.data")


ratings = movies.select("userId", "movieId", "rating")

# +------+------------------+
# |userId|              base|
# +------+------------------+
# |   148|               4.0|
# |   463|2.8646616541353382|
userAverageRatings: DataFrame = ratings.groupBy("userId").agg(func.avg("rating").alias("base"))

# +------+-------+------+------------------+--------------------+
# |userId|movieId|rating|              base|     adjusted_rating|
# +------+-------+------+------------------+--------------------+
# |   196|    242|     3|3.6153846153846154| -0.6153846153846154|
# |   186|    302|     3|3.4130434782608696| -0.4130434782608696|
dataWithAdjustedRatings: DataFrame = ratings \
  .join(userAverageRatings, on="userId") \
  .withColumn("adjusted_rating", func.col("rating") - func.col("base"))


# +------+------+------+-------------------+-------------------+                  
# |userId|movie1|movie2|            rating1|            rating2|
# +------+------+------+-------------------+-------------------+
# |   196|   242|   269|-0.6153846153846154|-0.6153846153846154|
# |   196|   242|   845|-0.6153846153846154| 0.3846153846153846|
# |   196|   242|  1022|-0.6153846153846154| 0.3846153846153846|
moviePairsWithAdjustedRating: DataFrame = dataWithAdjustedRatings.alias("ratings1") \
        .join(dataWithAdjustedRatings.alias("ratings2"), 
              (func.col("ratings1.userId") == func.col("ratings2.userId")) &
              (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
        .select(
            func.col("ratings1.userId").alias("userId"), # for demo purposes
            func.col("ratings1.movieId").alias("movie1"),
            func.col("ratings2.movieId").alias("movie2"),
            func.col("ratings1.adjusted_rating").alias("rating1"),
            func.col("ratings2.adjusted_rating").alias("rating2")
        )

# moviePairsWithAdjustedRating.show()

moviePairSimilarities = computeCosineSimilarity(moviePairsWithAdjustedRating).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0 # as negative numbers are possible after 'normalization'
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        (
          # pair exist
          (func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
            
          (
            # two vectors are close enough
            func.col("score") > scoreThreshold) & 
          (
            # more than 50 people watched both movies 
            func.col("numberOfPairOccurrences") > coOccurrenceThreshold)
          )

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numberOfPairOccurrences))
        
# Top 10 similar movies for Star Wars (1977)                                      
# Empire Strikes Back, The (1980) score: 0.8262872791622538       strength: 345
# Return of the Jedi (1983)       score: 0.7281820538032594       strength: 480
# Raiders of the Lost Ark (1981)  score: 0.7142501268163446       strength: 380
# Close Shave, A (1995)   score: 0.6593791029476354       strength: 92
# 12 Angry Men (1957)     score: 0.6384721998021429       strength: 109
# Wrong Trousers, The (1993)      score: 0.6087991576299646       strength: 103
# Usual Suspects, The (1995)      score: 0.5964212951876744       strength: 223
# Wallace & Gromit: The Best of Aardman Animation (1996)  score: 0.5939777531901022       strength: 58
# L.A. Confidential (1997)        score: 0.5885224685158132       strength: 158
# Bridge on the River Kwai, The (1957)    score: 0.5675231682967397       strength: 145
        
