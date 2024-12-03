from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
# The content of each row is stored under a default column named value.
# +--------------------------------------------------------------------------------+
# |value                                                                           |
# +--------------------------------------------------------------------------------+
# |Self-Employment: Building an Internet Business of One                           |
# |Achieving Financial and Personal Freedom through a Lifestyle Technology Business|
# |By Frank Kane                                                                   |
# +--------------------------------------------------------------------------------+
inputDF = spark.read.text("./data/Book")
# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

wordsWithoutEmptyString.show()
# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
# wordCountsSorted.show(wordCountsSorted.count())

spark.stop()