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
# Register the inputDF as a temporary view
inputDF.createOrReplaceTempView("input_view")

words = spark.sql("""
    SELECT 
        EXPLODE(SPLIT(value, '[\\\\W]+')) AS word
    FROM 
        input_view
    AS words_view
""")

words.createOrReplaceTempView("words_view")
wordsWithoutEmptyString = spark.sql("""
    SELECT 
        word
    FROM 
        words_view
    WHERE 
        word != ''
""")

wordsWithoutEmptyString.createOrReplaceTempView("words_no_empty_view")

lowercaseWords = spark.sql("""
    SELECT 
        LOWER(word) AS word
    FROM 
        words_no_empty_view
""")


lowercaseWords.createOrReplaceTempView("lowercase_words_view")

wordCounts = spark.sql("""
    SELECT 
        word, 
        COUNT(*) AS count
    FROM 
        lowercase_words_view
    GROUP BY 
        word
""")

wordCounts.createOrReplaceTempView("word_counts_view")

wordCountsSorted = spark.sql("""
    SELECT 
        word, 
        count
    FROM 
        word_counts_view
    ORDER BY 
        count ASC
""")
wordCountsSorted.show() # show only top 20
# wordCountsSorted.show(wordCountsSorted.count()) === show all
spark.stop()