from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sparkSession: SparkSession = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schemaToApplyForNewTable: StructType = StructType([ \
                    StructField("id", IntegerType(), True), \
                    StructField("name", StringType(), True)])

names: DataFrame = sparkSession.read \
    .schema(schemaToApplyForNewTable) \
    .option("sep", " ") \
    .csv("./data/Marvel-Names.txt")

# Table X with one column "values"
oneColumnTable: DataFrame = sparkSession.read.text("./data/Marvel-Graph.txt")    
connections = oneColumnTable \
    .withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

