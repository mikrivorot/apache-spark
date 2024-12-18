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
connections: DataFrame = oneColumnTable \
    .withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id") \
    .agg(func.sum("connections") \
    .alias("connections"))
    
lessPopular: DataFrame = connections.sort(func.col("connections").asc()) # connections.agg(func.min("connections") 
minConnectionCount: int = lessPopular.first()[1]; # get value in second column

superherosWithMinConnections: DataFrame = connections.filter(func.col("connections") == minConnectionCount)

# SELECT x.name FROM  superherosWithMinConnections AS y JOIN names AS x on y.id = x.id
superherosNamesWithMinConnections: DataFrame = superherosWithMinConnections \
    .join(names, "id") \
    .select("name") \
    .withColumnRenamed("name", "less_popular_hero");
superherosNamesWithMinConnections.show()