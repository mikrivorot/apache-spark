# to find where spark is installed
import findspark
findspark.init()
from pyspark.sql import Row, SparkSession, functions
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# userID,name,age,friends
# 0,Will,33,385
# 1,Jean-Luc,26,2
friendsAsDataFrame = spark.read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv("./data/fakefriends-header.csv")
    

friendsAsDataFrame = friendsAsDataFrame.select('age', 'friends')

# groupBy("age"): Group people by age (e.g. 31 years: 192, 15, 481, 340, 172, 109, 394 and 435 friends)
# count(): Count grouped elements (e.g. 8 persons are 31 years old)
friendsAsDataFrame.groupBy("age").count().show()

# groupBy("age"): Group people by age (e.g. 31 years: 192, 15, 481, 340, 172, 109, 394 and 435 friends)
# count(): Calculate (e.g. (192 + 15 + 481 + 340 + 172 + 109 + 394 + 435) / 8) = 267.25)
friendsAsDataFrame.groupBy("age").avg('friends').sort("age").show();

friendsAsDataFrame\
    .groupBy("age")\
    .agg(functions.round(functions.avg("friends"), 2))\
    .alias("averageNumberOfFriends")\
    .sort("age")\
    .show();
    
# why we should stop? we should stop a server?
spark.stop()