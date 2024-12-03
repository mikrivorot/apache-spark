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
friendsAsDataFrame.createOrReplaceTempView("friends_view")

# Same as friendsAsDataFrame.groupBy("age").count().show()
# Group people by age (e.g. 31 years: 192, 15, 481, 340, 172, 109, 394 and 435 friends)
# Count grouped elements (e.g. 8 persons are 31 years old)
count_persons_by_age = spark.sql("SELECT  age, COUNT(*) AS count FROM friends_view GROUP BY age")

# Same as friendsAsDataFrame.groupBy("age").avg('friends').show()
count_average_friends_by_age = spark.sql("SELECT age, AVG(friends) FROM friends_view GROUP BY age")
count_average_friends_by_age.show()

# Same as friendsAsDataFrame.groupBy("age").agg(functions.round(functions.avg("friends"), 2)).alias("averageNumberOfFriends").sort("age").show();
count_average_friends_by_age_with_round = spark.sql("""
    SELECT 
        age, 
        ROUND(AVG(friends), 2) AS averageNumberOfFriends
    FROM 
        friends_view
    GROUP BY 
        age
    ORDER BY 
        age
""")
count_average_friends_by_age_with_round.show()
    
# why we should stop? we should stop a server?
spark.stop()