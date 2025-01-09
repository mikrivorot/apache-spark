# to find where spark is installed
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2]) # convert into number
    friends = int(fields[3]) # convert into number
    return (age, friends) # tuple, e.g. (33,385), (26,2), (33,471)

# 0,Will,33,385
# 1,Jean-Luc,26,2
# 2,Hugh,55,221
lines = sc.textFile("./data/fakefriends.csv")
rdd = lines.map(parseLine)

# lambda x: (x, 1): (33,385) -> (33,385,1) | (26,2) -> (26,2,1) |  (33,471) ->  (33,471, 1)
# lambda x, y: (x[0] + y[0], x[1] + y[1]) where x = numFriends and y = 1 (sum)
# reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])):   (33,385,1) and   (33,471, 1) -> (22, 856, 2)
totalsByAge = rdd \
    .mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    
    
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]) # (22, 856, 2) => (22, 856/2) => (22, 428.0000)
# `.collect()` is used to return all the elements of an RDD to the driver program as an array. 
# This function should be used with caution as it can lead to out-of-memory errors 
# if the size of the RDD is large
results = averagesByAge.collect() 
for result in results:
    print(result)
