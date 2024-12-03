# to find where spark is installed
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram") 
sc = SparkContext(conf = conf)
sc.setLogLevel('OFF')

lines = sc.textFile('../data/ml-100k/u.data')
# 196	242	3	88125.949
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
