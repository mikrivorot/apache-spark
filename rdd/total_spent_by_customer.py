# customer_id, _, spent_amount
# 44,8602,37.19

import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)
input = sc.textFile("../data/customer-orders.csv")


# line: customer_id, _, spent_amount
# line: 44,8602,37.19
def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    spent_amount = float(fields[2])
    return (customer_id, spent_amount)

parsedLines = input.map(parseLine)

totalSpent = parsedLines.reduceByKey(lambda x, y: x + y)
totalSpentSortedByCustomer = totalSpent.sortByKey()
totalSpentSortedByAmountDesc = totalSpent.map(lambda x: (x[1], x[0])).sortByKey(ascending= False);


for key, value in totalSpentSortedByAmountDesc.collect():
    print(value, key)