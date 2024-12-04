# customer_id, _, spent_amount
# 44,8602,37.19
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

import findspark
findspark.init()

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                    StructField("customer_id", IntegerType(), True), \
                    StructField("smth", IntegerType(), True), \
                    StructField("spent_amount", FloatType(), True)])

df = spark.read.schema(schema).csv("./data/customer-orders.csv")

total_spent_by_customer = df\
    .select("customer_id", "spent_amount")\
    .groupBy("customer_id")\
    .sum("spent_amount")\
    .withColumnRenamed("sum(spent_amount)", "total_spent")\
    .withColumn("total_spent", func.round("total_spent", 2))\
    .orderBy("total_spent", ascending=False)

# or shorter
# total_spent_by_customer = df\
#     .groupBy("customer_id")\
#     .agg(func.round(func.sum("spent_amount"), 2)\
#     .alias("total_spent"))\
#     .orderBy("total_spent", ascending=False)

total_spent_by_customer.show()
spark.stop()