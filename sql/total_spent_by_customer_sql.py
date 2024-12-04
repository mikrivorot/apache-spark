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
df.createOrReplaceTempView("orders_view")

total_spent_by_customer = spark.sql('SELECT customer_id, ROUND(SUM(spent_amount), 2) AS total_spent ' + 
                                    'FROM orders_view GROUP BY customer_id ORDER BY total_spent DESC')

total_spent_by_customer.show()
spark.stop()