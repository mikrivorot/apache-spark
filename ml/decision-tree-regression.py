import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

spark: SparkSession = SparkSession.builder.appName("RealEstatePricePrediction").getOrCreate()
columns_to_use_in_prediction = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores", "Latitude", "Longitude"]
column_to_predict = "PriceOfUnitArea"

data: DataFrame = spark.read.csv("./data/realestate.csv", header=True, inferSchema=True)

# VectorAssembler is a transformer that takes multiple feature columns and combines them into a single feature vector. 
# Spark ML models (such as DecisionTreeRegressor) require all input features to be combined into one column of type Vector
# `outputCol="features"`` parameter specifies the name of the new column that will store the assembled feature vector.
assembler: VectorAssembler = VectorAssembler(
    inputCols=columns_to_use_in_prediction,
    outputCol="AssembledFeatureVector"
)

dataWithSingleFeatureVector: DataFrame = assembler.transform(data)

train_data, test_data = dataWithSingleFeatureVector.randomSplit([0.8, 0.2], seed=42)

dt = DecisionTreeRegressor(
  featuresCol="AssembledFeatureVector", 
  labelCol=column_to_predict)

start_time = time.time()
model = dt.fit(train_data)

end_time = time.time()
execution_time = end_time - start_time

predictions = model.transform(test_data)

# `RegressionEvaluator`` is used to measure how well the model is performing. 
# It takes the actual values (labelCol="PriceOfUnitArea") and compares them with the predicted values (predictionCol="prediction").
# It provides a metric (like RMSE) that helps in assessing the quality of the model.
# The metricName="rmse" specifies that we are using Root Mean Squared Error (RMSE) as the evaluation metric. 
# RMSE measures the average error in predictions and gives more weight to large errors.
# E.g. Root Mean Squared Error (RMSE): 6.928751364156523
evaluator = RegressionEvaluator(
    labelCol="PriceOfUnitArea", predictionCol="prediction", metricName="rmse"
)

# Other possible metrics you can use:
# "mae" (Mean Absolute Error) – average absolute difference between actual and predicted values.
# "mse" (Mean Squared Error) – similar to RMSE but without the square root.
# "r2" (R-squared) – measures how well the model explains variance in the data (1.0 means perfect prediction).
rmse = evaluator.evaluate(predictions)

predictions.select("PriceOfUnitArea", "prediction").show()
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Model Training Execution Time: {execution_time:.2f} seconds")

spark.stop()
