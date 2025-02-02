from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

spark = SparkSession.builder.appName("RealEstateGBT").getOrCreate()

data = spark.read.csv("./data/realestate.csv", header=True, inferSchema=True)

columns_to_use = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]
target_column = "PriceOfUnitArea"

assembler = VectorAssembler(inputCols=columns_to_use, outputCol="features_raw")
data = assembler.transform(data)

scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")
scaler_model = scaler.fit(data)
data = scaler_model.transform(data)

# Step 5: Create Stratified Price Categories
data = data.withColumn("price_category", 
    when(col(target_column) < 30, "low")
    .when((col(target_column) >= 30) & (col(target_column) < 50), "medium")
    .otherwise("high")
)

# Step 6: Stratified Sampling (80% train, 20% test)
fractions = {"low": 0.8, "medium": 0.8, "high": 0.8}
train_data = data.sampleBy("price_category", fractions, seed=42)
test_data = data.subtract(train_data)

# Step 7: Train GBTRegressor
gbt = GBTRegressor(featuresCol="features", labelCol=target_column, seed=42)

# Step 8: Hyperparameter Tuning with ParamGridBuilder
paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [3, 5, 7]) \
    .addGrid(gbt.maxBins, [16, 32, 64]) \
    .addGrid(gbt.maxIter, [10, 50, 100]) \
    .build()

# Step 9: Cross-Validation Setup
evaluator = RegressionEvaluator(labelCol=target_column, metricName="rmse")

crossval = CrossValidator(
    estimator=gbt,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=4
)

# Step 10: Train the Model with Timing
import time
start_time = time.time()

cvModel = crossval.fit(train_data)  # Train with cross-validation

end_time = time.time()
execution_time = end_time - start_time
print(f"Model Training Execution Time: {execution_time:.2f} seconds")

predictions = cvModel.transform(test_data)
rmse = evaluator.evaluate(predictions)

predictions.select(target_column, "prediction").show()
print(f"Root Mean Squared Error (RMSE): {rmse}")

best_model = cvModel.bestModel

print(f"Best maxDepth: {best_model.getMaxDepth()}")
print(f"Best maxBins: {best_model.getMaxBins()}")
print(f"Best maxIter: {best_model.getMaxIter()}")
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Model Training Execution Time: {execution_time:.2f} minutes")

spark.stop()
