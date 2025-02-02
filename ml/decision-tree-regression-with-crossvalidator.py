import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.regression import DecisionTreeRegressor 
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

spark: SparkSession = SparkSession.builder.appName("RealEstatePricePrediction").getOrCreate()
columns_to_use_in_prediction = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores", "Latitude", "Longitude"]
column_to_predict = "PriceOfUnitArea"
assembled_column_named = "AssembledFeatureVector"
seed=42

data: DataFrame = spark.read.csv("./data/realestate.csv", header=True, inferSchema=True)

data = data.na.drop()  # Remove missing values
print(f"Number of records after dropping nulls: {data.count()}")

assembler: VectorAssembler = VectorAssembler(
    inputCols=columns_to_use_in_prediction,
    outputCol=assembled_column_named
)

dataWithSingleFeatureVector: DataFrame = assembler.transform(data)

train_data, test_data = dataWithSingleFeatureVector.randomSplit([0.8, 0.2], seed=seed)

dt = DecisionTreeRegressor(
    featuresCol=assembled_column_named, 
    labelCol=column_to_predict,
    seed=seed
)

paramGrid = ParamGridBuilder() \
    .addGrid(dt.maxDepth, [3, 5, 7, 10]) \
    .addGrid(dt.minInstancesPerNode, [1, 3, 5]) \
    .addGrid(dt.maxBins, [16, 32, 64]) \
    .build()

evaluator=RegressionEvaluator(labelCol="PriceOfUnitArea", metricName="rmse")

crossval = CrossValidator(
    estimator=dt,
    estimatorParamMaps=paramGrid,
    evaluator = evaluator,
    numFolds=5
)

start_time = time.time()
cvModel = crossval.fit(train_data)
end_time = time.time()
execution_time = (end_time - start_time)/60

predictions = cvModel.transform(test_data)

rmse = evaluator.evaluate(predictions)

predictions.select("PriceOfUnitArea", "prediction").show()

best_model = cvModel.bestModel

print(f"Best maxDepth: {best_model.getMaxDepth()}")
print(f"Best minInstancesPerNode: {best_model.getMinInstancesPerNode()}")
print(f"Best maxBins: {best_model.getMaxBins()}")
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Model Training Execution Time: {execution_time:.2f} minutes")
# Best maxDepth: 3
# Best minInstancesPerNode: 5
# Best maxBins: 16
# Root Mean Squared Error (RMSE): 7.433715053695809
spark.stop()
