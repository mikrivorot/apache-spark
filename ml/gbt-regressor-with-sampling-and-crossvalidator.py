import time
from pyspark.sql import SparkSession, DataFrame, functions
from pyspark.sql.functions import when, col
from pyspark.ml.regression import GBTRegressor 
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

spark: SparkSession = SparkSession.builder.appName("RealEstatePricePrediction").getOrCreate()
columns_to_use_in_prediction = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores", "Latitude", "Longitude"]
column_to_predict = "PriceOfUnitArea"
assembled_column_name = "AssembledFeatureVector"
seed=42

data: DataFrame = spark.read.csv("./data/realestate.csv", header=True, inferSchema=True)
# Define price categories based on PriceOfUnitArea
data = data.withColumn("price_category", 
    when(col("PriceOfUnitArea") < 30, "low")
    .when((col("PriceOfUnitArea") >= 30) & (functions.col("PriceOfUnitArea") < 50), "medium")
    .otherwise("high")
)
data = data.na.drop()  # Remove missing values
print(f"Number of records after dropping nulls: {data.count()}")

assembler: VectorAssembler = VectorAssembler(
    inputCols=columns_to_use_in_prediction,
    outputCol=assembled_column_name
)

dataWithSingleFeatureVector: DataFrame = assembler.transform(data)

# Define sampling fractions for each price category
# Ensures each price range (low, medium, high) is equally represented in train & test data.
# Avoids issues where rare price categories might get dropped in random sampling.
fractions = {"low": 0.8, "medium": 0.8, "high": 0.8}  # 80% training, 20% testing

# use sampleBy() to ensure each category is sampled proportionally.
train_data = dataWithSingleFeatureVector.sampleBy("price_category", fractions, seed=42)

# Create test data by selecting remaining records
test_data = dataWithSingleFeatureVector.subtract(train_data)

# No additional parameters here as we will set them in ParamGridBuilder
gbt = GBTRegressor(
    featuresCol=assembled_column_name, 
    labelCol=column_to_predict,
    seed=seed
)

# .addGrid(gbt.maxDepth, [3, 5, 7]) \  # Depth of each tree
# .addGrid(gbt.maxBins, [16, 32, 64]) \  # Number of bins for continuous features
# .addGrid(gbt.maxIter, [10, 50, 100]) \  # Number of boosting iterations
paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [3, 5, 7]) \
    .addGrid(gbt.maxBins, [16, 32, ]) \
    .addGrid(gbt.maxIter, [10, 50, 100]) \
    .build()
    
evaluator=RegressionEvaluator(labelCol="PriceOfUnitArea", metricName="rmse")

# numFolds=3 # This means Spark trains the model 5 times, 
# each time using a different test set, and then takes the average RMSE.

crossval = CrossValidator(
    estimator=gbt,
    estimatorParamMaps=paramGrid,
    evaluator = evaluator,
    numFolds=5 )


start_time = time.time()
cvModel = crossval.fit(train_data)
end_time = time.time()
execution_time = (end_time - start_time)/60

predictions = cvModel.transform(test_data)

rmse = evaluator.evaluate(predictions)
predictions.select("PriceOfUnitArea", "prediction").show()

best_model = cvModel.bestModel


print(f"Best maxDepth: {best_model.getMaxDepth()}")
print(f"Best maxBins: {best_model.getMaxBins()}")
print(f"Best maxIter: {best_model.getMaxIter()}")
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Model Training Execution Time: {execution_time:.2f} minutes")
# Best maxDepth: 3
# Best maxBins: 16
# Best maxIter: 10
# Root Mean Squared Error (RMSE): 7.159653191811454
# Model Training Execution Time: 3.60 minutes
spark.stop()
