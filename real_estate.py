from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("RealEstate").getOrCreate()
    
    data = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("realestate.csv")
        
    assembler = VectorAssembler(inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]).setOutputCol("features")
    
    df = assembler.transform(data).select("PriceOfUnitArea", "features")
    
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]
    
    dtr = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")
    
    model = dtr.fit(trainingDF)
    
    fullPredictions = model.transform(testDF).cache()
    
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])
    
    predictonAndLabel = predictions.zip(labels).collect()
    
    for prediction in predictonAndLabel:
        print(prediction)
        
    mse = fullPredictions.rdd.map(lambda x: (x[0] - x[1]) ** 2).reduce(lambda x, y: x + y) / fullPredictions.count()
    
    print("Mean Squared Error: " + str(mse))
    
    spark.stop()
