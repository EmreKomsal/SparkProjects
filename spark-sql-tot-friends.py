from pyspark.sql import SparkSession , Row, functions as func

spark = SparkSession.builder.appName('nlp').getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

people.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()