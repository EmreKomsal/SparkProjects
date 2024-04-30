from pyspark import SparkConf, SparkContext
import collections
import os

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram").set("spark.local.dir", "/tmp/spark-temp")
sc = SparkContext(conf = conf)

# Using a relative path based on the current directory
file_path = "ml-100k/u.data"
print(f"Attempting to load data from {file_path}")
print("Current working directory:", os.getcwd())

lines = sc.textFile(file_path)  # Load the data into an RDD
ratings = lines.map(lambda x: x.split()[2]) # Extract the third column (ratings)
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f"{key} {value}")

sc.stop()

