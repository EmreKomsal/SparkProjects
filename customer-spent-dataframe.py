from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder.appName("CustomerSpent").getOrCreate()

schema = StructType([ \
                        StructField("customerID", StringType(), True), \
                        StructField("itemID", StringType(), True), \
                        StructField("amountSpent", FloatType(), True)])

df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()

# Aggregate by customerID
customerSpent = df.groupBy("customerID").sum("amountSpent")
customerSpent.show()

# Sort by amountSpent
customerSpentSorted = customerSpent.sort("sum(amountSpent)", ascending=False)
customerSpentSorted.show(customerSpentSorted.count())

