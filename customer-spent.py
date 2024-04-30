from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("CustomerOrders").setMaster("local")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLine)

total_by_customer = rdd.reduceByKey(lambda x, y: x + y)

total_by_customer_sort = total_by_customer.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = total_by_customer_sort.collect()

for result in results:
    custormerID = result[1]
    total = result[0]
    
    print(f"Customer {custormerID} spent {total:.2f}")