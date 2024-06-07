from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext(appName="SparkCoreExample")

# Create an RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Perform a transformation and action on the RDD
squared_rdd = rdd.map(lambda x: x ** 2)
result = squared_rdd.collect()

# Print the result
for num in result:
    print(num)

# Stop the SparkContext
sc.stop()
