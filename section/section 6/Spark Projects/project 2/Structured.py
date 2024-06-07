from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SparkStructuredAPIExample").getOrCreate()

# Create a DataFrame
data = [
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 35)
]
df = spark.createDataFrame(data, ["Name", "Age"])

# Perform DataFrame transformations and actions
filtered_df = df.filter(df.Age > 30)
result = filtered_df.select("Name").collect()

# Print the result
for row in result:
    print(row.Name)

# Stop the SparkSession
spark.stop()
