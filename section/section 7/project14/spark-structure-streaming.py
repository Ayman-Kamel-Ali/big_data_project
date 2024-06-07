from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import pymysql

#conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
#cursor = conn.cursor()

def insert_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "big_data"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    column1_value = row.name
    column2_value = row.age

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO test (name, age) VALUES ('{column1_value}', '{column2_value}')"
    
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("id", IntegerType()).add("name", StringType()).add("age", IntegerType())


# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test3") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \

# Select specific columns from "data"
df = df.select("data.name", "data.age")

# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(insert_into_phpmyadmin) \
    .start()

# Wait for the query to finish
query.awaitTermination()
