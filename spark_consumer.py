'''
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("ChurnAnalysis") \
    .config("spark.jars", r"C:\spark\spark-3.2.4-bin-hadoop2.7\jars\mysql-connector-j-8.2.0.jar") \
    .getOrCreate()

topic = 'mytopic'

kafka_params = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": "topic", "startingOffsets": "earliest"}


json_schema = StructType().add("Churn", StringType()) \
                          .add("AccountWeeks", StringType()) \
                          .add("ContractRenewal", StringType()) \
                          .add("DataPlan", StringType()) \
                          .add("CustServCalls", StringType()) \
                          .add("DataUsage", StringType()) \
                          .add("DayMins", StringType()) \
                          .add("DayCalls", StringType()) \
                          .add("MonthlyCharge", StringType()) \
                          .add("OverageFee", StringType()) \
                          .add("RoamMins", StringType())

kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mytopic") \
    .option("startingOffsets", "earliest") \
    .load()

df = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", json_schema).alias("data")) \
    .select("data.*")

# Perform aggregations and transformations
result = df.groupBy("Churn", "AccountWeeks", "ContractRenewal", "DataPlan", "CustServCalls") \
    .agg(sum("DataUsage").alias("total_data_usage"),
         avg("DayMins").alias("avg_day_mins"),
         avg("MonthlyCharge").alias("avg_monthly_charge"))

console_query = result \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Write the results to MySQL using foreachBatch
def write_to_mysql(df, epoch_id):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:mysql://localhost:3306/big-data") \
      .option("dbtable", "churn_analysis_results") \
      .option("user", "root") \
      .option("password", "") \
      .mode("append") \
      .save()

# Use foreachBatch to write micro-batches to MySQL
mysql_query = result \
    .writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("update") \
    .start()

console_query.awaitTermination()
mysql_query.awaitTermination()




'''














#--------------------------------------------------------------------


















from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("ChurnAnalysis") \
    .config("spark.jars", r"C:\spark\spark-3.2.4-bin-hadoop2.7\jars\mysql-connector-j-8.2.0.jar") \
    .getOrCreate()

topic = 'mytopic'

kafka_params = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": "topic", "startingOffsets": "earliest"}

# Define the schema for the Kafka stream
json_schema = StructType().add("Churn", StringType()) \
                          .add("AccountWeeks", StringType()) \
                          .add("ContractRenewal", StringType()) \
                          .add("DataPlan", StringType()) \
                          .add("CustServCalls", StringType()) \
                          .add("DataUsage", StringType()) \
                          .add("DayMins", StringType()) \
                          .add("DayCalls", StringType()) \
                          .add("MonthlyCharge", StringType()) \
                          .add("OverageFee", StringType()) \
                          .add("RoamMins", StringType())

# Read data from Kafka stream
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mytopic") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract data from Kafka message value
df = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", json_schema).alias("data")) \
    .select("data.*")

# Perform aggregations and transformations
result1 = df.groupBy("Churn", "AccountWeeks", "ContractRenewal", "DataPlan", "CustServCalls") \
    .agg(sum("DataUsage").alias("total_data_usage"),
         avg("DayMins").alias("avg_day_mins"),
         avg("MonthlyCharge").alias("avg_monthly_charge"))

result2 = df.groupBy("Churn", "AccountWeeks", "ContractRenewal", "DataPlan", "CustServCalls") \
    .agg(
        sum("DayCalls").alias("total_calls"),
        avg("DayCalls").alias("avg_day_calls")
    )

# Write the results to the console
console_query1 = result1 \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

console_query2 = result2 \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Write the results to MySQL using foreachBatch
def write_to_mysql(df, epoch_id, table_name):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:mysql://localhost:3306/our_database") \
      .option("dbtable", table_name) \
      .option("user", "root") \
      .option("password", "") \
      .mode("append") \
      .save()

# Use foreachBatch to write micro-batches to MySQL
mysql_query1 = result1 \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, "churn_analysis_results1")) \
    .outputMode("update") \
    .start()

mysql_query2 = result2 \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, "churn_analysis_results2")) \
    .outputMode("update") \
    .start()

console_query1.awaitTermination()
console_query2.awaitTermination()
mysql_query1.awaitTermination()
mysql_query2.awaitTermination()














