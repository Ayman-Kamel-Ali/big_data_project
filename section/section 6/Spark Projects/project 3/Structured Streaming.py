
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()

df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

writer = df.writeStream.format('console').outputMode('append')

query = writer.start()

query.awaitTermination()

# download nmap: https://nmap.org/download#windows
# first open cmd and run this line: ncat -l -p 9999
# after that run this code with spark-submit
