from pyspark.sql import SparkSession
import time

# Start Spark
spark = SparkSession.builder \
    .appName("MicroBatchStreaming") \
    .master("local[*]") \
    .getOrCreate()

df = spark.readStream.format("rate").option("rowspersecond",5).load()

jvm = spark._jvm
continuous_t = jvm.org.apache.spark.sql.streaming.Trigger.Continuous("1 second")

# Write using micro-batch mode (default)
writer = df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append")

writer._jwrite = writer._jwrite.trigger(continuous_t)

query = writer.start()
print("Running in continuous mode for 10 seconds")
time.sleep(10)
query.stop()

