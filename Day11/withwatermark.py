from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

# Start Spark session
spark = SparkSession.builder \
    .appName("WithWatermark") \
    .master("local[*]") \
    .getOrCreate()

# Simulated input stream (rate source)
stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

#Windowed aggregation with watermark
windowed_counts = stream_df \
    .withWatermark("timestamp", "30 seconds").groupBy(window(col("timestamp"), "10 seconds")) \
    .count()

# Print streaming output to console
query = windowed_counts.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", False) \
    .option("checkpointLocation", "file:///C:/Users/kafeel.kamran/Desktop/New folder/DE training/Day11/checkpoints/with_watermark") \
    .start("/delta/read")

print("Streaming WITH watermark (30 seconds). Output below. Please wait...")

# Keep process alive
query.awaitTermination()
