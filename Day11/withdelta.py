from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col
import time

# Start Spark session with Delta support
spark = SparkSession.builder \
    .appName("DeltaWriteStream") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Simulated input stream (rate source)
stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Windowed aggregation without watermark
windowed_counts = stream_df \
    .groupBy(window(col("timestamp"), "10 seconds")) \
    .count()

# Write streaming output to Delta Lake
query = windowed_counts.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "file:///C:/Users/kafeel.kamran/Desktop/New folder/DE training/Day11/checkpoints/delta") \
    .start("file:///C:/Users/kafeel.kamran/Desktop/New folder/DE training/Day11/output/delta_table")

print("Streaming to Delta Lake WITHOUT watermark. Writing to Delta table. Please wait...")

# Run the query for a fixed time (e.g., 30 seconds), then stop
duration_seconds = 30
start_time = time.time()

try:
    while query.isActive and (time.time() - start_time < duration_seconds):
        time.sleep(1)
finally:
    print("Stopping query after", duration_seconds, "seconds.")
    query.stop()
    query.awaitTermination()
