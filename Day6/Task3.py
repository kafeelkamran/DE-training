from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder \
    .appName("Cache vs Checkpoint Pipeline") \
    .master("local[*]") \
    .getOrCreate()

# Set checkpoint directory
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints")  # or any valid HDFS/local path

activity_stats_df = spark.read.parquet("output/user_activity_summary")
activity_stats_df.show()

# Simulate a long pipeline
pipeline_df = activity_stats_df \
    .join(activity_stats_df, on="id") \
    .filter(col("total_duration_min") > 50) \
    .groupBy("name", "region") \
    .agg(
        sum("total_duration_min").alias("sum_duration"),
        avg("avg_duration_min").alias("mean_avg_duration")
    )

# Cache before sort
cached_df = pipeline_df.cache()

# Trigger caching
cached_df.count()

# Continue pipeline (sort)
sorted_cached_df = cached_df.sort(col("sum_duration").desc())
sorted_cached_df.show()
