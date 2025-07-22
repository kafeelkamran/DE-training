from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from delta.tables import DeltaTable

# 1. Start Spark with Delta support
spark = SparkSession.builder \
    .appName("DeltaLogAnatomy") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# 2. Paths to your CSVs and Delta table
csv1_path   = "data/user.csv"    # first batch of data
csv2_path   = "data/user_additional.csv" # second batch to append
delta_path  = "output/path/delta_table"

# 3. Load first CSV, write as Delta (overwrite)
df1 = spark.read.csv(csv1_path, header=True, inferSchema=True)
df1.write.format("delta").mode("overwrite").save(delta_path)

# 4. Load second CSV, append to the same Delta table
df2 = spark.read.csv(csv2_path, header=True, inferSchema=True)
df2.write.format("delta").mode("append").save(delta_path)

# 5. Read and display the full Delta table contents
print("Delta Table Content after append:")
spark.read.format("delta").load(delta_path).show(truncate=False)

# 6. Vacuum old files (0 retention hours) to clean up
DeltaTable.forPath(spark, delta_path).vacuum(0.0)

# 7. Inspect the _delta_log folder
log_path = os.path.join(delta_path, "_delta_log")
print("Files inside _delta_log:")
for f in sorted(os.listdir(log_path)):
    print("  ", f)