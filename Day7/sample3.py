from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import os
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("DeltaLogAnatomy") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Load employee JSON
emp_df = spark.read.json("data/emp.json")

# Load notes from text file
notes_df_raw = spark.read.text("data/emp_notes.txt")

# Split each line into id and content
notes_df = notes_df_raw.withColumn("id", split(col("value"), "\\|")[0].cast("int")) \
                       .withColumn("content", split(col("value"), "\\|")[1]) \
                       .drop("value")

# Join JSON + text data on id
final_df = emp_df.join(notes_df, on="id", how="inner")

# Show final output
final_df.select("id", "name", "content").show(truncate=False)