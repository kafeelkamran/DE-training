from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, expr
from delta.tables import DeltaTable
import os

# Init Spark
spark = SparkSession.builder \
    .appName("ShopFast SCD Type 2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

delta_path = "./delta"
sat_customer_path = f"{delta_path}/scd_sat_customer"

print("Creating SCD-enabled Delta table...")

initial_data = spark.createDataFrame([
    ("c001", "Alice", "NYC", "Gold"),
    ("c002", "Bob", "LA", "Silver"),
    ("c003", "Charlie", "Chicago", "Bronze"),
    ("c004", "Daisy", "Houston", "Silver")
], ["customer_hk", "name", "city", "loyalty_status"])


initial_data = initial_data \
    .withColumn("start_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("record_source", lit("initial_load"))

initial_data.write.format("delta").mode("overwrite").save(sat_customer_path)

print("Initial SCD Delta table written.\n")

print("Loading new incoming data (Bob moved to SF)...")

incoming_data = spark.createDataFrame([
    ("c002", "Bob", "San Diego", "Gold"),       # Loyalty and city change
    ("c003", "Charlie", "Chicago", "Silver"),   # Loyalty level changed
    ("c004", "Daisy", "Houston", "Silver")      # No actual change — should not trigger insert
], ["customer_hk", "name", "city", "loyalty_status"])


incoming_data = incoming_data \
    .withColumn("start_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("record_source", lit("update_batch"))

print("Merging with SCD Type 2 logic...")

delta_table = DeltaTable.forPath(spark, sat_customer_path)

# Join on business key and detect changes
merge_condition = "target.customer_hk = source.customer_hk AND target.is_current = true"

delta_table.alias("target").merge(
    incoming_data.alias("source"),
    merge_condition
).whenMatchedUpdate(condition="""
    target.city <> source.city OR
    target.loyalty_status <> source.loyalty_status
""", set={
    "is_current": lit(False),
    "end_date": current_timestamp()
}).whenNotMatchedInsert(values={
    "customer_hk": "source.customer_hk",
    "name": "source.name",
    "city": "source.city",
    "loyalty_status": "source.loyalty_status",
    "start_date": "source.start_date",
    "end_date": "source.end_date",
    "is_current": "source.is_current",
    "load_timestamp": "source.load_timestamp",
    "record_source": "source.record_source"
}).execute()

print("SCD Type 2 merge completed.\n")

result_df = spark.read.format("delta").load(sat_customer_path)
result_df.orderBy("customer_hk", "start_date").show(truncate=False)
