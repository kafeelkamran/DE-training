from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
import os

# Init Spark
spark = SparkSession.builder \
    .appName("PIT and Bridge Tables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

delta_path = "./delta"

print("Creating PIT table for latest customer records...")

# load sat_cust with historical records (scdtype2 style)
scd_df = spark.read.format("delta").load(f"{delta_path}/scd_sat_customer")

# use row_num to get most recent version
window_spec = Window.partitionBy("customer_hk").orderBy(scd_df["start_date"].desc())

latest_df = scd_df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

# Write PIT to Delta
pit_path = f"{delta_path}/pit_customer"
latest_df.write.format("delta").mode("overwrite").save(pit_path)

print("PIT customer snapshot created.\n")

print("Creating Bridge Table for Customer-Product relationships...")

# load raw link table and hubs
link_df = spark.read.format("delta").load(f"{delta_path}/link_customer_product")
hub_customer = spark.read.format("delta").load(f"{delta_path}/hub_customer")
hub_product = spark.read.format("delta").load(f"{delta_path}/hub_product")

# Join them to flatten business relationship
bridge_df = link_df \
    .join(hub_customer, "customer_id", "left") \
    .join(hub_product, "product_id", "left") \
    .select(
        "customer_id", "email",
        "product_id", "product_name",
        "order_id", "order_date",
        "link_cp_hk", "order_hk"
    )

bridge_path = f"{delta_path}/bridge_customer_product_order"
bridge_df.write.format("delta").mode("overwrite").save(bridge_path)

print("Bridge table created.\n")

print("PIT Table:")
spark.read.format("delta").load(pit_path).show(truncate=False)

print("Bridge Table:")
spark.read.format("delta").load(bridge_path).show(truncate=False)
