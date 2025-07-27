from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, sha2
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ShopFast Delta Table Setup") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

delta_path = "./delta"
os.makedirs(delta_path, exist_ok=True)

print("Writing DataFrames to Delta...")

hub_customer = spark.createDataFrame([
    ("c001", "C001", "a@x.com"),
    ("c002", "C002", "b@y.com")
], ["customer_hk", "customer_id", "email"]) \
.withColumn("load_timestamp", current_timestamp()) \
.withColumn("record_source", lit("initial"))

hub_customer.write.format("delta").mode("overwrite").save(f"{delta_path}/hub_customer")

sat_customer = spark.createDataFrame([
    ("c001", "Alice", "NYC", "Gold"),
    ("c002", "Bob", "LA", "Silver")
], ["customer_hk", "name", "city", "loyalty_status"]) \
.withColumn("load_date", current_timestamp()) \
.withColumn("record_source", lit("initial")) \
.withColumn("load_timestamp", current_timestamp())

sat_customer.write.format("delta").mode("overwrite").save(f"{delta_path}/sat_customer")

print("Tables written to Delta.\n")

print("Evolving Schema...")

sat_product = spark.createDataFrame([
    ("p001", 29.99, "Clothing"),
    ("p002", 59.99, "Footwear")
], ["product_hk", "price", "category"]) \
.withColumn("load_date", current_timestamp()) \
.withColumn("record_source", lit("initial")) \
.withColumn("load_timestamp", current_timestamp()) \
.withColumn("discount_code", lit("SUMMER10"))

sat_product.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(f"{delta_path}/sat_product")

print("Schema evolved with new column in sat_product.\n")

print("GDPR masking for customer_id = 'C002'")

hub_customer_df = spark.read.format("delta").load(f"{delta_path}/hub_customer")
hub_customer_df = hub_customer_df.withColumn("email", lit(None))
hub_customer_df.write.format("delta").mode("overwrite").save(f"{delta_path}/hub_customer")

sat_customer_df = spark.read.format("delta").load(f"{delta_path}/sat_customer")
sat_customer_df = sat_customer_df.withColumn("name", lit("ANONYMIZED")) \
    .withColumn("city", lit(None)) \
    .withColumn("is_deleted", lit(True)) \
    .withColumn("deleted_at", current_timestamp())

sat_customer_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{delta_path}/sat_customer")

print("GDPR masking and soft deletion complete.\n")

print("All tasks completed using DataFrame API.")
