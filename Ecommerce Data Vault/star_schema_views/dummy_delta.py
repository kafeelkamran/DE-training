from pyspark.sql import SparkSession
import os

# Start Spark with Delta support
spark = SparkSession.builder \
    .appName("Create Dummy Delta Tables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Base delta path relative to this script
base_delta_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../delta"))

# Create dummy data
df_fact = spark.createDataFrame([
    (1, "2024-01-01", 500.0, "CUST001", "PROD001")
], ["sale_sk", "sale_date", "amount", "customer_hk", "product_hk"])

df_pit_customer = spark.createDataFrame([
    ("CUST001", "kafeel@example.com", "Bangalore", "Gold")
], ["customer_hk", "email", "city", "loyalty_status"])

df_hub_customer = spark.createDataFrame([
    ("CUST001", "KAF123", "kafeel@example.com")
], ["customer_hk", "customer_id", "email"])

df_sat_product = spark.createDataFrame([
    ("PROD001", 99.0, "Electronics")
], ["product_hk", "price", "category"])

df_hub_product = spark.createDataFrame([
    ("PROD001", "PRD123", "Mobile Phone")
], ["product_hk", "product_id", "product_name"])

# Write all delta tables
df_fact.write.format("delta").mode("overwrite").save(os.path.join(base_delta_path, "fact_sales"))
df_pit_customer.write.format("delta").mode("overwrite").save(os.path.join(base_delta_path, "pit_customer"))
df_hub_customer.write.format("delta").mode("overwrite").save(os.path.join(base_delta_path, "hub_customer"))
df_sat_product.write.format("delta").mode("overwrite").save(os.path.join(base_delta_path, "sat_product"))
df_hub_product.write.format("delta").mode("overwrite").save(os.path.join(base_delta_path, "hub_product"))

print("Dummy Delta tables created.")
