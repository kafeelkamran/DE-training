from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from hashkey_gen import generate_hash_key
import pandas as pd
import os

# Init Spark
spark = SparkSession.builder \
    .appName("ShopFast ETL Load Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

delta_path = "./delta"
os.makedirs(delta_path, exist_ok=True)

raw_customers = pd.DataFrame({
    "customer_id": ["C001", "C002", "C003"],
    "email": ["a@x.com", "b@y.com", "c@z.com"],
    "name": ["Alice", "Bob", "Charlie"],
    "city": ["NYC", "LA", "SF"]
})

raw_products = pd.DataFrame({
    "product_id": ["P001", "P002", "P003"],
    "product_name": ["Shirt", "Shoes", "Watch"],
    "price": [29.99, 59.99, 99.99],
    "category": ["Clothing", "Footwear", "Accessories"]
})

raw_orders = pd.DataFrame({
    "order_id": ["O001", "O002", "O003"],
    "customer_id": ["C001", "C002", "C003"],
    "product_id": ["P001", "P002", "P003"],
    "order_date": ["2023-01-01", "2023-01-02", "2023-01-03"]
})

# Cnvrt to Spark DaF
df_customer = spark.createDataFrame(raw_customers)
df_product = spark.createDataFrame(raw_products)
df_order = spark.createDataFrame(raw_orders)

# Add hash keys
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

generate_hash_key_udf = udf(generate_hash_key, StringType())

df_customer = df_customer.withColumn("customer_hk", generate_hash_key_udf("customer_id"))
df_product = df_product.withColumn("product_hk", generate_hash_key_udf("product_id"))
df_order = df_order.withColumn("order_hk", generate_hash_key_udf("order_id")) \
                   .withColumn("link_cp_hk", generate_hash_key_udf("customer_id", "product_id"))

# Add load metadata
df_customer = df_customer.withColumn("load_timestamp", current_timestamp()).withColumn("record_source", lit("etl_pipeline"))
df_product = df_product.withColumn("load_timestamp", current_timestamp()).withColumn("record_source", lit("etl_pipeline"))
df_order = df_order.withColumn("load_timestamp", current_timestamp()).withColumn("record_source", lit("etl_pipeline"))

# Save to Delta Vault (Hub)
df_customer.select("customer_hk", "customer_id", "email", "load_timestamp", "record_source") \
    .write.format("delta").mode("overwrite").save(f"{delta_path}/hub_customer")

df_product.select("product_hk", "product_id", "product_name", "load_timestamp", "record_source") \
    .write.format("delta").mode("overwrite").save(f"{delta_path}/hub_product")

# Save to Link
df_order.select("link_cp_hk", "customer_id", "product_id", "order_id", "order_date", "order_hk", "load_timestamp", "record_source") \
    .write.format("delta").mode("overwrite").save(f"{delta_path}/link_customer_product")

# Save Satellites
df_customer.select("customer_hk", "name", "city", "load_timestamp", "record_source") \
    .write.format("delta").mode("overwrite").save(f"{delta_path}/sat_customer")

df_product.select("product_hk", "price", "category", "load_timestamp", "record_source") \
    .write.format("delta").mode("overwrite").save(f"{delta_path}/sat_product")

print("ETL pipeline loaded Hubs, Links, Satellites into Delta Vault.")
