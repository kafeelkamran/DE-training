from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import *
import hashlib
import os
from datetime import datetime

spark = SparkSession.builder \
    .appName("ETL Data Vault Pipeline") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data_dir = "./data"
output_dir = "./output"
customer_path = f"{data_dir}/customer.csv"
product_path = f"{data_dir}/product.csv"
purchase_path = f"{data_dir}/purchase.csv"


record_source = lit("CSV_Import")
load_date = current_timestamp()
batch_id = lit("batch_001")

def hash_col(*cols):
    return sha2(concat_ws("||", *cols), 256)

customer_df = spark.read.option("header", True).csv(customer_path)
product_df = spark.read.option("header", True).csv(product_path)
purchase_df = spark.read.option("header", True).csv(purchase_path)


hub_customer = customer_df.withColumn("customer_hk", hash_col(col("customer_id"))) \
    .select("customer_hk", "customer_id") \
    .withColumn("load_date", load_date) \
    .withColumn("record_source", record_source)


hub_product = product_df.withColumn("product_hk", hash_col(col("product_sku"))) \
    .select("product_hk", "product_sku") \
    .withColumn("load_date", load_date) \
    .withColumn("record_source", record_source)


hub_purchase = purchase_df.withColumn("purchase_hk", hash_col(col("purchase_id"))) \
    .select("purchase_hk", "purchase_id") \
    .withColumn("load_date", load_date) \
    .withColumn("record_source", record_source)


link_df = purchase_df \
    .withColumn("link_hk", hash_col(col("customer_id"), col("product_sku"), col("purchase_date"))) \
    .withColumn("customer_hk", hash_col(col("customer_id"))) \
    .withColumn("product_hk", hash_col(col("product_sku"))) \
    .withColumn("purchase_hk", hash_col(col("customer_id"), col("product_sku"), col("purchase_date"))) \
    .select("link_hk", "customer_hk", "product_hk", "purchase_hk") \
    .withColumn("load_date", load_date) \
    .withColumn("record_source", record_source)


sat_customer = customer_df.withColumn("customer_hk", hash_col(col("customer_id"))) \
    .withColumn("hash_diff", hash_col(col("name"), col("email"))) \
    .select("customer_hk", "name", "email") \
    .withColumn("load_date", load_date) \
    .withColumn("record_source", record_source)


sat_product = product_df.withColumn("product_hk", hash_col(col("product_id"))) \
    .withColumn("hash_diff", hash_col(col("name"), col("category"))) \
    .select("product_hk", "name", "category") \
    .withColumn("load_date", load_date) \
    .withColumn("record_source", record_source)


sat_purchase = purchase_df \
    .withColumn("purchase_hk", hash_col(col("customer_id"), col("product_sku"), col("purchase_date"))) \
    .withColumn("hash_diff", hash_col(col("quantity"), col("sales_amount"))) \
    .select("purchase_hk", "quantity", "sales_amount", "purchase_date") \
    .withColumn("load_date", load_date) \
    .withColumn("record_source", record_source)

pit_customer = sat_customer.groupBy("customer_hk").agg(max("load_date").alias("latest_load_date"))

pit_product = sat_product.groupBy("product_hk").agg(max("load_date").alias("latest_load_date"))

pit_purchase = sat_purchase.groupBy("purchase_hk").agg(max("load_date").alias("latest_load_date"))


dim_customer = sat_customer.alias("s").join(
    pit_customer.alias("p"),
    (col("s.customer_hk") == col("p.customer_hk")) & (col("s.load_date") == col("p.latest_load_date"))
).select("s.customer_hk", "s.name", "s.email")


dim_product = sat_product.alias("s").join(
    pit_product.alias("p"),
    (col("s.product_hk") == col("p.product_hk")) & (col("s.load_date") == col("p.latest_load_date"))
).select("s.product_hk", "s.name", "s.category")


fact_purchase = link_df.alias("l") \
    .join(dim_customer.alias("dc"), "customer_hk") \
    .join(dim_product.alias("dp"), "product_hk") \
    .join(sat_purchase.alias("sp"), "purchase_hk") \
    .select("l.link_hk", "dc.name", "dp.name", "sp.quantity", "sp.price", "sp.purchase_date")


def save_df(df, folder, name):
    path = f"{output_dir}/{folder}/{name}"
    df.write.mode("overwrite").parquet(path)


save_df(hub_customer, "raw_vault", "hub_customer")
save_df(hub_product, "raw_vault", "hub_product")
save_df(hub_purchase, "raw_vault", "hub_purchase")
save_df(link_df, "raw_vault", "link_customer_product_purchase")
save_df(sat_customer, "raw_vault", "sat_customer")
save_df(sat_product, "raw_vault", "sat_product")
save_df(sat_purchase, "raw_vault", "sat_purchase")


save_df(pit_customer, "pit", "pit_customer")
save_df(pit_product, "pit", "pit_product")
save_df(pit_purchase, "pit", "pit_purchase")


save_df(dim_customer, "star_schema", "dim_customer")
save_df(dim_product, "star_schema", "dim_product")
save_df(fact_purchase, "star_schema", "fact_purchase")

print("ETL Pipeline Execution Completed!")

