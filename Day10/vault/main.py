from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, col, row_number
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("DataVaultPipeline") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


customers_path = "data/customer.csv"
products_path = "data/product.csv"
purchases_path = "data/purchase.csv"


customers_df = spark.read.option("header", True).csv(customers_path)
products_df = spark.read.option("header", True).csv(products_path)
purchases_df = spark.read.option("header", True).csv(purchases_path)


hub_customer = customers_df.select("customer_id") \
    .withColumn("customer_hk", sha2(col("customer_id"), 256)) \
    .withColumn("load_date", current_timestamp()) \
    .dropDuplicates(["customer_hk"])


hub_customer.write.format("delta").mode("overwrite").save("delta/hub_customer")


hub_product = products_df.select("product_sku") \
    .withColumn("product_hk", sha2(col("product_sku"), 256)) \
    .withColumn("load_date", current_timestamp()) \
    .dropDuplicates(["product_hk"])

hub_product.write.format("delta").mode("overwrite").save("delta/hub_product")


link_purchase = purchases_df.select("customer_id", "product_sku", "purchase_date") \
    .withColumn("link_purchase_hk", sha2(concat_ws("||", "customer_id", "product_sku", "purchase_date"), 256)) \
    .withColumn("customer_hk", sha2(col("customer_id"), 256)) \
    .withColumn("product_hk", sha2(col("product_sku"), 256)) \
    .withColumn("load_date", current_timestamp()) \
    .dropDuplicates(["link_purchase_hk"])


sat_customer = customers_df \
    .withColumn("customer_hk", sha2(col("customer_id"), 256)) \
    .withColumn("load_date", current_timestamp()) \
    .dropDuplicates(["customer_hk", "name", "address", "contact"])


sat_product = products_df \
    .withColumn("product_hk", sha2(col("product_sku"), 256)) \
    .withColumn("load_date", current_timestamp()) \
    .dropDuplicates(["product_hk", "product_name", "category", "price"])


sat_purchase = purchases_df \
    .withColumn("link_purchase_hk", sha2(concat_ws("||", "customer_id", "product_sku", "purchase_date"), 256)) \
    .withColumn("load_date", current_timestamp()) \
    .dropDuplicates(["link_purchase_hk", "quantity", "sales_amount"])


window_customer = Window.partitionBy("customer_hk").orderBy(col("load_date").desc())
pit_customer = sat_customer.withColumn("row_num", row_number().over(window_customer)) \
    .filter(col("row_num") == 1).drop("row_num")


window_product = Window.partitionBy("product_hk").orderBy(col("load_date").desc())
pit_product = sat_product.withColumn("row_num", row_number().over(window_product)) \
    .filter(col("row_num") == 1).drop("row_num")


window_purchase = Window.partitionBy("link_purchase_hk").orderBy(col("load_date").desc())
pit_purchase = sat_purchase.withColumn("row_num", row_number().over(window_purchase)) \
    .filter(col("row_num") == 1).drop("row_num")


dim_customer = hub_customer.alias("hub") \
    .join(pit_customer.alias("sat"), "customer_hk") \
    .select("hub.customer_id", "sat.name", "sat.address", "sat.contact")


dim_product = hub_product.alias("hub") \
    .join(pit_product.alias("sat"), "product_hk") \
    .select("hub.product_sku", "sat.product_name", "sat.category", "sat.price")


fact_purchase = link_purchase.alias("link") \
    .join(pit_purchase.alias("sat"), "link_purchase_hk") \
    .join(dim_customer.alias("dc"), "customer_id") \
    .join(dim_product.alias("dp"), "product_sku") \
    .select(
        "link.customer_id", "dc.name", "link.product_sku", "dp.product_name",
        "link.purchase_date", "sat.quantity", "sat.sales_amount"
    )


def validate_hash_collisions(df, hash_col):
    return df.groupBy(hash_col).count().filter("count > 1")

print("Validating Hash Key Collisions...\n")
for name, df, hk in [
    ("Hub_Customer", hub_customer, "customer_hk"),
    ("Hub_Product", hub_product, "product_hk"),
    ("Link_Purchase", link_purchase, "link_purchase_hk")
]:
    dupes = validate_hash_collisions(df, hk)
    if dupes.count() > 0:
        print(f"Collision found in {name}:")
        dupes.show()
    else:
        print(f"No collisions found in {name}.")


print("\n Dimension - Customer")
dim_customer.show(truncate=False)

print("\n Dimension - Product")
dim_product.show(truncate=False)

print("\n Fact Table - Purchases")
fact_purchase.show(truncate=False)

spark.stop()
