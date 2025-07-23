from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import random
import time

spark = SparkSession.builder.appName("BroadcastVsNormalJoinPerformance").getOrCreate()

products = [("P01", "Laptop"), ("P02", "Smartphone"), ("P03", "Tablet"), ("P04", "Monitor"), ("P05", "Keyboard")]
productSchema = ["product_id", "product_name"]
product_df = spark.createDataFrame(products, productSchema)

product_ids = [p[0] for p in products]
sales = [("Customer" + str(i), random.choice(product_ids), random.randint(100, 2000)) for i in range(1_000_000)]
salesSchema = ["customer_name", "product_id", "amount"]
sales_df = spark.createDataFrame(sales, salesSchema)


def time_it(label, func):
    print(f"\n {label}:")
    start = time.time()
    func().show(5)
    print(f"Time taken: {time.time() - start:.4f} seconds")

time_it("Normal Join", lambda: sales_df.join(product_df, "product_id").select("customer_name", "product_name", "amount"))

time_it("Broadcast Join", lambda: sales_df.join(broadcast(product_df), "product_id").select("customer_name", "product_name", "amount"))
