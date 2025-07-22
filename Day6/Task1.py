from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Start Spark session
spark = SparkSession.builder.appName("RevenueByCategory").master("local[*]").getOrCreate()

products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)


# Join products and orders on product_id
joined_df = orders_df.join(products_df, orders_df.product_id == products_df.id, "inner")

# revenue = quantity * price -> Calculate revenue per category
revenue_df = joined_df.withColumn("revenue", col("quantity") * col("price")) \
                      .groupBy("category") \
                      .sum("revenue") \
                      .withColumnRenamed("sum(revenue)", "total_revenue")

# Show final revenue per category
revenue_df.show()

input()

