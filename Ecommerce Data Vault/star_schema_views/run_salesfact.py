from pyspark.sql import SparkSession
import os

# Start Spark
spark = SparkSession.builder \
    .appName("Sales Fact Star View") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Register Delta tables
delta_base = os.path.abspath(os.path.join(os.path.dirname(__file__), "../delta"))
spark.read.format("delta").load(os.path.join(delta_base, "fact_sales")).createOrReplaceTempView("fact_sales")
spark.read.format("delta").load(os.path.join(delta_base, "pit_customer")).createOrReplaceTempView("pit_customer")
spark.read.format("delta").load(os.path.join(delta_base, "hub_customer")).createOrReplaceTempView("hub_customer")
spark.read.format("delta").load(os.path.join(delta_base, "sat_product")).createOrReplaceTempView("sat_product")
spark.read.format("delta").load(os.path.join(delta_base, "hub_product")).createOrReplaceTempView("hub_product")

# Read SQL
sql_path = os.path.join(os.path.dirname(__file__), "salesfact_view.sql")
with open(sql_path, "r") as f:
    sql_query = f.read()

# Create and query the view
spark.sql(sql_query)
df = spark.sql("SELECT * FROM sales_fact_star")
df.show(truncate=False)

# Save as Parquet
output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output/star_fact_data"))
df.write.format("parquet").mode("overwrite").save(output_dir)
print(f"Data saved to: {output_dir}")
