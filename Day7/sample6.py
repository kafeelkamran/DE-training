from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when
import time

# Step 1: Create SparkSession with Delta support
spark = SparkSession.builder \
    .appName("Delta Z-Order Optimization") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Step 2: Generate dummy data (1000+ rows across multiple regions)
regions = ["US", "EU", "IN", "UK", "AU"]
df = spark.range(1000).withColumn(
    "region", when((rand() < 0.2), "US")
               .when((rand() < 0.4), "EU")
               .when((rand() < 0.6), "IN")
               .when((rand() < 0.8), "UK")
               .otherwise("AU")
)

# Step 3: Write the data as a Delta table to disk and register in Hive
delta_path = "zorder_demo_delta"
df.write.format("delta").mode("overwrite").save(delta_path)

# Register the Delta table in the metastore
spark.sql("DROP TABLE IF EXISTS zorder_demo")  # Clean up if exists
spark.sql(f"CREATE TABLE zorder_demo USING DELTA LOCATION '{delta_path}'")

# Step 4: Run query before Z-Ordering
start_time = time.time()
count_before = spark.sql("SELECT * FROM zorder_demo WHERE region = 'US'").count()
end_time = time.time()
print(f"\n🔍 Count Before Z-ORDER: {count_before}, Time: {end_time - start_time:.4f} sec")

# Step 5: Run OPTIMIZE ZORDER BY region
spark.sql(f"OPTIMIZE zorder_demo ZORDER BY (region)")

# Step 6: Run query again after Z-Ordering
start_time = time.time()
count_after = spark.sql("SELECT * FROM zorder_demo WHERE region = 'US'").count()
end_time = time.time()
print(f"\n⚡ Count After Z-ORDER: {count_after}, Time: {end_time - start_time:.4f} sec")
