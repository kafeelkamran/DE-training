from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import random
import time

spark = SparkSession.builder.appName("BroadcastJoinExample").getOrCreate()

regions = ["US", "EU", "ASIA", "ME", "AU"]
large_data = [(i, random.choice(regions), round(random.uniform(100.0, 10000.0), 2))
              for i in range(1, 1000001)]
large_df = spark.createDataFrame(large_data, ["sale_id", "region_code", "amount"])

region_data = [("US", "United States"), ("EU", "Europe"),
               ("ASIA", "Asia"), ("ME", "Middle East"), ("AU", "Australia")]
small_df = spark.createDataFrame(region_data, ["region_code", "region_name"])


start1 = time.time()
regular_join = large_df.join(small_df, "region_code")
regular_join.count()  # Action to trigger execution
end1 = time.time()

print(f"Regular Join Time: {end1 - start1:.2f} seconds")

start2 = time.time()
broadcast_join = large_df.join(broadcast(small_df), "region_code")
broadcast_join.count()
end2 = time.time()

print(f"Broadcast Join Time: {end2 - start2:.2f} seconds")
