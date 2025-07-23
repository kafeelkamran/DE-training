from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random
import time

spark = SparkSession.builder \
    .appName("PartitionedParquetDataGen") \
    .getOrCreate()


names = ["Alice", "Bob", "Charlie", "David", "Eve"]
years = [2022, 2023, 2024]
months = [1, 2, 3, 4, 5, 6]

data = []
for i in range(1, 1001):
    data.append((
        i,
        random.choice(names),
        round(random.uniform(100.0, 1000.0), 2),
        random.choice(years),
        random.choice(months)
    ))

columns = ["id", "name", "sales", "year", "month"]
df = spark.createDataFrame(data, columns)

# Write Parquet file with partitioning
df.write.partitionBy("year", "month").mode("overwrite").parquet("partitioned_data")
df.show()


input()
print("Partitioned data written to 'partitioned_data/' directory.")

start1 = time.time()
df1 = spark.read.parquet("parquet_non_partitioned")
filtered1 = df1.filter((col("year") == 2023) & (col("month") == 2))
count1 = filtered1.count()
end1 = time.time()
print(f"⏱ Non-partitioned Read Time: {end1 - start1:.2f} seconds — Count: {count1}")

# Load and filter partitioned
start2 = time.time()
df2 = spark.read.parquet("parquet_partitioned")
filtered2 = df2.filter((col("year") == 2023) & (col("month") == 2))
count2 = filtered2.count()
end2 = time.time()
print(f"⚡ Partitioned Read Time: {end2 - start2:.2f} seconds — Count: {count2}")