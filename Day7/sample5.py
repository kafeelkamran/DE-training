from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import StringType
import random
import pandas as pd
import os
import time
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("ZOrderDemo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()


regions = ['US', 'EU', 'IN', 'AU', 'CA']
data = {
    "id": range(1, 5001),
    "name": [f"Person_{i}" for i in range(1, 5001)],
    "region": [random.choice(regions) for _ in range(5000)],
    "sales": [random.randint(100, 10000) for _ in range(5000)]
}

df = spark.createDataFrame(pd.DataFrame(data))
df.write.format("delta").mode("overwrite").save("delta_table/zorder_demo")

spark.sql("DROP TABLE IF EXISTS zorder_demo")
spark.sql("""
    CREATE TABLE zorder_demo
    USING DELTA
    LOCATION 'delta_table/zorder_demo'
""")


start = time.time()
spark.sql("SELECT * FROM zorder_demo WHERE region = 'US'").count()
end = time.time()
print("Execution Time BEFORE ZORDER: {:.2f} seconds".format(end - start))

spark.sql("OPTIMIZE zorder_demo ZORDER BY (region)")


start = time.time()
spark.sql("SELECT * FROM zorder_demo WHERE region = 'US'").count()
end = time.time()
print("Execution Time AFTER ZORDER: {:.2f} seconds".format(end - start))
