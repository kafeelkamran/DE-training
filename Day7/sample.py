# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# import os
# from delta.tables import DeltaTable

# #step1: Start Spark Session with Delta Support
# spark = SparkSession.builder\
#     .appName("DeltaLogAnatomy")\
#     .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")\
#     .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")\
#     .config("spark.databricks.delta.retentionDuration.enabled","false")\
#     .getOrCreate()

# #step2: Create initial DataFrame
# data  = [(1,"Alice"),(2,"bob"),(3,"Carol")]
# df = spark.createDataFrame(data,["id","name"])

# #step3: save Delta tables
# delta_path = "output/path/delta_table"
# df.write.format("delta").mode("overwrite").save(delta_path)

# #step4:
# df2 = spark.createDataFrame([(4,"David"),(5,"Eva")],["id","name"])
# df2.write.format("delta").mode("append").save(delta_path)

# #step5: Read and show the final result
# print("Delta Table content:")
# spark.read.format("delta").load(delta_path).show()

# #step6: Vaccum to force chckpoint generation(optional)
# #spark.sql(f"VACUUM delta.'{delta_path}' RETAIN 0 HOURS")

# DeltaTable.forPath(spark, delta_path).vacuum(0.0)

# #step7: Inspect _delta_log contents
# log_path = os.path.join(delta_path,"_delta_log")
# print("files inside _delta_log:")
# print(os.listdir(log_path))



from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("DeltaLogAnatomy") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

#Create DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
df = spark.createDataFrame(data, ["id", "name"])

delta_path = "outputs/path/delta_table"
df.write.format("delta").mode("overwrite").save(delta_path)

df2 = spark.createDataFrame([(4, "David"), (5, "Eva")], ["id", "name"])
df2.write.format("delta").mode("append").save(delta_path)

print("Delta Table Content:")
spark.read.format("delta").load(delta_path).show()

DeltaTable.forPath(spark, delta_path).vacuum(0.0)

log_path = os.path.join(delta_path, "_delta_log")
print("Files inside _delta_log:")
print(os.listdir(log_path))