from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract

# Start Spark session
spark = SparkSession.builder \
    .appName("CV Mapping to Delta") \
    .master("local[*]") \
    .getOrCreate()

# Path to JSON and CV folder
json_path = "data/emp.json"
cv_path = "data/path/files/cvs/*.txt"

# Step 1: Read employee JSON
emp_df = spark.read.json(json_path)

# Step 2: Read CV text files
cv_df = spark.read.text(cv_path) \
    .withColumn("filename", input_file_name()) \
    .withColumn("empName", regexp_extract("filename", r"\/([a-z]+)[0-9]+cv", 1)) \
    .withColumn("id", regexp_extract("filename", r"([0-9]+)cv-[0-9]+\.txt", 1).cast("int")) \
    .withColumnRenamed("value", "cv_text") \
    .select("id", "empName", "cv_text")

# Step 3: Join with emp_df to get final output
final_df = emp_df.join(cv_df, on=["id", "empName"], how="inner")

# Step 4: Show result
final_df.show(truncate=False)

# Step 5: Write to Delta (optional)
final_df.write.format("delta").mode("overwrite").save("data/path/emp_cv_delta")
