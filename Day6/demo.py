from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, lit, udf, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("ComplexPySparkCheck").getOrCreate()

print("✅ SparkSession started")

# Sample data with some missing values and duplicates
data = [
    (1, "Alice", 29, "F", 1000),
    (2, "Bob", None, "M", 1500),
    (3, "Charlie", 25, None, 1300),
    (4, "David", 32, "M", None),
    (5, "Eva", 29, "F", 1200),
    (6, "Frank", 29, "M", 1200),
    (7, "Grace", 40, "F", 1800),
    (8, "Alice", 29, "F", 1000),  # Duplicate row
]

columns = ["id", "name", "age", "gender", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
print("✅ DataFrame created:")
df.show()

# Drop duplicates
df = df.dropDuplicates()
print("✅ Duplicates dropped:")
df.show()

# Handle missing values: fill missing age with average age, missing gender with 'unknown', missing salary with 0
avg_age = df.select(avg(col("age"))).first()[0]
df = df.fillna({"age": avg_age, "gender": "unknown", "salary": 0})
print("✅ Missing values filled:")
df.show()

# Add a new column: salary band
def salary_band(salary):
    if salary < 1200:
        return "Low"
    elif salary < 1600:
        return "Medium"
    else:
        return "High"

salary_band_udf = udf(salary_band, StringType())
df = df.withColumn("salary_band", salary_band_udf(col("salary")))
print("✅ Salary band added:")
df.show()

# Group by gender and salary_band: count and average salary
grouped_df = df.groupBy("gender", "salary_band") \
    .agg(count("*").alias("count"), avg("salary").alias("avg_salary")) \
    .orderBy("gender", "salary_band")

print("✅ Grouped aggregation:")
grouped_df.show()

# Register as temp view and run SQL query
df.createOrReplaceTempView("employees")

sql_result = spark.sql("""
    SELECT gender, COUNT(*) AS cnt, AVG(age) AS avg_age
    FROM employees
    GROUP BY gender
    HAVING COUNT(*) > 1
    ORDER BY gender
""")

print("✅ SQL query result:")
sql_result.show()

# Window function: rank employees by salary partitioned by gender
window_spec = Window.partitionBy("gender").orderBy(col("salary").desc())

df_with_rank = df.withColumn("salary_rank", row_number().over(window_spec))
print("✅ DataFrame with window function (salary_rank):")
df_with_rank.show()

# Write the final DataFrame to local CSV
output_path = "./spark_test_output"
df_with_rank.write.mode("overwrite").option("header", True).csv(output_path)
print(f"✅ DataFrame written to CSV at {output_path}")

# Stop Spark
# spark.stop()

input()
print("✅ SparkSession stopped")
