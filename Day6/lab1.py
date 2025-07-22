from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test DataFrame").master("local[*]").getOrCreate()

# df = spark.read.text("path").count() -> for count of lines
df = spark.read.text("C:\\Users\\kafeel.kamran\\Desktop\\New folder\\DE training\\Day6\\Test.txt")

print(df)

df.show(truncate=False)