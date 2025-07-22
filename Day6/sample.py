from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("SimpleCheck").getOrCreate()

# Get Spark context
sc = spark.sparkContext

# Create a simple RDD and count elements
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)
count = rdd.count()

print(f"Number of elements in RDD: {count}")

# Stop Spark session
spark.stop()