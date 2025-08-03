from pyspark.sql import SparkSession
import time

spark = SparkSession.builder\
        .appName("DummyStream")\
        .master("local[*]")\
        .getOrCreate()

df = spark.readStream.format("cloudfiles").option("cloudFiles.format","csv").load("customers-100.csv")

df.writeStream.format("console").outputMode("append").option("truncate",False).start()