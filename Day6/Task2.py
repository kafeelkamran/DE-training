from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder.appName("User Activity Analysis").master("local[*]").getOrCreate()

user_df = spark.read.csv("user.csv",header=True,inferSchema=True)
activity_df = spark.read.csv("activity.csv",header=True,inferSchema=True)

filtered_user_df = user_df.filter(col("age")>30)
filtered_user_df.show()

joined_df = filtered_user_df.join(activity_df,filtered_user_df.id == activity_df.user_id, "inner")
joined_df.show()

joined_with_hours_df = joined_df.selectExpr("id","name","age","region","activity_type","duration_min","duration_min/60.0 as duration_hrs")
joined_with_hours_df.show()

agg_df = joined_with_hours_df.groupBy("id","name","region").agg(
    sum("duration_min").alias("total_duration_min"),avg("duration_min").alias("avg_duration_min")
)

agg_df.write.mode("overwrite").partitionBy("region").parquet("output/user_activity_summary")