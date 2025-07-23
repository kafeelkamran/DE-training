from pyspark.sql import SparkSession
from pyspark.sql.functions import split, trim, col

spark = SparkSession.builder.appName("Atomicity").getOrCreate()

data = [("John Doe | john@example.com | +91-99999-88888", "123 Main St, Springfield, IL, 62701")]
columns = ["ContactInfo", "FullAddress"]

df = spark.createDataFrame(data, columns)

df_split = df \
    .withColumn("FullName", trim(split("ContactInfo", "\\|")[0])) \
    .withColumn("Email", trim(split("ContactInfo", "\\|")[1])) \
    .withColumn("Phone", trim(split("ContactInfo", "\\|")[2])) \
    .withColumn("Street", trim(split("FullAddress", ",")[0])) \
    .withColumn("City", trim(split("FullAddress", ",")[1])) \
    .withColumn("State", trim(split("FullAddress", ",")[2])) \
    .withColumn("PostalCode", trim(split("FullAddress", ",")[3])) \
    .drop("ContactInfo", "FullAddress")

df_split.show()

df_split.write.format("delta").mode("overwrite").save("/tmp/atomic_customer")

result_df = spark.sql("SELECT * FROM delta.`/tmp/atomic_customer`")

result_df.show()