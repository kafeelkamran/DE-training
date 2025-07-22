from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test DataFrame").master("local[*]").getOrCreate()

df = spark.read.csv("data.csv",header  = True, inferSchema = True)
finalData = df.filter("Salary > 6000")
df.show()
df.printSchema()

avg_salary = df.groupBy("Dept").avg("Salary")

avg_salary = avg_salary.withColumnRenamed("avg(Salary)", "Average_Salary")
avg_salary.show()

df.withColumn("tax",df.Salary*0.18)
df.selectExpr("Salary*0.18 as tax")