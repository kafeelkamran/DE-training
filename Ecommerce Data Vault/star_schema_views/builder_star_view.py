from pyspark.sql import SparkSession
import os

# Init Spark
spark = SparkSession.builder \
    .appName("Star Schema Views") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# delta path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
delta_base_path = os.path.join(project_root, "delta")

required_tables = {
    "fact_sales": "fact_sales",
    "hub_customer": "hub_customer",
    "sat_product": "sat_product",
    "hub_product": "hub_product",
    "pit_customer": "pit_customer",
    "fact_inventory": "fact_inventory",
    "sat_warehouse": "sat_warehouse",
    "hub_warehouse": "hub_warehouse",
    "fact_customer": "fact_customer",
    "sat_customer_profile": "sat_customer_profile"
}

# Register views from delta tables
for view_name, folder in required_tables.items():
    delta_path = os.path.join(delta_base_path, folder)
    if os.path.exists(delta_path):
        spark.read.format("delta").load(delta_path).createOrReplaceTempView(view_name)
        print(f"Registered view: {view_name}")
    else:
        print(f"WARNING: Delta table not found at {delta_path}. Skipping view: {view_name}")

# Read and execute all .sql files in current folder
sql_folder = os.path.dirname(__file__)
for filename in os.listdir(sql_folder):
    if filename.endswith(".sql"):
        view_name = filename.replace(".sql", "")
        sql_path = os.path.join(sql_folder, filename)
        with open(sql_path, "r") as file:
            sql = file.read()

        try:
            spark.sql(sql)
            print(f"Created view: {view_name}")
        except Exception as e:
            print(f"Error creating view {view_name}: {e}")
            continue

        try:
            df = spark.sql(f"SELECT * FROM {view_name}")
            df.show(truncate=False)
            output_path = os.path.join(project_root, "output", "star_views", view_name)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.write.format("parquet").mode("overwrite").save(output_path)
            print(f"View data saved at: {output_path}")
        except Exception as e:
            print(f"Error querying/writing view {view_name}: {e}")
