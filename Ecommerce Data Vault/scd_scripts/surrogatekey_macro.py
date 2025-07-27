from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
import os

# Init Spark
spark = SparkSession.builder \
    .appName("Surrogate Key Generator Macro") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def generate_surrogate_keys(df, sk_column, order_by_col, delta_table_path):
    """
    Appends surrogate key to new DataFrame and writes to Delta Table.
    """
    
    os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)

    # Read existing delta table (if exists)
    try:
        existing_df = spark.read.format("delta").load(delta_table_path)
        max_sk = existing_df.agg({sk_column: "max"}).collect()[0][0] or 0
    except:
        existing_df = None
        max_sk = 0

    # Add SK using row_num + max_sk
    window_spec = Window.orderBy(order_by_col)
    df_with_sk = df.withColumn(sk_column, row_number().over(window_spec) + max_sk)

    # Reorder col
    final_df = df_withColumnOrder(df_with_sk, [sk_column] + [col for col in df.columns])

    # Write (append or create)
    write_mode = "append" if existing_df else "overwrite"
    final_df.write.format("delta").mode(write_mode).save(delta_table_path)

    return final_df

def df_withColumnOrder(df, col_order):
    return df.select(col_order)

# 4–5 new trans (no SK yet)
sales_data = spark.createDataFrame([
    ("c005", "p001", "2023-02-01", 110.0),
    ("c006", "p002", "2023-02-02", 89.5),
    ("c007", "p003", "2023-02-03", 60.0),
    ("c008", "p004", "2023-02-04", 200.0),
    ("c009", "p005", "2023-02-05", 130.5)
], ["customer_hk", "product_hk", "sale_date", "amount"])

# Gen surrkeys and write
delta_path = "./delta/fact_sales"
output_df = generate_surrogate_keys(
    df=sales_data,
    sk_column="sale_sk",
    order_by_col="sale_date",
    delta_table_path=delta_path
)

# result
print("Final data with surrogate keys:")
output_df.orderBy("sale_sk").show(truncate=False)
