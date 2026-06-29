import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType, DoubleType, IntegerType
from datetime import datetime

# ============================================
# SETUP
# ============================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ============================================
# PATHS
# ============================================
new_orders_path = "s3://sales-datalake-georgios/raw/new_orders.csv"
delta_sales_path = "s3://sales-datalake-georgios/delta/sales/"

# ============================================
# UDF TO PARSE DATE — same as original
# ============================================
def parse_date(s):
    if s is None:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%-m/%-d/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except:
            continue
    return None

parse_date_udf = udf(parse_date, DateType())

# ============================================
# READ NEW CSV
# ============================================
print("Reading new orders CSV...")
df_new = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(new_orders_path)

# Clean column names — same as original
def clean_col(name):
    return name.lower().replace(" ", "_").replace("-", "_")

df_new = df_new.toDF(*[clean_col(c) for c in df_new.columns])

# Fix dates — same as original
df_new = df_new \
    .withColumn("order_date", parse_date_udf(col("order_date"))) \
    .withColumn("ship_date", parse_date_udf(col("ship_date")))

# Fix numeric types — same as original
df_new = df_new \
    .withColumn("sales", col("sales").cast(DoubleType())) \
    .withColumn("profit", col("profit").cast(DoubleType())) \
    .withColumn("discount", col("discount").cast(DoubleType())) \
    .withColumn("quantity", col("quantity").cast(IntegerType()))

# Keep only sales columns
df_new = df_new.select(
    "row_id", "order_id", "order_date", "ship_date",
    "ship_mode", "customer_id", "product_id",
    "sales", "quantity", "discount", "profit"
)

print(f"New rows to add: {df_new.count()}")
df_new.show()

# ============================================
# CHECK ROWS BEFORE APPEND
# ============================================
df_before = spark.read.format("delta").load(delta_sales_path)
print(f"Rows BEFORE append: {df_before.count()}")

# ============================================
# APPEND TO DELTA TABLE
# ============================================
print("Appending to Delta table...")
df_new.write \
    .format("delta") \
    .mode("append") \
    .save(delta_sales_path)

# ============================================
# CHECK ROWS AFTER APPEND
# ============================================
df_after = spark.read.format("delta").load(delta_sales_path)
print(f"Rows AFTER append: {df_after.count()}")

print("New orders appended successfully! 🚀")
job.commit()
