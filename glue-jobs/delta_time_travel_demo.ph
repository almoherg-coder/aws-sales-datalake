import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# ============================================
# SETUP
# ============================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Delta table path
sales_path = "s3://sales-datalake-georgios/delta/sales/"

# ============================================
# LATEST VERSION
# ============================================
print("=== LATEST VERSION ===")
df_latest = spark.read.format("delta") \
    .load(sales_path)
print(f"Latest rows: {df_latest.count()}")
df_latest.show(5)

# ============================================
# VERSION 0 — First run
# ============================================
print("=== VERSION 0 ===")
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(sales_path)
print(f"Version 0 rows: {df_v0.count()}")
df_v0.show(5)

# ============================================
# VERSION 1 — Second run
# ============================================
print("=== VERSION 1 ===")
df_v1 = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load(sales_path)
print(f"Version 1 rows: {df_v1.count()}")
df_v1.show(5)

# ============================================
# BY DATE — Time Travel
# ============================================
print("=== BY TIMESTAMP ===")
df_date = spark.read.format("delta") \
    .option("timestampAsOf", "2026-06-27 08:00:00") \
    .load(sales_path)
print(f"Timestamp rows: {df_date.count()}")
df_date.show(5)

print("Time Travel demo complete! ")
