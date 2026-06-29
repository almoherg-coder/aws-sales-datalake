import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType, DoubleType, IntegerType
from datetime import datetime

# ── Init ──────────────────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ── CONFIG ────────────────────────────────────────────────────────
BUCKET = "s3://sales-datalake-georgios"
RAW    = f"{BUCKET}/raw/"
OUT    = f"{BUCKET}/processed"

# ── UDF to parse any date format ──────────────────────────────────
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

# ── Read raw CSV ──────────────────────────────────────────────────
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW)

# ── Clean column names ────────────────────────────────────────────
def clean_col(name):
    return name.lower().replace(" ", "_").replace("-", "_")

df = df.toDF(*[clean_col(c) for c in df.columns])

# ── Fix dates ─────────────────────────────────────────────────────
df = df.withColumn("order_date", parse_date_udf(col("order_date")))
df = df.withColumn("ship_date",  parse_date_udf(col("ship_date")))

# ── Fix numeric types ─────────────────────────────────────────────
df = df.withColumn("sales",    col("sales").cast(DoubleType())) \
       .withColumn("profit",   col("profit").cast(DoubleType())) \
       .withColumn("discount", col("discount").cast(DoubleType())) \
       .withColumn("quantity", col("quantity").cast(IntegerType()))

# ════════════════════════════════════════════════════════════════
# TABLE 1 — sales
# ════════════════════════════════════════════════════════════════
sales_df = df.select(
    "row_id", "order_id", "order_date", "ship_date",
    "ship_mode", "customer_id", "product_id",
    "sales", "quantity", "discount", "profit"
)
sales_df.write.mode("overwrite").parquet(f"{OUT}/sales/")
print(f"sales — {sales_df.count()} rows")

# ════════════════════════════════════════════════════════════════
# TABLE 2 — products
# ════════════════════════════════════════════════════════════════
products_df = df.select(
    "product_id", "product_name", "category", "sub_category"
).dropDuplicates(["product_id"])
products_df.write.mode("overwrite").parquet(f"{OUT}/products/")
print(f"products — {products_df.count()} rows")

# ════════════════════════════════════════════════════════════════
# TABLE 3 — customers
# ════════════════════════════════════════════════════════════════
customers_df = df.select(
    "customer_id", "customer_name", "segment",
    "country", "city", "state", "postal_code", "region"
).dropDuplicates(["customer_id"])
customers_df.write.mode("overwrite").parquet(f"{OUT}/customers/")
print(f"customers — {customers_df.count()} rows")

# ── Done ──────────────────────────────────────────────────────────
print("All 3 tables created successfully!")
job.commit()
