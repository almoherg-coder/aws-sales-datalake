import sys
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# ============================================
# SETUP
# ============================================
# SparkContext = the engine that runs PySpark
sc = SparkContext()

# GlueContext = connects Spark with AWS Glue
glueContext = GlueContext(sc)

# spark = our main object to read/write data
spark = glueContext.spark_session

# boto3 client = connects to Glue Catalog API
glue_client = boto3.client("glue", region_name="eu-west-1")

# ============================================
# PATHS & CONFIG
# ============================================
processed_path = "s3://sales-datalake-georgios/processed/"
delta_path = "s3://sales-datalake-georgios/delta/"
database_name = "superstore_db"
tables = ["sales", "customers", "products"]

# ============================================
# CONVERT PARQUET → DELTA
# ============================================
for table in tables:
    print(f"Converting {table}...")
    
    # READ Parquet from processed/
    df = spark.read.parquet(f"{processed_path}{table}/")
    
    # WRITE as Delta format to delta/
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(f"{delta_path}{table}/")
    
    print(f"{table} converted successfully! ")

# ============================================
# REGISTER TABLES IN GLUE CATALOG
# ============================================
for table in tables:
    delta_table_name = f"delta_{table}"
    table_location = f"{delta_path}{table}/"
    
    print(f"Registering {delta_table_name} in Glue Catalog...")
    
    # Read schema from Delta table
    df = spark.read.format("delta").load(table_location)
    
    # Build columns list from schema
    columns = [
        {"Name": field.name, "Type": str(field.dataType.simpleString())}
        for field in df.schema.fields
    ]
    
    # Delete table if already exists
    try:
        glue_client.delete_table(
            DatabaseName=database_name,
            Name=delta_table_name
        )
        print(f"Deleted existing {delta_table_name}")
    except:
        pass
    
    # Create table in Glue Catalog
    glue_client.create_table(
    DatabaseName=database_name,
    TableInput={
        "Name": delta_table_name,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": columns,
            "Location": table_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {
                    "serialization.format": "1",
                    "path": table_location
                }
            },
            "Compressed": False,
            "StoredAsSubDirectories": False
        },
        "Parameters": {
            "classification": "parquet",
            "table_type": "EXTERNAL_TABLE",
            "spark.sql.sources.provider": "delta",
            "path": table_location
        }
    }
)
    print(f"{delta_table_name} registered successfully! ")

print("All tables converted and registered! ")
