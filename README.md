# 🏗️ Smart Sales Data Lake + AI Insights Engine

An end-to-end AWS Data Engineering project built on the Kaggle Superstore dataset.

## 🎯 Project Overview

This project demonstrates a complete modern data engineering pipeline on AWS, combining:
- **Delta Lake** for ACID transactions and Time Travel
- **AI-powered querying** using Amazon Bedrock (Nova Lite)
- **Data Warehouse** with Amazon Redshift
- **BI Dashboards** with Amazon QuickSight

---

## 🏛️ Architecture

```
Raw CSV (S3)
    ↓ AWS Glue ETL
Parquet Tables (S3)
    ↓ AWS Glue ETL
Delta Lake (S3)
    ├── Athena → Lambda → Bedrock AI (Natural Language queries)
    └── Redshift → QuickSight (BI Dashboards)
```

---

## 🛠️ AWS Services Used

| Service | Purpose |
|---|---|
| Amazon S3 | Data Lake storage |
| AWS Glue | ETL jobs (PySpark) |
| AWS Glue Catalog | Metadata management |
| Amazon Athena | SQL queries on Delta Lake |
| AWS Lambda | Serverless AI pipeline |
| Amazon Bedrock | AI SQL generation (Nova Lite) |
| Amazon Redshift Serverless | Data Warehouse |
| Amazon QuickSight | BI Dashboards |

---

## 📁 Project Structure

```
aws-smart-sales-datalake/
    ├── glue-jobs/
    │     ├── csv_to_parquet.py
    │     ├── parquet_to_delta_converter.py
    │     ├── delta_append_new_orders.py
    │     └── delta_time_travel_demo.py
    ├── lambda/
    │     └── superstore_ai_query.py
    ├── screenshots/
    └── sample-data/
          └── new_orders.csv
```

---

## 🚀 Key Features

### 1. Delta Lake + Time Travel
- Converted Parquet tables to Delta format
- ACID transactions support
- Query historical data by version or timestamp

### 2. AI-Powered Querying
- Ask questions in natural language
- Amazon Bedrock generates SQL automatically
- Athena executes queries on Delta tables

### 3. Data Warehouse
- Loaded data into Redshift Serverless
- Fast analytics queries
- Connected to QuickSight for visualization

### 4. BI Dashboard
- Total Sales KPI: $2,272,449
- Total Profit KPI: $285,707
- Sales trends over time
- Top 10 products by sales

---

## 📊 Dashboard Screenshots

https://github.com/almoherg-coder/aws-sales-datalake/tree/main/screenshots

---

## 🗂️ Data Source

Original dataset: [Kaggle Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)

---

## 💡 Key Learnings

- Delta Lake requires Spark — Athena supports SELECT but not Time Travel
- Glue Crawlers don't support Delta format — tables must be registered via boto3
- Redshift cannot read Delta format directly — plain Parquet needed as intermediary
- Bedrock Nova Lite requires cross-region inference profile in eu-west-1

---
## Notes to understand why we use each service
## AWS Glue Crawler

The AWS Glue Crawler is responsible for discovering and cataloging the data stored in Amazon S3.

It performs three main tasks:

1. **Scans Parquet files in S3**
   - Reads all files from:
     ```
     s3://sales-datalake-georgios/processed/
     ```
   - Extracts metadata from the data files.

2. **Automatically detects the schema**
   - Identifies column names and data types, for example:
     ```
     Order_ID    → String
     Sales       → Double
     Order_Date  → Date
     ```

3. **Creates tables in AWS Glue Data Catalog**
   - Registers the discovered schema as tables inside:
     ```
     superstore_db
     ```
   - The Crawler only stores metadata; it does not copy or move the actual data.

### Data Flow

Before Glue Crawler:
Before Glue Crawler:


S3 (Parquet Files) → Athena


Athena cannot query the data because it does not know the table structure.

After Glue Crawler:


S3 (Parquet Files)
↓
Glue Data Catalog (Schema Metadata)
↓
Athena (SQL Queries)


Athena uses the Glue Catalog metadata to understand the structure of the Parquet files and


## 👤 Author
Georgios — Data Engineer
